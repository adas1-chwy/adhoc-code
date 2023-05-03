use database edldb;
use schema discovery_sandbox;
set experiment_name = 'ATTACH_SMARTSHELF_02';
set start_Date = '2023-02-14';
set end_date = '2023-02-26';

  
  
/** Creating experiment pool based cohorts based on activations into the different groups 
here we are excluding spillovers for cleaner measurement **/ 
CREATE 
OR 
REPLACE temp TABLE exp_base AS
( SELECT
    t1.experiment 
    , t1.session_date 
    , t1.session_id 
    , t1.personalization_id 
    , t1.test_arm
  FROM discovery_sandbox.prd_d_d_dis_expr_pool AS t1
  WHERE 
    t1.session_date BETWEEN $start_date AND $end_date
  AND t1.experiment = 'ATTACH_SMARTSHELF_02'
    -- remove spill over
    QUALIFY COUNT(DISTINCT t1.test_arm) over ( 
                                            PARTITION BY 
                                              t1.personalization_id) = 1 
);

  
  

--- Mapping pIDs to Customer_IDs from the exp pool table 
CREATE
OR
REPLACE temp TABLE exp_base_cust AS
SELECT
  DISTINCT a.personalization_id
  , customer_id
FROM exp_base a
LEFT JOIN discovery_sandbox.prd_d_d_dis_expr_pool b
ON a.personalization_id = b.personalization_id
WHERE
  b.session_date BETWEEN $start_date AND $end_date;


-- filtering for PIDs that we have a CID for
CREATE
OR
REPLACE temp TABLE exp_base_customer_id AS
(select *
from 
        (SELECT
          DISTINCT a.personalization_id
          , customer_id
        FROM exp_base_cust a
        WHERE
          customer_id is not null 
          --- removing pids mapped to multiple CIDs 
          qualify count(distinct customer_ID) over(partition by personalization_id) = 1 
        )         
        --- removing CIDs mappped to multiple PIDs
QUALIFY count(distinct personalization_id) over(partition by customer_ID) = 1)        
  ;



--subsettiing original exp_base table with cohort assignments for filtered PIDs that are uniquely mapped to 1 cid 
CREATE
OR
REPLACE temp TABLE exp_base_cohort AS
(
select b.*, a.customer_id
from exp_base_customer_id a 
inner join exp_base b 
on a.personalization_id = b.personalization_id 
--and a.customer_id = b.customer_id
);



/*** all date and related attributes **/ 

CREATE 
OR 
REPLACE temp TABLE dates AS
SELECT 
  common_date_dttm 
  , financial_calendar_reporting_year AS financial_year 
  , financial_calendar_reporting_quarter AS financial_quarter 
  , financial_calendar_reporting_period AS financial_period 
  , financial_calendar_reporting_week_of_year AS financial_week_of_year 
  , financial_calendar_last_day_reporting_week AS last_day_of_week 
  , COMMON_DAY_OF_YEAR 
  , common_week_of_year 
  , common_week_of_month
FROM cdm.common_date
WHERE 
  common_date_dttm >= $start_date
AND common_date_dttm <= CURRENT_DATE 
;


/* Below table now pools in all orders for our cohort customers over time since the beginning of experiment 
to track the trend ***/ 
  
CREATE
OR
REPLACE temp TABLE exp_base_cohort_orders AS
( SELECT
    a.customer_id
    , a.test_arm
    , a.personalization_id
    , b.transaction_session_date
    , b.transaction_id
    , b.part_number
    , b.price
    , b.quantity
    , b.revenue
    , b.gross_margin
    , b.order_product_mc1
    , b.order_product_mc2
    , b.order_product_mc3
    , b.order_product_category_level1
    , b.order_product_category_level2
    , b.order_product_category_level3
    , b.order_product_attach_flag
    , b.cat_food_order_flag
    , b.dog_food_order_flag
    , b.attach_order_flag
    , b.rpm_order_flag
    , b.attribution_type
    , b.attributed_widget_id
    , b.attributed_widget_parent_group
    , b.attributed_widget_rpm_flag
    , b.page_type
    , coalesce(b.order_session_device_category, FIRST_VALUE( b.order_session_device_category) IGNORE NULLS OVER ( PARTITION BY b.transaction_id ORDER BY b.part_number )) as device_category
    , coalesce(b.order_session_new_customer_flag, FIRST_VALUE( b.order_session_new_customer_flag) IGNORE NULLS OVER ( PARTITION BY b.transaction_id ORDER BY b.part_number )) as new_customer_flag
    , coalesce(b.order_session_active_autoship_flag, FIRST_VALUE( b.order_session_active_autoship_flag) IGNORE NULLS OVER ( PARTITION BY b.transaction_id ORDER BY b.part_number )) as active_autoship_flag
    , coalesce(b.order_session_channel, FIRST_VALUE( b.order_session_channel) IGNORE NULLS OVER ( PARTITION BY b.transaction_id ORDER BY b.part_number )) as channel
--    , b.new_customer_flag
--    , b.active_autoship_flag
--    , b.channel
    , financial_year||'- Wk'||lpad(financial_week_of_year, 2,'0') as week_period
        
  FROM ( SELECT
        DISTINCT customer_id
        , test_arm
        , personalization_id
      FROM exp_base_cohort) a
  INNER JOIN discovery_sandbox.prd_f_d_first_touch_attribution b
  ON a.customer_id = b.customer_id
  join dates c 
        on b.transaction_session_date = c.common_date_dttm
  WHERE
    b.transaction_session_date > $start_Date
    and b.attribution_type = 'in-session');


/*** weekly Summary stats 
Since all the dimensions are at a customer level, it makes it easier to aggregate the distinct counts 
across these dimensions when pivoting in excel 
**/ 


SELECT 
  week_period 
  , test_arm 
  , new_customer_flag 
  , active_autoship_flag 
  , channel 
  , device_category 
  , count(distinct transaction_id) as total_orders
  , count(distinct customer_id) as unique_ordering_customers
  , sum(quantity) as total_units
  , sum(revenue) as total_revenue
  , count(distinct(case when dog_food_order_flag = true then transaction_id end)) as dog_orders
  , count(distinct(case when dog_food_order_flag = true and attach_order_flag = true then transaction_id end)) as dog_orders_attach
--  , count(distinct(case when dog_food_order_flag = true and attach_order_flag = true and rpm_order_flag = true then transaction_id end)) as dog_orders_rpm_attach
  , count(distinct(case when dog_food_order_flag = true and order_product_attach_flag = true and attributed_widget_rpm_flag = true then transaction_id end)) as dog_orders_rpm_attach
  , count(distinct(case when cat_food_order_flag = true then transaction_id end)) as cat_orders
  , count(distinct(case when cat_food_order_flag = true and attach_order_flag = true then transaction_id end)) as cat_orders_attach
  , count(distinct(case when cat_food_order_flag = true and order_product_attach_flag = true and attributed_widget_rpm_flag = true then transaction_id end)) as cat_orders_rpm_attach 
  , count(distinct(case when order_product_category_level2 in ('Treats','Toys') then transaction_ID end)) as toys_treats_orders
  , sum(case when order_product_category_level2 in ('Treats','Toys') then quantity end) as toys_treats_units
  , sum(case when order_product_category_level2 in ('Treats','Toys') then revenue end) as toys_treats_revenue
  , count(distinct(case when order_product_category_level2 in ('Litter & Accessories', 'Health & Wellness') then transaction_ID end)) as litter_hw_orders
  , sum(case when order_product_category_level2 in ('Litter & Accessories', 'Health & Wellness') then quantity end) as litter_hw_units
  , sum(case when order_product_category_level2 in ('Litter & Accessories', 'Health & Wellness') then revenue end) as litter_hw_revenue
from exp_base_cohort_orders
group by 1,2,3,4,5,6;



