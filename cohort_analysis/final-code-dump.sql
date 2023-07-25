
use role discovery_developer;
use database edldb;
use schema discovery_sandbox;
set experiment_name = 'ATTACH_SMARTSHELF_02';
set start_Date = '2023-02-13';
set end_date = '2023-02-27';


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
    , t1.customer_ID
  FROM discovery_sandbox.prd_d_d_dis_expr_pool AS t1
  WHERE 
    t1.session_date BETWEEN $start_date AND $end_date
  AND t1.experiment = 'ATTACH_SMARTSHELF_02'
    -- remove spill over
    QUALIFY COUNT(DISTINCT t1.test_arm) over ( 
                                            PARTITION BY 
                                              t1.personalization_id) = 1 
);



CREATE OR REPLACE TABLE 
  discovery_sandbox.ad_ca_raw_exp_sessions AS
  ( SELECT 
      b.test_arm, 
      B.CUSTOMER_ID AS EXP_POOL_CUSTOMER_ID,
      b.session_date, 
      b.session_ID,
      a.FULL_VISITOR_ID,
        a.row_type,
        a.UNIQUE_VISIT_ID,
        a.customer_ID,
        a.TYPE,       
        a.CHANNEL_GROUPING,
        a.TRAFFIC_SOURCE,
        a.SESSION_CUSTOM_ATTRIBUTES,
        a.HIT_NUMBER,
        a.HIT_TIMESTAMP,
        a.IS_ENTRANCE,
        a.IS_EXIT,
        a.HIT_CUSTOM_ATTRIBUTES,
        a.IS_IMPRESSION,
        a.IS_CLICK,
        a.PRODUCT_CUSTOM_METRICS,
        a.PRODUCT_LIST_NAME,
        a.PRODUCT_LIST_POSITION,
        a.PRODUCT_SKU,
        a.PRODUCT_ID,
        a.PRODUCT_PART_NUMBER,
        a.PRODUCT_CATEGORY_LEVEL1,
        a.PRODUCT_CATEGORY_LEVEL2,
        a.PRODUCT_CATEGORY_LEVEL3,
        a.PRODUCT_CATEGORY_LIST,
        a.PARENT_PRODUCT_PART_NUMBER,
        a.PRODUCT_MANUFACTURER_NAME,
        a.PRODUCT_ATTRIBUTE_PRODUCT_TYPE,
        a.PRODUCT_ATTRIBUTE_BREED_SIZE,
        a.PRODUCT_ATTRIBUTE_LIFE_STAGE,
        a.PRODUCT_MERCH_CLASSIFICATION1,
        a.PRODUCT_MERCH_CLASSIFICATION2,
        a.PRODUCT_MERCH_CLASSIFICATION3,
        a.KEY_FUNNEL_EVENT,
        a.PAGE_PATH,
        a.PAGE_TITLE,
        a.EVENT_ACTION,
        a.EVENT_LABEL,
        a.EVENT_VALUE,
        a.EVENT_CATEGORY,
        a.NEW_CUSTOMER_FLAG,
        a.TRANSACTION_ID,
        a.CHANNEL,
        a.LIST_CATEGORY,
        a.LIST_PAGE_NUMBER,
        a.AUTOSHIP_ID,
        a.AUTOSHIP_FREQUENCY,
        a.AUTOSHIP_OPTION,
        a.CART_ORDER_ID,
        a.PARENT_AUTOSHIP_ORDER_ID,
        a.ORDER_ID,
        a.ACTIVE_AUTOSHIP_FLAG,
        a.TRIED_AUTOSHIP_FLAG,
        a.PLP_SESSION,
        a.PDP_SESSION,
        a.ATC_SESSION,
        a.CHECKOUT_SESSION,
        a.PURCHASE_SESSION,
        a.BEFORE_PLP,
        a.BEFORE_PDP,
        a.BEFORE_ATC,
        a.BEFORE_CHECKOUT,
        a.BEFORE_PURCHASE,
        a.PDP_VIEWS,
        a.PDP_VIEW_RATE,
        a.PLP_SESSIONS_CM,
        a.PLP_RATE,
        a.CART_TYPE,
        a.AUTOSHIP_ELIGIBLE,
        a.AUTOSHIP_APPLIED,
        a.SCREEN_NAME,
        a.BOT_TYPE
    FROM ga.ga_sessions_hits_products_union a
    INNER JOIN 
      ( SELECT 
          DISTINCT session_id 
          , test_arm 
          , customer_id 
          , session_date 
        FROM exp_base 
        WHERE 
          customer_id IS NOT NULL) b
    ON a.unique_visit_id = b.session_id
    AND a.ga_sessions_date = b.session_date
    where a.ga_sessions_date between '2023-02-12' AND '2023-02-28'  ---- in reality this was inserted with couple days at a time 
  ) 
;
COMMIT;

/*Limiting sessions to those that had a purchase during the experiment */
CREATE OR REPLACE TABLE 
  discovery_sandbox.ad_ca_sessions_purchases AS
SELECT 
  * 
FROM discovery_sandbox.ad_ca_raw_exp_sessions
WHERE 
  session_ID IN 
  ( SELECT 
      DISTINCT session_id
    FROM discovery_sandbox.ad_ca_raw_exp_sessions
    WHERE 
      event_action = 'purchase' 
    OR transaction_id IS NOT NULL ) 
;
 
 
 
 
 /*
*//*** gettting ecom base table with order data for purchasing customers from experiment ***//* 


CREATE temp TABLE purchasing_CIDs AS
SELECT 
  DISTINCT customer_id 
FROM discovery_sandbox.ad_ca_sessions_purchases 
WHERE 
  transaction_id IS NOT NULL 
;

select top 10 * from ecom.order_line



CREATE OR REPLACE TABLE 
  discovery_sandbox.ad_ca_pcid_orders AS
  ( SELECT 
      order_id 
      , a.customer_id 
      , DATE(order_placed_dttm) AS order_date 
      , part_number 
      , order_order_line_status 
      , order_status_description 
      , order_first_order_placed_flag 
      , subscription_ID 
      , order_submitted_by 
      , business_channel_name 
      , order_line_quantity 
      , order_line_each_price 
      , order_line_total_price 
      , order_line_ship_charge 
      , order_line_total_adjustment 
      , refund_credit_amount 
      , appeasement_credit_amount 
      , raw_product_total_cost 
      , raw_product_total_cost_provisional 
      , inventory_adjustment_cost 
      , rebate_amount 
      , autoship_rebate_amount 
      , order_line_royalty_amount 
      , shipment_net_charge_allocation 
      , shipment_net_charge_allocation_default 
      , corrugate_cost 
      , ((((((((((((COALESCE(order_line_total_price, 0::numeric(18,0)) + COALESCE(order_line_ship_charge, 0::numeric(18,0))) + COALESCE(order_line_total_adjustment, 0::numeric(18,0))) 
      + COALESCE(refund_credit_amount, 0::numeric(18,0))) + COALESCE(appeasement_credit_amount, 0::numeric(18,0))) + COALESCE( 
      CASE WHEN (COALESCE(raw_product_total_cost, 0::numeric(18,0)) = 0::numeric(18,0))
        THEN raw_product_total_cost_provisional
        ELSE raw_product_total_cost
      END, 0::numeric(18,0)) 
      ) + COALESCE(inventory_adjustment_cost, 0::numeric(18,0))) + COALESCE(rebate_amount, 0::numeric(18,0))) + COALESCE(autoship_rebate_amount, 0::numeric(18,0))) 
      + COALESCE(order_line_royalty_amount, 0::numeric(18,0))) + COALESCE(shipment_net_charge_allocation, 0::numeric(18,0))) 
      + COALESCE(shipment_net_charge_allocation_default, 0::numeric(18,0))) + COALESCE(corrugate_cost, 0::numeric(18,0))) AS gross_margin 
      , financial_calendar_reporting_year 
      , financial_calendar_reporting_period
    FROM ecom.order_line a
    INNER JOIN purchasing_CIDs b
    ON a.customer_id = b.customer_id
    where DATE(a.order_placed_dttm) >= '2023-01-01'   *//* This table has all order data from 2021-01-01 inserted in parts for all purchasing customers during the experiment*//*
  ) 
;commit;

*/



/*Identifying sessions that need to be excluded because of AddtoAutoship activation */
create or replace temp table all_sessions_autoship_flags as 
(select a.session_ID, a.customer_id
        , test_arm
        , max(case when event_action = 'addToAutoship' and  event_label = 'pdp' then 1 else 0 end) as autoship_add
        , max(case when event_action = 'addToCart' and  event_label = 'pdp' then 1 else 0 end) as cart_add 

from 
        discovery_sandbox.ad_ca_sessions_purchases a 
        inner join  -- below join ensuring we are pulling in events prior to first activation into the experiment
        (select session_id, min(hit_number) as hit_number
        from discovery_sandbox.ad_ca_raw_exp_sessions
        where  event_action = 'ATTACH_SMARTSHELF_02'
        group by 1
        ) b 
        on a.session_id = b.session_id 
        and a.hit_number < b.hit_number
group by 1,2,3);



/* Filtering for first purchases during experiment period for cohort determination, 
excluding incorrect activations (from AddToAutoship)  */
create or replace temp table first_purchase as 
select * 
from 
        (select a.*, 
                row_number() over(partition by a.customer_id order by session_date) as order_number
        from 
        (select distinct a.session_ID, a.customer_id, a.transaction_ID, a.session_date 
                from discovery_sandbox.ad_ca_sessions_purchases a 
                left join 
                (select session_ID from all_sessions_autoship_flags 
                 where cart_add = 0 and autoship_add = 1) b  --- removing autoship activations 
                on a.session_id = b.session_ID                 
                where a.transaction_ID is not null 
                and b.session_ID is null
        ) a 
        inner join ecom.orders eo
        on a.transaction_ID = eo.order_ID
where eo.order_status NOT IN ('X', 'P', 'J') 
)
where order_number = 1 
;



/** First purchase session with relevant event records filtered to derive cohort determination attributes **/
create or replace table discovery_sandbox.ad_ca_first_purchase_events as 
select case 
        when  (event_action = 'addToCart' and event_label = 'pdp')  then 'PDP-ATC'
        when  (event_action = 'impression' and event_label IN ('upsell-drawer','promo-drawer'))  then 'SS-Impression'
        when  (event_action = 'addToCart' and event_label IN ('upsell_10_control'))  then 'SS-ATC'
        when  (event_action = 'impression' and event_label IN ('upsell_10_control'))  then 'PLAS-Impression'
        when  (event_action = 'productClick' and event_label IN ('upsell_10_control'))  then 'PLAS-ProductClick'
        when  ( event_action = 'purchase') then 'Purchase' END as event_type
        ,b.transaction_ID as transaction_ID_ref
        ,a.* 
from discovery_sandbox.ad_ca_sessions_purchases a 
inner join first_purchase b on a.session_id = b.session_id 
where (event_action = 'addToCart' and event_label = 'pdp') 
OR (event_action = 'impression' and event_label IN ('upsell-drawer','promo-drawer'))
OR (event_action = 'addToCart' and event_label IN ('upsell_10_control'))
OR (event_action = 'impression' and event_label IN ('upsell_10_control'))
OR (event_action = 'productClick' and event_label IN ('upsell_10_control'))
OR ( event_action = 'purchase') 
OR (a.transaction_id is not null)
;
commit;






/* Creating the base dataset based on first purchases during the experiment 
with the individual features defined to create the cohorts we want to based on specific actions  */

--- first the helper tables for each kind of action 

create or replace temp table purchased as 
select * 
from discovery_sandbox.ad_ca_first_purchase_events
where event_type = 'Purchase';

create or replace temp table pdp_atc as 
select * 
from discovery_sandbox.ad_ca_first_purchase_events
where event_type = 'PDP-ATC';

create or replace temp table ss_ATC as 
select * 
from discovery_sandbox.ad_ca_first_purchase_events
where event_type = 'SS-ATC';
 

create or replace table discovery_sandbox.ad_ca_cohort_features as 
select 
        case when a.product_category_level3 = 'Dry Food' then 1 else 0 end as purchase_dry_food
        , case when a.product_category_level3 = 'Wet Food' then 1 else 0 end as purchase_wet_food
        , case when a.product_category_level2 = 'Treats' then 1 else 0 end as purchase_treats
        , case when a.product_category_level2 = 'Toys' then 1 else 0 end as purchase_toys
        , case when a.product_category_level2 = 'Health & Wellness' then 1 else 0 end as purchase_hw
        , case when a.product_category_level2 = 'Litter & Accessories' then 1 else 0 end as purchase_litter
        , case when a.product_category_level2 = 'Food' then 1 else 0 end as purchase_food_cl2
        , case when b.product_part_number is not null and a.product_category_level3 = 'Dry Food' then 1 else 0 end as pdp_atc_dry_food
        , case when b.product_part_number is not null and a.product_category_level3 = 'Wet Food' then 1 else 0 end as pdp_atc_wet_food
        , case when b.product_part_number is not null and a.product_category_level2 = 'Food' then 1 else 0 end as pdp_atc_food_cl2
        , case when c.product_part_number is not null and a.product_category_level2 = 'Treats' then 1 else 0 end as SS_atc_treat
        , case when c.product_part_number is not null and a.product_category_level2 = 'Toys' then 1 else 0 end as SS_atc_toys
        , case when c.product_part_number is not null and a.product_category_level2 = 'Health & Wellness' then 1 else 0 end as SS_atc_hw
        , case when c.product_part_number is not null and a.product_category_level2 = 'Litter & Accessories' then 1 else 0 end as SS_atc_litter
        , case when c.product_part_number is not null and a.product_category_level3 = 'Wet Food' then 1 else 0 end as SS_atc_wet_food
        , case when c.product_part_number is not null and a.product_category_level3 = 'Dry Food' then 1 else 0 end as SS_atc_dry_food
        , case when d.session_ID is not null then 1 else 0 end as ss_impression
        , case when e.session_ID is not null then 1 else 0 end as ss_atc
        , a.*
from purchased a 
left join pdp_atc b 
on a.session_id = b.session_id 
and a.product_part_number = b.product_part_number 
LEFT JOIN ss_atc c 
on a.session_id = c.session_id 
and a.product_part_number = c.product_part_number 
--- below join to get if the session had SS impression 
LEFT JOIN (select distinct session_id
 from discovery_sandbox.ad_ca_raw_exp_sessions
 where event_action = 'impression' and event_label IN ('upsell-drawer','promo-drawer')) d 
 on a.session_ID = d.session_ID
 -- below snippet to get if session had any SS adds from PLAS
LEFT JOIN (select distinct session_id
 from discovery_sandbox.ad_ca_raw_exp_sessions
 where event_action = 'addToCart' and event_label IN ('upsell_10_control')) e 
 on a.session_ID = e.session_ID
;




/* using the above features for unique flags denoting a single cohort */ 
create or replace table discovery_sandbox.ad_ca_customer_level_cohort_flags as 
select test_arm, customer_id,  new_customer_flag, active_autoship_flag, 
        case 
        --cohort #1
        when test_arm = 'CONTROL' and sum(purchase_wet_food)>0 and sum(ss_atc_wet_food)>0 
                and sum(purchase_dry_food) > 0 and sum(pdp_atc_dry_food) > 0 then 1 else 0 end  as C1_Control_WetFood_DryFood
        , case 
        --cohort #2
        when test_arm = 'CONTROL' and sum(purchase_wet_food) > 0 and sum(ss_atc_wet_food)>0 and sum(pdp_atc_wet_food) >0 then 1 else 0 end  as C2_Control_WetFood_WetFood
        , case
        --cohort #control new
        when test_arm = 'CONTROL' and (sum(purchase_dry_food) > 0 or sum(purchase_wet_food) > 0) 
                and sum(pdp_atc_wet_food) = 0 and sUm(pdp_atc_dry_food)=0 
                and (sum(ss_atc_wet_food)>0 OR sum(ss_atc_dry_food)>0) then 1 else 0 end as Control_SS_add_food_pdp_nonfood 
        , case 
        --cohort #5 
        when test_arm = 'VARIANT_01' and (sum(purchase_wet_food) > 0 or sum(purchase_dry_food) >0 ) and sum(ss_impression) >0  
                and sum(ss_atc) = 0  then 1 else 0 end  as C5_Variant1_Checkout_wo_engagement
        , case 
        --cohort #6 
        when test_arm = 'VARIANT_01' and sum(purchase_treats) >0 and sum(ss_atc_treat) >0
                and sum(purchase_dry_food) >0 and sum(pdp_atc_dry_food) > 0 then 1 else 0 end  as C6_Variant1_Treats_DryFood
        , case 
        --cohort #7
        when test_arm = 'VARIANT_01' and sum(purchase_treats) >0 and sum(ss_atc_treat) >0
                and sum(purchase_wet_food) >0 and sum(pdp_atc_wet_food) > 0 then 1 else 0 end  as C7_Variant1_Treats_WetFood
        , CASE
        --cohort #8
        when test_arm = 'VARIANT_01' and sum(purchase_toys) >0 and sum(ss_atc_toys) >0
                and sum(purchase_dry_food) >0 and sum(pdp_atc_dry_food) > 0 then 1 else 0 end  as C8_Variant1_Toys_DryFood
        , case 
        --cohort #9
        when test_arm = 'VARIANT_01' and sum(purchase_toys) >0 and sum(ss_atc_toys) >0
                and sum(purchase_wet_food) >0 and sum(pdp_atc_wet_food) > 0 then 1 else 0 end  as C9_Variant1_Toys_WetFood
        , case 
        --cohort #10
        when test_arm = 'VARIANT_02' and sum(purchase_litter) >0 and sum(ss_atc_litter) >0
                and sum(purchase_food_cl2) >0 and sum(pdp_atc_food_cl2) > 0 then 1 else 0 end  as C10_Variant2_Litter_Food_Cl2
        , CASE
        when test_arm = 'VARIANT_02' and sum(purchase_hw) >0 and sum(ss_atc_hw) >0
                and sum(purchase_food_cl2) >0 and sum(pdp_atc_food_cl2) > 0 then 1 else 0 end as C11_Variant2_HW_FoodCl2
     
from discovery_sandbox.ad_ca_cohort_features
group by 1,2,3,4;
commit;





/*Creating a cohort map for all customers*/
create or replace table discovery_sandbox.ad_ca_customer_cohort_map as 
(
select customer_id, test_arm, new_customer_flag, active_autoship_flag, 
      case 
        when test_arm = 'CONTROL' and C1_Control_WetFood_DryFood = 1 and C2_Control_WetFood_WetFood = 1 then 'Control(PC): Wet and Dry food' 
        when test_arm = 'CONTROL' and C1_Control_WetFood_DryFood = 1 and C2_Control_WetFood_WetFood = 0 then 'C1: Control - Purchased Wet food (SS) w/ Dry food (ATC)' 
        when test_arm = 'CONTROL' and C1_Control_WetFood_DryFood = 0 and C2_Control_WetFood_WetFood = 1 then 'C2: Control - Purchased Wet food (SS) w/ Wet food (ATC)' 
        when test_arm = 'CONTROL' and CONTROL_SS_ADD_FOOD_PDP_NONFOOD = 1 then 'Control: Purchased Dry/Wet Food (SS) w/ Non-food(ATC)'
        when test_arm = 'CONTROL' then 'Control: Not Mapped'
        when test_arm = 'VARIANT_01' and C5_VARIANT1_CHECKOUT_WO_ENGAGEMENT = 1 then 'C5: Variant1 - Purchased Wet/Dry food w/o SS Engagement'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 1 and C7_VARIANT1_TREATS_WETFOOD = 0 and C8_VARIANT1_TOYS_DRYFOOD = 0 and C9_VARIANT1_TOYS_WETFOOD = 0 then 'C6: Variant1 - Purchased Treats (SS) w/ Dry food (ATC)'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 0 and C7_VARIANT1_TREATS_WETFOOD = 1 and C8_VARIANT1_TOYS_DRYFOOD = 0 and C9_VARIANT1_TOYS_WETFOOD = 0 then 'C7: Variant1 - Purchased Treats (SS) w/ Wet food (ATC)'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 0 and C7_VARIANT1_TREATS_WETFOOD = 0 and C8_VARIANT1_TOYS_DRYFOOD = 1 and C9_VARIANT1_TOYS_WETFOOD = 0 then 'C8: Variant1 - Purchased Toys (SS) w/ Dry food (ATC)'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 0 and C7_VARIANT1_TREATS_WETFOOD = 0 and C8_VARIANT1_TOYS_DRYFOOD = 0 and C9_VARIANT1_TOYS_WETFOOD = 1 then 'C9: Variant1 - Purchased Toys (SS) w/ Wet food (ATC)'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 0 and C7_VARIANT1_TREATS_WETFOOD = 0 and C8_VARIANT1_TOYS_DRYFOOD = 0 and C9_VARIANT1_TOYS_WETFOOD = 0 then 'Variant1 - Not mapped'
        when test_arm = 'VARIANT_01' and (C6_Variant1_Treats_DryFood = 1 OR C7_VARIANT1_TREATS_WETFOOD = 1 OR C8_VARIANT1_TOYS_DRYFOOD = 1 OR C9_VARIANT1_TOYS_WETFOOD = 1) then 'Variant1(PC) - Purchased Toys/Treats (SS) w/ Wet/Dry food(ATC)'
        when test_arm = 'VARIANT_02' and C10_VARIANT2_LITTER_FOOD_CL2 = 1 and C11_VARIANT2_HW_FOODCL2 =  0 then 'C10: Variant2 - Purchased Litter(SS) w/ Food (ATC)'
        when test_arm = 'VARIANT_02' and C10_VARIANT2_LITTER_FOOD_CL2 = 0 and C11_VARIANT2_HW_FOODCL2 =  1 then 'C11: Variant2 - Purchased H&W(SS) w/ Food (ATC)'
        when test_arm = 'VARIANT_02' and C10_VARIANT2_LITTER_FOOD_CL2 = 0 and C11_VARIANT2_HW_FOODCL2 =  0 then 'Variant2 - Not mapped'
        else 'Not Mapped' end as cohort_name
from discovery_sandbox.ad_ca_customer_level_cohort_flags
where test_arm <> 'FALLBACK'
);
commit;




/*Getting all order data*/

---- Cohort table by test_arm for where customers had a PLAS impression
create or replace table discovery_sandbox.ad_ca_impression_cohort as 
select test_arm,customer_id
from discovery_sandbox.ad_ca_session_events
where event_type = 'PLAS-Impression'
group by 1,2;
commit;

---- Cohort table by test_arm for where customers had a PLAS engagement (ATC or product click)
create or replace table discovery_sandbox.ad_ca_engagement_cohort as 
select test_arm,customer_id 
from discovery_sandbox.ad_ca_session_events
where event_type IN ('SS-ATC','PLAS-ProductClick')
group by 1,2;
commit;


---- Cohort table by test_arm for where customers had a PLAS add and purchase for the same item
create or replace table discovery_sandbox.ad_ca_purchase_cohort as 
select test_arm, customer_id
from 
(select test_arm,customer_id, session_id, product_part_number, 
        max(case when event_type = 'SS-ATC' then 1 else 0 end) as ss_atc,
        max(case when event_type = 'Purchase' then 1 else 0 end ) as purchase
from discovery_sandbox.ad_ca_session_events
where event_type IN ('SS-ATC','Purchase')
group by 1,2,3,4
)
where ss_atc = 1 and purchase = 1 --- ensuring that atleast 1 item added from SS(PLAS) is also purchased eventually by the customer in a single session
group by 1,2;
commit;





/*** Creating orders base table for all CIDs for ongoing measurement ***/

create or replace temp table eligible_cids as 
(select distinct customer_ID from discovery_sandbox.ad_ca_impression_cohort 
union
select distinct customer_ID from discovery_sandbox.ad_ca_engagement_cohort 
union
select distinct customer_ID from discovery_sandbox.ad_ca_purchase_cohort
UNION 
select distinct customer_ID from discovery_sandbox.ad_ca_customer_cohort_map
);



CREATE OR REPLACE TABLE 
  discovery_sandbox.ad_ca_pcid_orders AS
  ( SELECT 
      order_id 
      , a.customer_id 
      , DATE(order_placed_dttm) AS order_date 
      , part_number 
      , order_order_line_status 
      , order_status_description 
      , order_first_order_placed_flag 
      , subscription_ID 
      , order_submitted_by 
      , business_channel_name 
      , order_line_quantity 
      , order_line_each_price 
      , order_line_total_price 
      , order_line_ship_charge 
      , order_line_total_adjustment 
      , refund_credit_amount 
      , appeasement_credit_amount 
      , raw_product_total_cost 
      , raw_product_total_cost_provisional 
      , inventory_adjustment_cost 
      , rebate_amount 
      , autoship_rebate_amount 
      , order_line_royalty_amount 
      , shipment_net_charge_allocation 
      , shipment_net_charge_allocation_default 
      , corrugate_cost 
      , ((((((((((((COALESCE(order_line_total_price, 0::numeric(18,0)) + COALESCE(order_line_ship_charge, 0::numeric(18,0))) + COALESCE(order_line_total_adjustment, 0::numeric(18,0))) 
      + COALESCE(refund_credit_amount, 0::numeric(18,0))) + COALESCE(appeasement_credit_amount, 0::numeric(18,0))) + COALESCE( 
      CASE WHEN (COALESCE(raw_product_total_cost, 0::numeric(18,0)) = 0::numeric(18,0))
        THEN raw_product_total_cost_provisional
        ELSE raw_product_total_cost
      END, 0::numeric(18,0)) 
      ) + COALESCE(inventory_adjustment_cost, 0::numeric(18,0))) + COALESCE(rebate_amount, 0::numeric(18,0))) + COALESCE(autoship_rebate_amount, 0::numeric(18,0))) 
      + COALESCE(order_line_royalty_amount, 0::numeric(18,0))) + COALESCE(shipment_net_charge_allocation, 0::numeric(18,0))) 
      + COALESCE(shipment_net_charge_allocation_default, 0::numeric(18,0))) + COALESCE(corrugate_cost, 0::numeric(18,0))) AS gross_margin 
      , financial_calendar_reporting_year 
      , financial_calendar_reporting_period
    FROM ecom.order_line a
    INNER JOIN eligible_cids b
    ON a.customer_id = b.customer_id
    where DATE(a.order_placed_dttm) >= '2021-02-01'   
--    AND order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
  ) 
;commit;



--delete from discovery_sandbox.ad_ca_pcid_orders where order_date >= '2023-06-20'; commit;
insert into discovery_sandbox.ad_ca_pcid_orders 
  ( SELECT 
      order_id 
      , a.customer_id 
      , DATE(order_placed_dttm) AS order_date 
      , part_number 
      , order_order_line_status 
      , order_status_description 
      , order_first_order_placed_flag 
      , subscription_ID 
      , order_submitted_by 
      , business_channel_name 
      , order_line_quantity 
      , order_line_each_price 
      , order_line_total_price 
      , order_line_ship_charge 
      , order_line_total_adjustment 
      , refund_credit_amount 
      , appeasement_credit_amount 
      , raw_product_total_cost 
      , raw_product_total_cost_provisional 
      , inventory_adjustment_cost 
      , rebate_amount 
      , autoship_rebate_amount 
      , order_line_royalty_amount 
      , shipment_net_charge_allocation 
      , shipment_net_charge_allocation_default 
      , corrugate_cost 
      , ((((((((((((COALESCE(order_line_total_price, 0::numeric(18,0)) + COALESCE(order_line_ship_charge, 0::numeric(18,0))) + COALESCE(order_line_total_adjustment, 0::numeric(18,0))) 
      + COALESCE(refund_credit_amount, 0::numeric(18,0))) + COALESCE(appeasement_credit_amount, 0::numeric(18,0))) + COALESCE( 
      CASE WHEN (COALESCE(raw_product_total_cost, 0::numeric(18,0)) = 0::numeric(18,0))
        THEN raw_product_total_cost_provisional
        ELSE raw_product_total_cost
      END, 0::numeric(18,0)) 
      ) + COALESCE(inventory_adjustment_cost, 0::numeric(18,0))) + COALESCE(rebate_amount, 0::numeric(18,0))) + COALESCE(autoship_rebate_amount, 0::numeric(18,0))) 
      + COALESCE(order_line_royalty_amount, 0::numeric(18,0))) + COALESCE(shipment_net_charge_allocation, 0::numeric(18,0))) 
      + COALESCE(shipment_net_charge_allocation_default, 0::numeric(18,0))) + COALESCE(corrugate_cost, 0::numeric(18,0))) AS gross_margin 
      , financial_calendar_reporting_year 
      , financial_calendar_reporting_period
    FROM ecom.order_line a
    INNER JOIN eligible_cids b
    ON a.customer_id = b.customer_id
    where DATE(a.order_placed_dttm) >= '2023-06-20'   
--    AND order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
  ) 
;commit;

/**************** 
Mapping behavioral dimensions for each customer at the point of cohort detemrination ******/
/*aggregating 2 years data for overall customer level and also category level based 
on orders prior to beginning of the experiment */

CREATE OR REPLACE TABLE 
  discovery_sandbox.ad_ca_order_level_dimensions AS
SELECT 
  a.customer_ID 
  , COUNT(DISTINCT a.order_id) AS past_purchase_count 
  , COUNT(DISTINCT(b.category_level1 )) AS distinct_cat1_purchased 
  , COUNT(DISTINCT(b.category_level2 )) AS distinct_cat2_purchased 
  , COUNT(DISTINCT(b.category_level3 )) AS distinct_cat3_purchased 
  , MAX(CASE WHEN b.category_level1 = 'Dog' THEN 1 ELSE 0 END) AS cat1_dog_purchased 
  , MAX(CASE WHEN b.category_level1 = 'Cat' THEN 1 ELSE 0 END) AS cat1_cat_purchased
FROM discovery_sandbox.ad_ca_pcid_orders a
LEFT JOIN pdm.product_snapshot b
ON a.part_number = b.part_number
AND a.order_date = b.snapshot_date
WHERE 
  order_date BETWEEN DATEADD(YEAR, -2,'2023-02-12') AND '2023-02-12'
AND order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
GROUP BY 1
;
COMMIT;

--- getting purchase_categories for each CID 
CREATE OR REPLACE TABLE 
  discovery_sandbox.ad_ca_past_purchase_categories AS
  ( SELECT 
      a.customer_id 
      , b.category_level1 
      , b.category_level2 
      , b.category_level3
    FROM discovery_sandbox.ad_ca_pcid_orders a
    LEFT JOIN pdm.product_snapshot b
    ON a.part_number = b.part_number
    AND a.order_date = b.snapshot_date
    WHERE 
      order_date BETWEEN DATEADD(YEAR, -2,'2023-02-12') AND '2023-02-12'
    AND order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
    group by 1,2,3,4
  ) 
;
COMMIT;



/*Final table mapping individual cohorts with dimensions at CID level for metrics computing downstream*/
create or replace table discovery_sandbox.ad_ca_customer_cohort_map_dimension as 
select a.*, 
        case when past_purchase_count < 5 then past_purchase_count::varchar 
        when past_purchase_count >= 5 then '5+'
        else null end as previous_purchase_count,
        b.distinct_cat1_purchased,
        b.distinct_cat2_purchased,
        b.distinct_cat3_purchased,
        case when b.cat1_dog_purchased = 1 then TRUE else FALSE END as cat1_dog_purchased,
        case when b.cat1_cat_purchased = 1 then TRUE else FALSE END as cat1_cat_purchased
from discovery_sandbox.ad_ca_customer_cohort_map a 
left join discovery_sandbox.ad_ca_order_level_dimensions b 
on a.customer_id = b.customer_id ;








/*Revised cohorts combining variant and control to 4 cohorts 
control -bought food and added food from SS 
control - bought food with no SS add 
variant - bouhgt food with attach from SS 
variant - bought food with no SS add */


--- first the helper tables for each kind of action 

create or replace temp table purchased as 
select * 
from discovery_sandbox.ad_ca_first_purchase_events
where event_type = 'Purchase';

create or replace temp table pdp_atc as 
select * 
from discovery_sandbox.ad_ca_first_purchase_events
where event_type = 'PDP-ATC';

create or replace temp table ss_ATC as 
select * 
from discovery_sandbox.ad_ca_first_purchase_events
where event_type = 'SS-ATC';
 
create or replace temp table ss_product_click as 
select * 
from discovery_sandbox.ad_ca_first_purchase_events
where event_type = 'PLAS-ProductClick';
 

create or replace table discovery_sandbox.ad_ca_cohort_features_revised as 
select 
        case when a.product_category_level3 = 'Dry Food' then 1 else 0 end as purchase_dry_food
        , case when a.product_category_level3 = 'Wet Food' then 1 else 0 end as purchase_wet_food
        , case when a.product_category_level2 = 'Treats' then 1 else 0 end as purchase_treats
        , case when a.product_category_level2 = 'Toys' then 1 else 0 end as purchase_toys
        , case when a.product_category_level2 = 'Health & Wellness' then 1 else 0 end as purchase_hw
        , case when a.product_category_level2 = 'Litter & Accessories' then 1 else 0 end as purchase_litter
        , case when a.product_category_level2 = 'Food' then 1 else 0 end as purchase_food_cl2
        , case when b.product_part_number is not null and a.product_category_level3 = 'Dry Food' then 1 else 0 end as pdp_atc_dry_food
        , case when b.product_part_number is not null and a.product_category_level3 = 'Wet Food' then 1 else 0 end as pdp_atc_wet_food
        , case when b.product_part_number is not null and a.product_category_level2 = 'Food' then 1 else 0 end as pdp_atc_food_cl2
        , case when c.product_part_number is not null and a.product_category_level2 = 'Treats' then 1 else 0 end as SS_atc_treat
        , case when c.product_part_number is not null and a.product_category_level2 = 'Toys' then 1 else 0 end as SS_atc_toys
        , case when c.product_part_number is not null and a.product_category_level2 = 'Health & Wellness' then 1 else 0 end as SS_atc_hw
        , case when c.product_part_number is not null and a.product_category_level2 = 'Litter & Accessories' then 1 else 0 end as SS_atc_litter
        , case when c.product_part_number is not null and a.product_category_level3 = 'Wet Food' then 1 else 0 end as SS_atc_wet_food
        , case when c.product_part_number is not null and a.product_category_level3 = 'Dry Food' then 1 else 0 end as SS_atc_dry_food
        , case when d.session_ID is not null then 1 else 0 end as ss_impression
        , case when e.session_ID is not null then 1 else 0 end as ss_atc
        , case when pc.product_part_number is not null then 1 else 0 end as ss_plas_product_click
        , case when c.product_part_number is not null and a.product_category_level2 = 'Food' then 1 else 0 end as SS_atc_food_cl2
        , a.*
from purchased a 
left join pdp_atc b 
on a.session_id = b.session_id 
and a.product_part_number = b.product_part_number 
LEFT JOIN ss_atc c 
on a.session_id = c.session_id 
and a.product_part_number = c.product_part_number 
--- below join to get if the session had SS impression 
LEFT JOIN (select distinct session_id
 from discovery_sandbox.ad_ca_raw_exp_sessions
 where event_action = 'impression' and event_label IN ('upsell-drawer','promo-drawer')) d 
 on a.session_ID = d.session_ID
 -- below snippet to get if session had any SS adds from PLAS
LEFT JOIN (select distinct session_id
 from discovery_sandbox.ad_ca_raw_exp_sessions
 where event_action = 'addToCart' and event_label IN ('upsell_10_control')) e 
 on a.session_ID = e.session_ID
 LEFT JOIN ss_product_click pc 
 on a.session_id = pc.session_id 
and a.product_part_number = pc.product_part_number 
;
commit;




/* using the above features for unique flags denoting a single cohort */ 
create or replace table discovery_sandbox.ad_ca_customer_level_cohort_flags_revised as 
select test_arm, customer_id,  new_customer_flag, active_autoship_flag, 
        case 
        --cohort #1
        when test_arm = 'CONTROL' and sum(purchase_wet_food)>0 and sum(ss_atc_wet_food)>0 
                and sum(purchase_dry_food) > 0 and sum(pdp_atc_dry_food) > 0 then 1 else 0 end  as C1_Control_WetFood_DryFood
        , case 
        --cohort #2
        when test_arm = 'CONTROL' and sum(purchase_wet_food) > 0 and sum(ss_atc_wet_food)>0 and sum(pdp_atc_wet_food) >0 then 1 else 0 end  as C2_Control_WetFood_WetFood
        , case
        --cohort #control new
        when test_arm = 'CONTROL' and (sum(purchase_dry_food) > 0 or sum(purchase_wet_food) > 0) 
                and sum(pdp_atc_wet_food) = 0 and sUm(pdp_atc_dry_food)=0 
                and (sum(ss_atc_wet_food)>0 OR sum(ss_atc_dry_food)>0) then 1 else 0 end as Control_SS_add_food_pdp_nonfood 
        , case 
        --cohort #5 
        when test_arm = 'VARIANT_01' and (sum(purchase_wet_food) > 0 or sum(purchase_dry_food) >0 ) and sum(ss_impression) >0  
                and sum(ss_atc) = 0  then 1 else 0 end  as C5_Variant1_Checkout_wo_engagement
        , case 
        --cohort #6 
        when test_arm = 'VARIANT_01' and sum(purchase_treats) >0 and sum(ss_atc_treat) >0
                and sum(purchase_dry_food) >0 and sum(pdp_atc_dry_food) > 0 then 1 else 0 end  as C6_Variant1_Treats_DryFood
        , case 
        --cohort #7
        when test_arm = 'VARIANT_01' and sum(purchase_treats) >0 and sum(ss_atc_treat) >0
                and sum(purchase_wet_food) >0 and sum(pdp_atc_wet_food) > 0 then 1 else 0 end  as C7_Variant1_Treats_WetFood
        , CASE
        --cohort #8
        when test_arm = 'VARIANT_01' and sum(purchase_toys) >0 and sum(ss_atc_toys) >0
                and sum(purchase_dry_food) >0 and sum(pdp_atc_dry_food) > 0 then 1 else 0 end  as C8_Variant1_Toys_DryFood
        , case 
        --cohort #9
        when test_arm = 'VARIANT_01' and sum(purchase_toys) >0 and sum(ss_atc_toys) >0
                and sum(purchase_wet_food) >0 and sum(pdp_atc_wet_food) > 0 then 1 else 0 end  as C9_Variant1_Toys_WetFood
        , case 
        --cohort #10
        when test_arm = 'VARIANT_02' and sum(purchase_litter) >0 and sum(ss_atc_litter) >0
                and sum(purchase_food_cl2) >0 and sum(pdp_atc_food_cl2) > 0 then 1 else 0 end  as C10_Variant2_Litter_Food_Cl2
        , CASE
        when test_arm = 'VARIANT_02' and sum(purchase_hw) >0 and sum(ss_atc_hw) >0
                and sum(purchase_food_cl2) >0 and sum(pdp_atc_food_cl2) > 0 then 1 else 0 end as C11_Variant2_HW_FoodCl2
        ,
        
        ----Revised cohort flags 
        
        CASE WHEN test_arm = 'CONTROL' 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                and   (sum(SS_atc_wet_food)>0 OR sum(ss_atc_dry_food)>0) then 1 else 0 end as new_control_food_SS_add_food      
        ,
        CASE WHEN test_arm = 'CONTROL' 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                and   sum(ss_atc)=1 then 1 else 0 end as new_control_food_SS_add_any    
        ,
        CASE WHEN test_arm = 'CONTROL' 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                and  sum(ss_atc) = 0 then 1 else 0 end as new_control_food_no_SS_add     
        ,
        CASE WHEN test_arm = 'CONTROL' 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                and  sum(ss_atc) = 0 and sum(ss_plas_product_click)>0 then 1 else 0 end as new_control_food_no_SS_add_but_click      
        ,
        CASE WHEN test_arm = 'CONTROL' 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                and  sum(ss_atc) = 0 and sum(ss_plas_product_click) = 0 then 1 else 0 end as new_control_food_no_SS_add_no_click                     
        ,
        CASE WHEN test_arm IN ('VARIANT_01','VARIANT_02') 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                AND (sum(ss_atc_treat)>0 OR  sum(ss_atc_toys) >0 OR sum(ss_atc_litter) >0 OR sum(ss_atc_hw) >0)
                and (sum(purchase_treats) >0 OR sum(purchase_toys) >0 OR sum(purchase_litter) OR sum(purchase_hw) >0)   then 1 else 0 end as new_variant_food_SS_attach 
        ,
        CASE WHEN test_arm IN ('VARIANT_01','VARIANT_02') 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                AND sum(ss_atc) = 0 then 1 else 0 end as new_variant_food_no_ss_add
        ,
        CASE WHEN test_arm IN ('VARIANT_01','VARIANT_02') 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                AND sum(ss_atc) = 0 
                AND sum(ss_plas_product_click) > 0 then 1 else 0 end as new_variant_food_no_ss_add_but_click
        ,
        CASE WHEN test_arm IN ('VARIANT_01','VARIANT_02') 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                AND sum(ss_atc) = 0 
                AND sum(ss_plas_product_click) = 0 then 1 else 0 end as new_variant_food_no_ss_add_no_click 
        ,
        CASE WHEN test_arm IN ('VARIANT_01') 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                AND sum(ss_atc) = 0 
                AND sum(ss_plas_product_click) = 0 then 1 else 0 end as new_variant1_food_no_ss_add_no_click     
        ,
        CASE WHEN test_arm IN ('VARIANT_02') 
                and (sum(purchase_wet_food)>0 OR sum(purchase_dry_food)>0)
                and (sum(pdp_atc_wet_food)>0 OR sum(pdp_atc_dry_food)>0)
                AND sum(ss_atc) = 0 
                AND sum(ss_plas_product_click) = 0 then 1 else 0 end as new_variant2_food_no_ss_add_no_click                        
        
        
        ------- Food CL2 level flags         
        ,
        CASE WHEN test_arm = 'CONTROL' 
                and sum(purchase_food_cl2)>0 and sum(pdp_atc_food_cl2)>0 and sum(SS_atc_food_cl2)>0
                then 1 else 0 end as new_control_food_cl2_SS_add_food_cl2      
        ,
        CASE WHEN test_arm = 'CONTROL' 
                and sum(purchase_food_cl2)>0 and sum(pdp_atc_food_cl2)>0 and sum(SS_atc_food_cl2)=0
                then 1 else 0 end as new_control_food_cl2_SS_no_food_cl2_add
        ,
        CASE WHEN test_arm = 'CONTROL' 
                and sum(purchase_food_cl2)>0 and sum(pdp_atc_food_cl2)>0 and sum(SS_atc)=0
                then 1 else 0 end as new_control_food_cl2_SS_no_add                    
        ,
        CASE WHEN test_arm IN ('VARIANT_01','VARIANT_02') 
                and sum(purchase_food_cl2)>0 and sum(pdp_atc_food_cl2)>0 
                AND (sum(ss_atc_treat)>0 OR  sum(ss_atc_toys) >0 OR sum(ss_atc_litter) >0 OR sum(ss_atc_hw) >0)
                and (sum(purchase_treats) >0 OR sum(purchase_toys) >0 OR sum(purchase_litter) OR sum(purchase_hw) >0)
                then 1 else 0 end as new_variant_food_cl2_SS_attach          
        ,
        CASE WHEN test_arm IN ('VARIANT_01','VARIANT_02') 
                and sum(purchase_food_cl2)>0 and sum(pdp_atc_food_cl2)>0 
                AND sum(ss_atc) = 0 
                then 1 else 0 end as new_variant_food_cl2_no_ss_add                  
                
from discovery_sandbox.ad_ca_cohort_features_revised
group by 1,2,3,4;
commit;


/*
SELECT 
  new_control_food_SS_add_food 
  , new_control_food_no_SS_add 
  , new_control_food_no_SS_add_but_click 
  , new_control_food_no_SS_add_no_click 
  , new_variant_food_SS_attach 
  , new_variant_food_no_ss_add 
  , new_variant_food_no_ss_add_but_click 
  , new_variant_food_no_ss_add_no_click 
  , new_variant1_food_no_ss_add_no_click
  , new_variant2_food_no_ss_add_no_click
  , COUNT(*) 
  , COUNT(DISTINCT customer_ID)
FROM discovery_sandbox.ad_ca_customer_level_cohort_flags_revised
group by 1,2,3,4,5,6,7,8,9,10
order by 1,2,3,4,5,6,7,8,9,10;




select 
        new_control_food_cl2_SS_add_food_cl2, new_control_food_cl2_SS_no_food_cl2_add, new_control_food_cl2_SS_no_add, new_variant_food_cl2_SS_attach, new_variant_food_cl2_no_ss_add, COUNT(DISTINCT customer_ID)
from discovery_sandbox.ad_ca_customer_level_cohort_flags_revised
group by 1,2,3,4,5
order by 1,2,3,4,5;        
*/





/*Creating a cohort map for all customers*/
create or replace table discovery_sandbox.ad_ca_customer_cohort_map_revised as 
(
select customer_id, test_arm, new_customer_flag, active_autoship_flag, 
      case 
        when test_arm = 'CONTROL' and C1_Control_WetFood_DryFood = 1 and C2_Control_WetFood_WetFood = 1 then 'Control(PC): Wet and Dry food' 
        when test_arm = 'CONTROL' and C1_Control_WetFood_DryFood = 1 and C2_Control_WetFood_WetFood = 0 then 'C1: Control - Purchased Wet food (SS) w/ Dry food (ATC)' 
        when test_arm = 'CONTROL' and C1_Control_WetFood_DryFood = 0 and C2_Control_WetFood_WetFood = 1 then 'C2: Control - Purchased Wet food (SS) w/ Wet food (ATC)' 
        when test_arm = 'CONTROL' and CONTROL_SS_ADD_FOOD_PDP_NONFOOD = 1 then 'Control: Purchased Dry/Wet Food (SS) w/ Non-food(ATC)'
        when test_arm = 'CONTROL' then 'Control: Not Mapped'
        when test_arm = 'VARIANT_01' and C5_VARIANT1_CHECKOUT_WO_ENGAGEMENT = 1 then 'C5: Variant1 - Purchased Wet/Dry food w/o SS Engagement'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 1 and C7_VARIANT1_TREATS_WETFOOD = 0 and C8_VARIANT1_TOYS_DRYFOOD = 0 and C9_VARIANT1_TOYS_WETFOOD = 0 then 'C6: Variant1 - Purchased Treats (SS) w/ Dry food (ATC)'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 0 and C7_VARIANT1_TREATS_WETFOOD = 1 and C8_VARIANT1_TOYS_DRYFOOD = 0 and C9_VARIANT1_TOYS_WETFOOD = 0 then 'C7: Variant1 - Purchased Treats (SS) w/ Wet food (ATC)'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 0 and C7_VARIANT1_TREATS_WETFOOD = 0 and C8_VARIANT1_TOYS_DRYFOOD = 1 and C9_VARIANT1_TOYS_WETFOOD = 0 then 'C8: Variant1 - Purchased Toys (SS) w/ Dry food (ATC)'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 0 and C7_VARIANT1_TREATS_WETFOOD = 0 and C8_VARIANT1_TOYS_DRYFOOD = 0 and C9_VARIANT1_TOYS_WETFOOD = 1 then 'C9: Variant1 - Purchased Toys (SS) w/ Wet food (ATC)'
        when test_arm = 'VARIANT_01' and C6_Variant1_Treats_DryFood = 0 and C7_VARIANT1_TREATS_WETFOOD = 0 and C8_VARIANT1_TOYS_DRYFOOD = 0 and C9_VARIANT1_TOYS_WETFOOD = 0 then 'Variant1 - Not mapped'
        when test_arm = 'VARIANT_01' and (C6_Variant1_Treats_DryFood = 1 OR C7_VARIANT1_TREATS_WETFOOD = 1 OR C8_VARIANT1_TOYS_DRYFOOD = 1 OR C9_VARIANT1_TOYS_WETFOOD = 1) then 'Variant1(PC) - Purchased Toys/Treats (SS) w/ Wet/Dry food(ATC)'
        when test_arm = 'VARIANT_02' and C10_VARIANT2_LITTER_FOOD_CL2 = 1 and C11_VARIANT2_HW_FOODCL2 =  0 then 'C10: Variant2 - Purchased Litter(SS) w/ Food (ATC)'
        when test_arm = 'VARIANT_02' and C10_VARIANT2_LITTER_FOOD_CL2 = 0 and C11_VARIANT2_HW_FOODCL2 =  1 then 'C11: Variant2 - Purchased H&W(SS) w/ Food (ATC)'
        when test_arm = 'VARIANT_02' and C10_VARIANT2_LITTER_FOOD_CL2 = 0 and C11_VARIANT2_HW_FOODCL2 =  0 then 'Variant2 - Not mapped'
        else 'Not Mapped' end as cohort_name,
 --- new revised cohorts       
        case 
        when new_control_food_SS_add_food = 1 and new_control_food_no_SS_add = 0 then '3.Control - purchased wet/dry food adding from both PDP & SS' 
        when new_control_food_SS_add_food = 0 and new_control_food_no_SS_add = 1 and new_control_food_no_SS_add_but_click = 1 then '6.Control - purchased wet/dry food(PDP) & SS click only'
        when new_control_food_SS_add_food = 0 and new_control_food_no_SS_add = 1 and new_control_food_no_SS_add_but_click = 0 
                and new_control_food_no_SS_add_no_click = 1 then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
        when new_variant_food_SS_attach = 1 then '1.Variant - purchased wet/dry food(PDP) & Attach item on SS'
        when new_variant_food_SS_attach = 0 and new_variant_food_no_ss_add = 1 and new_variant_food_no_ss_add_but_click = 1 then '5.Variant - purchased wet/dry food(PDP) & SS click only'
        when new_variant_food_SS_attach = 0 and new_variant_food_no_ss_add = 1 and new_variant_food_no_ss_add_but_click = 0 
                and new_variant_food_no_ss_add_no_click = 1 then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
        end as cohort_name_new,
--- revised cohorts broken by variant1 and 2 in no SS engagement
        case 
        when new_control_food_SS_add_food = 1 and new_control_food_no_SS_add = 0 then '4.Control - purchased wet/dry food adding from both PDP & SS' 
        when new_control_food_SS_add_food = 0 and new_control_food_no_SS_add = 1 and new_control_food_no_SS_add_but_click = 1 then '7.Control - purchased wet/dry food(PDP) & SS click only'
        when new_control_food_SS_add_food = 0 and new_control_food_no_SS_add = 1 and new_control_food_no_SS_add_but_click = 0 
                and new_control_food_no_SS_add_no_click = 1 then '5.Control - purchased wet/dry food(PDP) & NO SS engagement'
        when new_variant_food_SS_attach = 1 then '1.Variant - purchased wet/dry food(PDP) & Attach item on SS'
        when new_variant_food_SS_attach = 0 and new_variant_food_no_ss_add = 1 and new_variant_food_no_ss_add_but_click = 1 then '6.Variant - purchased wet/dry food(PDP) & SS click only'
        when new_variant_food_SS_attach = 0 and new_variant_food_no_ss_add = 1 and new_variant_food_no_ss_add_but_click = 0 
                and new_variant_food_no_ss_add_no_click = 1 AND new_variant1_food_no_ss_add_no_click =1 then '2.Variant01 - purchased wet/dry food(PDP) & NO SS engagement'
        when new_variant_food_SS_attach = 0 and new_variant_food_no_ss_add = 1 and new_variant_food_no_ss_add_but_click = 0 
                and new_variant_food_no_ss_add_no_click = 1 AND new_variant2_food_no_ss_add_no_click =1 then '3.Variant02 - purchased wet/dry food(PDP) & NO SS engagement'        
        end as cohort_name_new_variant_split,
        
--- food CL2 based cohorts      
        case 
        when new_control_food_cl2_SS_add_food_cl2 = 1 and new_control_food_cl2_SS_no_add = 0 then 'Control - purchased food (CL2) adding from both PDP & SS' 
        when new_control_food_cl2_SS_add_food_cl2 = 0 and new_control_food_cl2_SS_no_food_cl2_add = 1 and new_control_food_cl2_SS_no_add = 0 then 'Control - purchased food(CL2-PDP) & No SS Food add'
        when new_control_food_cl2_SS_add_food_cl2 = 0 and new_control_food_cl2_SS_no_food_cl2_add = 1 and new_control_food_cl2_SS_no_add = 1 then 'Control - purchased food(CL2-PDP) & No SS add'
        when new_variant_food_cl2_SS_attach = 1 then 'Variant - purchased Food (CL2) & Attach item on SS' 
        when new_variant_food_cl2_SS_attach = 0 and new_variant_food_cl2_no_ss_add = 1 then 'Variant - purchased Food (CL2) & No SS add' 
        end as cohort_name_new_cl2
                       
from discovery_sandbox.ad_ca_customer_level_cohort_flags_revised
where test_arm <> 'FALLBACK'
);
commit;



select top 10 * from 
discovery_sandbox.ad_ca_customer_cohort_map_revised
where cohort_name = 'Variant2 - Not mapped'
and cohort_name_new = '1.Variant - purchased wet/dry food(PDP) & Attach item on SS';

select * from discovery_sandbox.ad_ca_customer_level_cohort_flags_revised
where customer_id = '197392610';


select * from discovery_sandbox.ad_ca_cohort_features_revised
where customer_id = '197392610';

/*Final table mapping individual cohorts with dimensions at CID level for metrics computing downstream*/
create or replace table discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised as 
select a.*, 
        case when past_purchase_count < 5 then past_purchase_count::varchar 
        when past_purchase_count >= 5 then '5+'
        else null end as previous_purchase_count,
        b.distinct_cat1_purchased,
        b.distinct_cat2_purchased,
        b.distinct_cat3_purchased,
        case when b.cat1_dog_purchased = 1 then TRUE else FALSE END as cat1_dog_purchased,
        case when b.cat1_cat_purchased = 1 then TRUE else FALSE END as cat1_cat_purchased
from discovery_sandbox.ad_ca_customer_cohort_map_revised a 
left join discovery_sandbox.ad_ca_order_level_dimensions b 
on a.customer_id = b.customer_id ;
commit;



/***** logic/queries for getting the metrics ***/ 
/*Part 1: order related metrics 
Part 2: attach metrics as reported today */


create or replace temp table eligible_cids as 
(select distinct customer_ID from discovery_sandbox.ad_ca_impression_cohort 
union
select distinct customer_ID from discovery_sandbox.ad_ca_engagement_cohort 
union
select distinct customer_ID from discovery_sandbox.ad_ca_purchase_cohort
UNION 
select distinct customer_ID from discovery_sandbox.ad_ca_customer_cohort_map
);


CREATE 
OR 
REPLACE TABLE discovery_sandbox.ad_ca_customer_metrics_revised AS
( SELECT 
    a.customer_id 
    , b.order_date 
    , b.order_ID
    , cd.financial_calendar_reporting_year AS reporting_year
    , cd.financial_calendar_reporting_week_of_year AS reporting_week
    , cd.chewy_financial_reporting_period AS financial_period
    , b.part_number
    , b.order_line_quantity
    , (b.order_line_quantity*b.order_line_each_price)::float as revenue
    , b.order_line_total_price
    , p.category_level1
    , p.category_level2
    , p.category_level3
    ---for autoship metrics 
    , eo.order_first_auto_reorder_order_placed_flag
    , b.business_channel_name    
    ---- for attach metrics 
    , CASE
            WHEN p.category_level2 IN (
                'Treats', 'Litter & Accessories', 'Health & Wellness', 'Toys'
                )
            THEN TRUE 
            ELSE FALSE 
        END AS order_product_attach_flag
     , CASE When p.category_level1 = 'Cat' AND p.category_level2 = 'Food' THEN TRUE ELSE FALSE end as cat_food_order_flag
     , CASE When p.category_level1 = 'Dog' AND p.category_level2 = 'Food' THEN TRUE ELSE FALSE end as dog_food_order_flag
     , b.order_order_line_status
     , eo.order_status
FROM /*discovery_sandbox.ad_ca_customer_cohort_map_dimension*/ eligible_cids a
INNER JOIN discovery_sandbox.ad_ca_pcid_orders b
ON a.customer_id = b.customer_id
left join pdm.product_snapshot p
on b.part_number = p.part_number
and b.order_date = p.snapshot_date
LEFT JOIN cdm.common_date cd
ON b.order_date = cd.common_date_dttm 
LEFT JOIN ecom.orders eo
on b.order_id = eo.order_id
where b.order_date >= '2023-01-01' 
--AND order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
);
commit;


/*CCP Base table */
create table discovery_sandbox.ad_ca_cohort_ccp as 
(select a.*
from mkt_sandbox.tbl_customer_ccp_detail_v2 a 
inner join discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised b --- cohort table at a CID level
on a.customer_id = b.customer_id 
where a.order_date between '2023-01-01' and '2023-06-30'
and b.cohort_name_new is not null);
commit;

 

/*** Creating orders base table**/ 
create or replace temp table order_base as (
select a.*, 
        b.order_id, 
        b.part_number, 
        b.reporting_year,
        b.reporting_week,
        b.order_date, 
        b.financial_period,
        to_char(b.order_date,'YYYY-MM') as calendar_month, 
        b.order_line_quantity, 
        b.revenue,
        b.order_line_total_price, 
        b.revenue/nullifzero(b.order_line_quantity) as order_line_each_price, 
        b.category_level1,
        b.category_level2,
        b.category_level3,
        b.order_first_auto_reorder_order_placed_flag,
        b.business_channel_name,
        b.order_product_attach_flag, 
        b.cat_food_order_flag, 
        b.dog_food_order_flag,
        b.order_order_line_status,
        b.order_status,
        p.merch_classification1 as mc1,
        p.merch_classification2 as mc2,
        p.merch_classification3 as mc3,
---- attach rate related dimensions at order level 
        max(cat_food_order_flag) over(partition by b.order_id) as cat_food_order,
        max(dog_food_order_flag) over(partition by b.order_id) as dog_food_order,
        max(order_product_attach_flag) over(partition by b.order_id) as order_has_attach        
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised  a 
INNER join discovery_sandbox.ad_ca_customer_metrics_revised b 
on a.customer_id = b.customer_id 
LEFT JOIN pdm.product_snapshot p 
on b.part_number = p.part_number 
and b.order_date = p.snapshot_date 
where order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
and test_arm != 'FALLBACK'
and business_channel_name IN ('Web','AutoReorder')
)
;

--/*Splitting by dog food order or cat food order customers */
create or replace temp table x as 
(select customer_id, 
        max(case when product_category_level1 = 'Dog' then true else false end) as dog_food_order_flag, 
        max(case when product_category_level1 = 'Cat' then true else false end) as cat_food_order_flag 
from discovery_sandbox.ad_ca_cohort_features_revised group by 1 );



select /*cohort_name_new*/
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite, 
        case when previous_purchase_count is not null then 'Existing' else 'New' end as new_existing_customer,
        count(distinct customer_ID) as customers, count(*)
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised
group by 1,2
order by 1,2;




select  
--        cohort_name_new,
        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
--        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,                
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        count(distinct a.customer_ID) as customer_count,
        count(distinct order_id) as order_count,
        sum(order_line_quantity) as total_units,
        count(distinct(case when category_level2 in ('Toys','Treats') then order_id end)) as order_count_toys_treats,
            
        count(distinct(case when cat_food_order = TRUE and order_has_attach = TRUE then order_id end)) as cat_food_attach_orders,
        count(distinct(case when cat_food_order = TRUE then order_id end)) as cat_food_orders,
        
        count(distinct(case when dog_food_order = TRUE and order_has_attach = TRUE then order_id end)) as dog_food_attach_orders,
        count(distinct(case when dog_food_order = TRUE then order_id end)) as dog_food_orders
        
from order_base a 
where cohort_name_new is not null
--and order_date < '2023-06-23'
group by 1,2,3,4,5;






--- by CL2

select  
--        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,                
        category_level2,
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units,
        count(distinct order_id) as order_count,
        count(distinct a.customer_id) as customers
        
from order_base a 
where cohort_name_new is not null
--and order_date < '2023-06-23'
group by 1,2,3,4,5;




select  
--        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,                
        MC2,
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units,
        count(distinct order_id) as order_count,
        count(distinct a.customer_id) as customers
        
from order_base a 
where cohort_name_new is not null
--and order_date < '2023-06-23'
group by 1,2,3,4,5;




--- frequence of orders for a CL2 item
select  
--        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,                
        category_level2,
        count(distinct order_id) as order_count,
        count(distinct a.customer_id) as customers
        
from order_base a 
where cohort_name_new is not null
and category_level2 in ('Toys','Treats','Health & Wellness','Litter & Accessories','Food')
and order_date between '2023-01-01' and '2023-06-30'
--and order_date < '2023-06-23'
group by 1,2,3,4,5;



/* customers engaging with smartself purchase more frequently on the website */

select 
        
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite, 
         count(distinct a.customer_id) as cohort_customer_count, 
         count(distinct( case when b.business_channel_name = 'AutoReorder' then order_ID end)) as as_autoorder_Count,
         count(distinct( case when b.business_channel_name = 'AutoReorder' then b.customer_ID end)) as as_autoorder_customers,
         count(distinct( case when b.business_channel_name = 'Web' then order_ID end)) as web_order_Count,
         count(distinct( case when b.business_channel_name = 'Web' then b.customer_ID end)) as web_order_customers         
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised  a 
left join order_base b
on a.customer_id = b.customer_ID 
where a.cohort_name_new is not null
and b.order_date between '2023-03-01' and '2023-06-30'
and b.business_channel_name = 'Web'
group by 1;


/*** Return frequency ***/
select 
        to_char(order_date,'YYYY-MM') as reporting_month,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
         count(distinct customer_ID) as ordering_customers 
from order_base 
where cohort_name_new is not null
group by 1,2;



/*Getting all Autoship Subscriptions with status for cohort group */
CREATE TABLE 
  discovery_sandbox.ad_ca_revised_cohort_as_subs AS
SELECT 
  a.* 
  , b.subscription_ID AS autoship_sub_id 
  , b.status 
  , b.start_dttm 
  , b.cancel_dttm 
  , b.business_channel 
  , b.total_quantity 
  , b.subscription_ID 
  , b.one_time_flag
FROM discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised a
INNER JOIN cdm.subscriptions b
ON a.customer_id = b.customer_id 
;
COMMIT;


/*** Getting AS Subscriptions and cancellations by cohort over time ***/

select b.calendar_month, 
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
         case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,           
        count(distinct(case when a.start_dttm < b.start_date and coalesce(a.cancel_dttm,current_date)> b.start_date then autoship_sub_id end)) as active_subs_bom,
        count(distinct(case when a.start_dttm < b.end_date and coalesce(a.cancel_dttm,current_date)> b.end_date then autoship_sub_id end)) as active_subs_eom,
        count(distinct(case when a.start_dttm < b.start_date and coalesce(a.cancel_dttm,current_date) between b.start_date and b.end_date then autoship_sub_id end)) as cancellations_in_month,
        count(distinct(case when coalesce(a.start_dttm,current_date) between b.start_date and b.end_date and coalesce(a.cancel_dttm,current_date) >= b.start_date then autoship_sub_id end)) as new_subs_in_month,
        count(distinct (case when autoship_sub_id is not null and a.start_dttm < b.start_date and coalesce(a.cancel_dttm,current_date)> b.end_date then customer_id end)) as customers
from discovery_sandbox.ad_ca_revised_cohort_as_subs a 
join
(SELECT 
  TO_CHAR(common_date_dttm,'YYYY-MM') AS calendar_month 
  , MIN(common_date_dttm) AS start_date 
  , MAX(common_date_dttm) AS end_date
FROM cdm.common_date
WHERE 
  common_date_dttm BETWEEN '2023-01-01' AND '2023-06-30'
GROUP BY 1 
) b
on 1=1
Where a.cohort_name_new is not null  
group by 1,2,3
order by 1,2,3;




/*Autoship start sessions */


select to_char(b.session_date,'YYYY-MM') as calendar_month, 
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,     
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                        
        count(distinct(session_id)) as total_sessions, 
        count(distinct(case when order_first_auto_reorder_order_placed_flag and b.transaction_ID is not null then b.session_ID end)) as autoship_start_sessions
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised  a 
INNER join discovery_sandbox.ad_ca_raw_exp_sessions_full  b 
on a.customer_id = b.customer_id 
INNER JOIN 
( SELECT 
    DISTINCT order_id 
    ,business_channel_name, order_first_auto_reorder_order_placed_flag
  FROM discovery_sandbox.ad_ca_customer_metrics 
  WHERE 
    order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
  AND business_channel_name IN ('Web','AutoReorder')) c 
  ON b.transaction_ID = c.order_id
where b.event_action = 'purchase' and transaction_ID is not null
and  a.cohort_name_new is not null 
group by 1,2,3;





/** New Cat food customers */
select  
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,      
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' then 'Autoship' else 'Non-Autoship' end as order_type,
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,
        case when (x.dog_food_order_flag or cat1_dog_purchased) and (x.cat_food_order_flag OR cat1_cat_purchased) then 'Cat & Dog Customer'
        when (x.dog_food_order_flag or cat1_dog_purchased) and not cat1_cat_purchased and not x.cat_food_order_flag then 'Dog Customer'
        when not cat1_dog_purchased and not x.dog_food_order_flag and (x.cat_food_order_flag OR cat1_cat_purchased) then 'Cat Customer' end as cat_dog_customer,      

--        count(distinct(case when new_customer_flag = TRUE then customer_id end)) as new_customers,
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        count(distinct a.customer_ID) as customer_count,
        count(distinct order_id) as order_count,
        sum(order_line_quantity) as total_units,
        count(distinct(case when category_level2 in ('Toys','Treats') then order_id end)) as order_count_toys_treats,
            
        count(distinct(case when cat_food_order = TRUE and order_has_attach = TRUE then order_id end)) as cat_food_attach_orders,
        count(distinct(case when cat_food_order = TRUE then order_id end)) as cat_food_orders,
        
        count(distinct(case when dog_food_order = TRUE and order_has_attach = TRUE then order_id end)) as dog_food_attach_orders,
        count(distinct(case when dog_food_order = TRUE then order_id end)) as dog_food_orders
        
from order_base a 
left join x 
on a.customer_id = x.customer_id 
where cohort_name_new is not null
group by 1,2,3,4,5;




/**** distinct categories cohort customers bought over time **/

select cohort_name_new_composite, reporting_month, order_type 
        , percentile_cont(0.5) within GROUP(ORDER BY distinct_cat3_purchased) AS p50_cat3_purchased
        , percentile_cont(0.9) within GROUP(ORDER BY distinct_cat3_purchased) AS p90_cat3_purchased
        , percentile_cont(0.5) within GROUP(ORDER BY distinct_cat2_purchased) AS p50_cat2_purchased
        , percentile_cont(0.9) within GROUP(ORDER BY distinct_cat2_purchased) AS p90_cat2_purchased
from         
(select  
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,      
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' then 'Autoship' else 'Non-Autoship' end as order_type,
        customer_Id,
--        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,
--        case when (x.dog_food_order_flag or cat1_dog_purchased) and (x.cat_food_order_flag OR cat1_cat_purchased) then 'Cat & Dog Customer'
--        when (x.dog_food_order_flag or cat1_dog_purchased) and not cat1_cat_purchased and not x.cat_food_order_flag then 'Dog Customer'
--        when not cat1_dog_purchased and not x.dog_food_order_flag and (x.cat_food_order_flag OR cat1_cat_purchased) then 'Cat Customer' end as cat_dog_customer,      

--        count(distinct(case when new_customer_flag = TRUE then customer_id end)) as new_customers,
--        sum(revenue)::numeric(38,4) as SFW_Revenue, 
--        count(distinct a.customer_ID) as customer_count,
--        count(distinct order_id) as order_count,
--        sum(order_line_quantity) as total_units,
--        count(distinct(case when category_level2 in ('Toys','Treats') then order_id end)) as order_count_toys_treats,
--            
--        count(distinct(case when cat_food_order = TRUE and order_has_attach = TRUE then order_id end)) as cat_food_attach_orders,
--        count(distinct(case when cat_food_order = TRUE then order_id end)) as cat_food_orders,
--        
--        count(distinct(case when dog_food_order = TRUE and order_has_attach = TRUE then order_id end)) as dog_food_attach_orders,
--        count(distinct(case when dog_food_order = TRUE then order_id end)) as dog_food_orders
        count(distinct category_level3) as distinct_cat3_purchased,
        count(distinct category_level2) as distinct_cat2_purchased
from order_base a 
--left join x 
--on a.customer_id = x.customer_id 
where cohort_name_new is not null
group by 1,2,3,4)
group by 1,2,3;






/*Autoship subscription rate/ new subscriptions per customer over time post experiment*/

select top 10 * from discovery_sandbox.ad_ca_revised_cohort_as_subs

select b.calendar_month, 
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
         case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,    
         count(distinct(case when a.start_dttm between b.start_date and b.end_date and one_time_flag = true then autoship_sub_id end)) as one_time_new_autoship_starts,        
         count(distinct(case when a.start_dttm between b.start_date and b.end_date and one_time_flag = false then autoship_sub_id end)) as multi_time_new_autoship_starts,
         count(distinct(case when coalesce(a.cancel_dttm,current_date)> b.end_date then autoship_sub_id end)) as active_subs_eom,
         count(distinct(case when a.start_dttm between b.start_date and b.end_date then customer_ID end)) as customers
--        count(distinct(case when a.start_dttm < b.start_date and coalesce(a.cancel_dttm,current_date)> b.start_date then autoship_sub_id end)) as active_subs_bom,
--        count(distinct(case when a.start_dttm < b.end_date and coalesce(a.cancel_dttm,current_date)> b.end_date then autoship_sub_id end)) as active_subs_eom,
--        count(distinct(case when a.start_dttm < b.start_date and coalesce(a.cancel_dttm,current_date) between b.start_date and b.end_date then autoship_sub_id end)) as cancellations_in_month,
--        count(distinct(case when coalesce(a.start_dttm,current_date) between b.start_date and b.end_date and coalesce(a.cancel_dttm,current_date) >= b.start_date then autoship_sub_id end)) as new_subs_in_month,
--        count(distinct (case when autoship_sub_id is not null and a.start_dttm < b.start_date and coalesce(a.cancel_dttm,current_date)> b.end_date then customer_id end)) as customers
from discovery_sandbox.ad_ca_revised_cohort_as_subs a 
join
(SELECT 
  TO_CHAR(common_date_dttm,'YYYY-MM') AS calendar_month 
  , MIN(common_date_dttm) AS start_date 
  , MAX(common_date_dttm) AS end_date
FROM cdm.common_date
WHERE 
  common_date_dttm BETWEEN '2023-01-01' AND '2023-06-30'
GROUP BY 1 
) b
on 1=1
Where a.cohort_name_new is not null  
group by 1,2,3
order by 1,2,3;



/*Getting all autoship subscription snapshots */
create or replace table discovery_sandbox.ad_ca_revised_cohort_as_subs_snapshots as 
select a.customer_id as cohort_customer_id, a.new_customer_flag, a.active_autoship_flag, a.cohort_name_new, a.previous_purchase_count, 
 b.*
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised a 
inner join cdm.subscriptions_snapshot b 
on a.customer_id = b.customer_id 
where a.cohort_name_new is not null 
and b.snapshot_date in ('2023-01-31','2023-02-28','2023-03-31','2023-04-30','2023-05-31','2023-06-30');
commit;


/* Autoship Subscription cancellations */
select to_char(snapshot_date, 'YYYY-MM') as snapshot_month, 
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then subscription_ID end)) as new_sub_starts,   
        count(distinct(case when status = 'Active' and start_dttm > snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) and one_time_flag = true then subscription_ID end)) as new_one_time_sub_starts,   
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then customer_id end)) as new_sub_customers,
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date then subscription_ID end)) as existing_subs,   
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date then customer_id end)) as existing_sub_customers,
        count(distinct(case when status = 'Cancelled' and cancel_dttm is not null and cancel_dttm > dateadd(mon,-1,snapshot_date) then subscription_ID end)) as new_cancel_subs   
from discovery_sandbox.ad_ca_revised_cohort_as_subs_snapshots
group by 1,2,3;

/*Changes in quantity in autoship subscriptions*/
select to_char(snapshot_date, 'YYYY-MM') as snapshot_month, 
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
--        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then subscription_ID end)) as new_sub_starts,   
--        count(distinct(case when status = 'Active' and start_dttm > snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) and one_time_flag = true then subscription_ID end)) as new_one_time_sub_starts,   
--        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then customer_id end)) as new_sub_customers,
        case when total_quantity < 5 then total_quantity::varchar else '5+' end as total_quantity,
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date then subscription_ID end)) as existing_subs   
--        count(distinct(case when status = 'Active' and start_dttm < snapshot_date then customer_id end)) as existing_sub_customers,
--        count(distinct(case when status = 'Cancelled' and cancel_dttm is not null and cancel_dttm > dateadd(mon,-1,snapshot_date) then subscription_ID end)) as new_cancel_subs   
        
from discovery_sandbox.ad_ca_revised_cohort_as_subs_snapshots
group by 1,2,3,4;



/* Customers in cohort creating new subscriptions over months */
select to_char(snapshot_date, 'YYYY-MM') as snapshot_month, 
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
        case when a.previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        count(distinct (a.customer_id)) as cohort_customers, 
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then b.subscription_ID end)) as new_sub_starts,  
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then b.customer_ID end)) as new_sub_customers
        
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised a 
inner join discovery_sandbox.ad_ca_revised_cohort_as_subs_snapshots b 
on a.customer_id = b.customer_id 
group by 1,2,3;




/*Number of orders by cohorts customers post experiment */

create temp table x as 
(select  
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,      
--        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' then 'Autoship' else 'Non-Autoship' end as order_type,
        a.customer_Id,
        count(distinct b.order_ID) as order_count
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised a 
left join order_base b
on a.customer_id = b.customer_id 
and b.order_date between '2023-03-01' and '2023-06-30'
where a.cohort_name_new is not null
group by 1,2,3);


select cohort_name_new_composite, count(distinct customer_id)
from x
group by 1;

select cohort_name_new_composite, order_type 
          , percentile_cont(0.25) within GROUP(ORDER BY coalesce(order_count,0)) AS p25_order_count
        , percentile_cont(0.5) within GROUP(ORDER BY coalesce(order_count,0)) AS p50_order_count
        , percentile_cont(0.9) within GROUP(ORDER BY coalesce(order_count,0)) AS p90_order_count
from         
(select  
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,      
--        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' then 'Autoship' else 'Non-Autoship' end as order_type,
        a.customer_Id,
        count(distinct b.order_ID) as order_count
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised a 
left join order_base b
on a.customer_id = b.customer_id 
and b.order_date between '2023-03-01' and '2023-06-30'
where a.cohort_name_new is not null
group by 1,2,3)
group by 1,2
order by 2,1;




/*Autoship order units by MC1 */
select  
--        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,                
        MC1,
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units,
        count(distinct order_id) as order_count,
        count(distinct a.customer_id) as customers
        
from order_base a 
where cohort_name_new is not null
--and order_date < '2023-06-23'
group by 1,2,3,4,5;




/*Autoship order units by MC2 */
select  
--        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,                
        MC2,
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units,
        count(distinct order_id) as order_count,
        count(distinct a.customer_id) as customers
        
from order_base a 
where cohort_name_new is not null --- for existing customers only 
and (cat_food_order = true or dog_food_order = true)
--and order_date < '2023-06-23'
group by 1,2,3,4,5;




/*Autoship order units by CL2 */
select  
--        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,                
        Category_level2,
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units,
        count(distinct order_id) as order_count,
        count(distinct a.customer_id) as customers
        
from order_base a 
where cohort_name_new is not null --- for existing customers only 
and (cat_food_order = true or dog_food_order = true)
--and order_date < '2023-06-23'
group by 1,2,3,4,5;




/*Orders overall for existing autoship*/

select  
--        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,                
--        MC2,
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units,
        count(distinct order_id) as order_count,
        count(distinct a.customer_id) as customers
        
from order_base a 
where cohort_name_new is not null
and (cat_food_order = true or dog_food_order = true)
and previous_purchase_count is not null
--and order_date < '2023-06-23'
group by 1,2,3,4;




/*CCP Base table */
create table discovery_sandbox.ad_ca_cohort_ccp as 
(select a.*
from mkt_sandbox.tbl_customer_ccp_detail_v2 a 
inner join discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised b --- cohort table at a CID level
on a.customer_id = b.customer_id 
where a.order_date between '2023-01-01' and '2023-06-30'
and b.cohort_name_new is not null);
commit;

/*CCP  summary */
select case 
                when b.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when b.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when b.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when b.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
         to_char(order_date,'YYYY-MM') as reporting_month,
         case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite, 
         count(distinct a.order_id) as order_count,
         count(distinct a.customer_ID) as customers, 
         sum(a.order_ccp) as ccp,
         sum(order_transactional_cp) as transactional_cp,
         sum(order_dsi) as dsi
from
discovery_sandbox.ad_ca_cohort_ccp a 
inner join discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised b 
on a.customer_id = b.customer_id 
INNER JOIN ecom.orders c 
on a.order_id = c.order_id
where a.order_date between '2023-01-01' and '2023-06-30'
and b.cohort_name_new is not null
and c.business_channel_name IN ('Web','AutoReorder')
group by 1,2,3;


select * from ecom.orders where order_id = '1334459489';



select case 
                when b.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when b.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when b.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when b.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,         
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
         to_char(order_date,'YYYY-MM') as reporting_month,
         count(distinct order_id) as order_count,
         count(distinct a.customer_ID) as customers, 
         sum(order_ccp) as ccp,
         sum(order_transactional_cp) as transactional_cp,
         sum(order_dsi) as dsi
from
discovery_sandbox.ad_ca_cohort_ccp a 
inner join discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised b 
on a.customer_id = b.customer_id 
lEft join ecom.orders c 
on a.order_id = c.order_id
where a.order_date between '2023-01-01' and '2023-06-30'
and b.cohort_name_new is not null
and a.business_channel_name ='Web'
group by 1,2,3;





/*New to Categories (CL2) for Customers in cohorts */

select order_month, cohort_name_new_composite, a.new_existing_customer, a.category_level2, 
        count(distinct a.customer_ID) as ordering_customers,  
        count(distinct case when b.customer_ID is  null then a.customer_ID end) as new_to_cl2_customers
from 
(select to_char(order_date,'YYYY-MM') as order_month, 
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,   
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        a.customer_id,
        a.category_level2, 
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units
from order_base a
where a.cohort_name_new is not null
and a.order_date between '2023-02-13' and '2023-02-27'
group by 1,2,3,4,5
) a 
left join         
(select distinct customer_id, category_level2 from discovery_sandbox.ad_ca_past_purchase_categories) b 
on a.customer_id = b.customer_id 
and a.category_level2 = b.category_level2
group by 1,2,3,4;








/*New to Categories (CL2) for Customers in cohorts by month */

select order_month, cohort_name_new_composite, a.new_existing_customer, a.category_level2, 
        count(distinct a.customer_ID) as ordering_customers,  
        count(distinct case when b.customer_ID is  null then a.customer_ID end) as new_to_cl2_customers
from 
(select to_char(order_date,'YYYY-MM') as order_month, 
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,   
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        a.customer_id,
        a.category_level2, 
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units
from order_base a
where a.cohort_name_new is not null
and a.order_date between '2023-02-13' and '2023-02-27'
group by 1,2,3,4,5
) a 
left join         
(select distinct customer_id, category_level2 from discovery_sandbox.ad_ca_past_purchase_categories) b 
on a.customer_id = b.customer_id 
and a.category_level2 = b.category_level2
group by 1,2,3,4

UNION ALL 

select order_month, cohort_name_new_composite, a.new_existing_customer, a.category_level2, 
        count(distinct a.customer_ID) as ordering_customers,  
        count(distinct case when b.customer_ID is  null then a.customer_ID end) as new_to_cl2_customers
from 
(select to_char(order_date,'YYYY-MM') as order_month, 
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,   
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        a.customer_id,
        a.category_level2, 
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units
from order_base a
where a.cohort_name_new is not null
and a.order_date between '2023-03-01' and '2023-03-31'
group by 1,2,3,4,5
) a 
left join         
(select distinct customer_id, category_level2 from discovery_sandbox.ad_ca_past_purchase_categories
UNION 
select distinct customer_id, category_level2 from order_base where order_date <'2023-03-01') b 
on a.customer_id = b.customer_id 
and a.category_level2 = b.category_level2
group by 1,2,3,4

UNION ALL 

select order_month, cohort_name_new_composite, a.new_existing_customer, a.category_level2, 
        count(distinct a.customer_ID) as ordering_customers,  
        count(distinct case when b.customer_ID is  null then a.customer_ID end) as new_to_cl2_customers
from 
(select to_char(order_date,'YYYY-MM') as order_month, 
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,   
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        a.customer_id,
        a.category_level2, 
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units
from order_base a
where a.cohort_name_new is not null
and a.order_date between '2023-04-01' and '2023-04-30'
group by 1,2,3,4,5
) a 
left join         
(select distinct customer_id, category_level2 from discovery_sandbox.ad_ca_past_purchase_categories
UNION 
select distinct customer_id, category_level2 from order_base where order_date <'2023-04-01') b 
on a.customer_id = b.customer_id 
and a.category_level2 = b.category_level2
group by 1,2,3,4

UNION ALL 

select order_month, cohort_name_new_composite, a.new_existing_customer, a.category_level2, 
        count(distinct a.customer_ID) as ordering_customers,  
        count(distinct case when b.customer_ID is  null then a.customer_ID end) as new_to_cl2_customers
from 
(select to_char(order_date,'YYYY-MM') as order_month, 
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,   
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        a.customer_id,
        a.category_level2, 
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units
from order_base a
where a.cohort_name_new is not null
and a.order_date between '2023-05-01' and '2023-05-31'
group by 1,2,3,4,5
) a 
left join         
(select distinct customer_id, category_level2 from discovery_sandbox.ad_ca_past_purchase_categories
UNION 
select distinct customer_id, category_level2 from order_base where order_date <'2023-05-01') b 
on a.customer_id = b.customer_id 
and a.category_level2 = b.category_level2
group by 1,2,3,4

UNION ALL 

select order_month, cohort_name_new_composite, a.new_existing_customer, a.category_level2, 
        count(distinct a.customer_ID) as ordering_customers,  
        count(distinct case when b.customer_ID is  null then a.customer_ID end) as new_to_cl2_customers
from 
(select to_char(order_date,'YYYY-MM') as order_month, 
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,   
        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        a.customer_id,
        a.category_level2, 
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
        sum(order_line_quantity) as total_units
from order_base a
where a.cohort_name_new is not null
and a.order_date between '2023-06-01' and '2023-06-30'
group by 1,2,3,4,5
) a 
left join         
(select distinct customer_id, category_level2 from discovery_sandbox.ad_ca_past_purchase_categories
UNION 
select distinct customer_id, category_level2 from order_base where order_date <'2023-06-01') b 
on a.customer_id = b.customer_id 
and a.category_level2 = b.category_level2
group by 1,2,3,4 ;




/** overall revenue and sales for all customers who saw smartshelf during experiment */ 


select a.test_arm,
        to_char(b.order_date,'YYYY-MM') as order_month,
        sum(b.order_line_quantity*b.order_line_each_price)::numeric(38,2) as SFW_Revenue, 
        count(distinct a.customer_ID) as customer_count,
        count(distinct order_id) as order_count,
        sum(order_line_quantity) as total_units/*,
        count(distinct(case when category_level2 in ('Toys','Treats') then order_id end)) as order_count_toys_treats,
            
        count(distinct(case when cat_food_order = TRUE and order_has_attach = TRUE then order_id end)) as cat_food_attach_orders,
        count(distinct(case when cat_food_order = TRUE then order_id end)) as cat_food_orders,
        
        count(distinct(case when dog_food_order = TRUE and order_has_attach = TRUE then order_id end)) as dog_food_attach_orders,
        count(distinct(case when dog_food_order = TRUE then order_id end)) as dog_food_orders*/
from 
(select distinct test_arm, customer_ID 
from 
discovery_sandbox.ad_ca_impression_cohort
where test_arm != 'FALLBACK'
) a inner join 
discovery_sandbox.ad_ca_pcid_orders b 
on a.customer_id = b.customer_id 
where b.order_date between '2023-02-01' and '2023-06-30'
group by 1,2;




/* Base table for all orders by all customers from th experiment*/

CREATE 
OR 
REPLACE TABLE discovery_sandbox.ad_ca_impression_customer_orders_revised AS
( SELECT 
    a.customer_id 
    , a.test_arm
    , b.order_date 
    , b.order_ID
    , cd.financial_calendar_reporting_year AS reporting_year
    , cd.financial_calendar_reporting_week_of_year AS reporting_week
    , cd.chewy_financial_reporting_period AS financial_period
    , b.part_number
    , b.order_line_quantity
    , (b.order_line_quantity*b.order_line_each_price)::float as revenue
    , b.order_line_total_price
    , p.category_level1
    , p.category_level2
    , p.category_level3
    ---for autoship metrics 
    , eo.order_first_auto_reorder_order_placed_flag
    , b.business_channel_name    
    ---- for attach metrics 
    , CASE
            WHEN p.category_level2 IN (
                'Treats', 'Litter & Accessories', 'Health & Wellness', 'Toys'
                )
            THEN TRUE 
            ELSE FALSE 
        END AS order_product_attach_flag
     , CASE When p.category_level1 = 'Cat' AND p.category_level2 = 'Food' THEN TRUE ELSE FALSE end as cat_food_order_flag
     , CASE When p.category_level1 = 'Dog' AND p.category_level2 = 'Food' THEN TRUE ELSE FALSE end as dog_food_order_flag
     , b.order_order_line_status
     , eo.order_status
FROM discovery_sandbox.ad_ca_impression_cohort a
INNER JOIN discovery_sandbox.ad_ca_pcid_orders b
ON a.customer_id = b.customer_id
left join pdm.product_snapshot p
on b.part_number = p.part_number
and b.order_date = p.snapshot_date
LEFT JOIN cdm.common_date cd
ON b.order_date = cd.common_date_dttm 
LEFT JOIN ecom.orders eo
on b.order_id = eo.order_id
where b.order_date >= '2023-01-01' 
--AND order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
);
commit;


/*** Creating orders base table for overall group control vs variant1 vs variant2**/ 
create or replace temp table order_base_total as (
select b.customer_id, b.test_arm, 
        b.order_id, 
        b.part_number, 
        b.reporting_year,
        b.reporting_week,
        b.order_date, 
        b.financial_period,
        to_char(b.order_date,'YYYY-MM') as calendar_month, 
        b.order_line_quantity, 
        b.revenue,
        b.order_line_total_price, 
        b.revenue/nullifzero(b.order_line_quantity) as order_line_each_price, 
        b.category_level1,
        b.category_level2,
        b.category_level3,
        b.order_first_auto_reorder_order_placed_flag,
        b.business_channel_name,
        b.order_product_attach_flag, 
        b.cat_food_order_flag, 
        b.dog_food_order_flag,
        b.order_order_line_status,
        b.order_status,
        p.merch_classification1 as mc1,
        p.merch_classification2 as mc2,
        p.merch_classification3 as mc3,
---- attach rate related dimensions at order level 
        max(cat_food_order_flag) over(partition by b.order_id) as cat_food_order,
        max(dog_food_order_flag) over(partition by b.order_id) as dog_food_order,
        max(order_product_attach_flag) over(partition by b.order_id) as order_has_attach        
from discovery_sandbox.ad_ca_impression_customer_orders_revised b 
LEFT JOIN pdm.product_snapshot p 
on b.part_number = p.part_number 
and b.order_date = p.snapshot_date 
where order_order_line_status NOT IN ('X', 'P', 'J') --- to remove cancellations
and test_arm != 'FALLBACK'
and business_channel_name IN ('Web','AutoReorder')
)
;


select test_arm,
        to_char(order_date,'YYYY-MM') as order_month,
        case when business_channel_name = 'AutoReorder' then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,
        sum(order_line_quantity*order_line_each_price )::numeric(38,2) as SFW_Revenue, 
        count(distinct customer_ID) as customer_count,
        count(distinct order_id) as order_count,
        sum(order_line_quantity) as total_units,
        count(distinct(case when category_level2 in ('Toys','Treats') then order_id end)) as order_count_toys_treats,
            
        count(distinct(case when cat_food_order = TRUE and order_has_attach = TRUE then order_id end)) as cat_food_attach_orders,
        count(distinct(case when cat_food_order = TRUE then order_id end)) as cat_food_orders,
        
        count(distinct(case when dog_food_order = TRUE and order_has_attach = TRUE then order_id end)) as dog_food_attach_orders,
        count(distinct(case when dog_food_order = TRUE then order_id end)) as dog_food_orders
from 
order_base_total b
where b.order_date between '2023-02-01' and '2023-06-30'
group by 1,2,3;




/*Getting incremental revenue for the first attach experiment */

--base table with all experiment sessions from the experiment   DISCOVERY_SANDBOX.AD_CA_RAW_EXP_SESSIONS
-- re run exp_base temp table up top in the script for getting all sessions in experiment 

/*Incremental revenue excluding substitution at CL3 */

CREATE or replace temp TABLE base_session_data AS
SELECT 
  b.test_arm 
  , a.*
FROM discovery_sandbox.prd_f_d_expr_metrics_base a
INNER JOIN exp_base b
ON a.session_id = b.session_id
where b.test_arm != 'FALLBACK'
; 




--- getting all add to carts for the exp sessions 
create or replace temp table pdp_add_to_Carts as
(select distinct a.test_arm, a.session_ID, a.page_type, a.part_number, b.category_level1,b.category_level2,b.category_level3, a.widget_name,
case when widget_name = 'upsell_10_control' then 1 else 0 end as plas_ss_add
from base_session_data a 
left join pdm.product_snapshot b 
on a.part_number = b.part_number
and a.session_date = b.snapshot_date
where add_to_cart >0
);


--- getting all sessions level checkout parts and revenue info
create or replace temp table checkouts as 
(select a.test_arm, a.channel, a.device_category, 
        case when a.new_customer_flag = true then 'Y' else 'N' end as new_customer_flag,
        case when a.active_autoship_flag = true then 'Y' else 'N' end as active_autoship_flag,
--        case when a.session_auth_flag = true then 'Y' else 'N' end as session_auth_flag,
        a.session_ID, a.session_DAte, 
        a.transaction_ID, 
        a.customer_id,
        a.page_type, a.part_number, 
        a.order_sales, a.order_unit,
        a.order_gross_margin, 
        b.category_level1,b.category_level2,b.category_level3
from base_session_data a 
left join pdm.product_snapshot b 
on a.part_number = b.part_number
and a.session_date = b.snapshot_date
where transaction_ID is not null);



--- mapping checked out item if they were added from PLAS or not 
create or replace temp table checkout_plas as 
select a.*, b.plas_ss_add
from checkouts a 
left join (select session_ID, part_number, case when sum(plas_ss_add)> 0 then 1 else 0 end as plas_ss_add from  pdp_add_to_carts group by 1,2) b 
on a.session_id = b.session_id  and a.part_number = b.part_number;


---getting items dropped from cart during checkout in a session 
create or replace temp table items_dropped_from_cart as 
(select a.session_ID, a.part_number, a.category_level1,a.category_level2,a.category_level3, a.widget_name
from pdp_add_to_carts a 
left join checkouts b 
on a.session_id = b.session_id  and a.part_number = b.part_number
where b.part_number is null);



---- for plas add item was there an atc of the same CL3 that was not checked out - if so flag them 
-- as substituted ?
CREATE 
OR 
REPLACE temp TABLE final AS
SELECT 
  a.* 
  , CASE WHEN b.category_level3 IS NOT NULL THEN 'Yes' ELSE 'No' END AS substituted
FROM checkout_plas a
LEFT JOIN 
  ( SELECT 
      DISTINCT session_ID 
      ,category_level1 
      ,category_level2 
      , category_level3 
    FROM items_dropped_from_cart) b
ON a.session_id = b.session_id
AND a.category_level3 = b.category_level3;



---Overall level incremental revenue 
SELECT 
  test_arm 
  , SUM(order_sales) AS total_revenue 
  , SUM(CASE WHEN plas_ss_add =1 AND substituted = 'No' THEN order_sales END) AS incremental_revenue
  , SUM(order_gross_margin) AS total_gross_margin 
  , SUM(CASE WHEN plas_ss_add =1 AND substituted = 'No' THEN order_gross_margin END) AS incremental_gross_margin
  , count(distinct customer_ID) as purchasing_customers
FROM final
GROUP BY 1 
;



select top 10 * from discovery_sandbox.ad_ca_revised_cohort_as_subs_snapshots;


/* Customers in cohort creating new subscriptions over months */
select to_char(snapshot_date, 'YYYY-MM') as snapshot_month, 
        case 
                when a.cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when a.cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when a.cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when a.cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
        case when a.previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        count(distinct (a.customer_id)) as cohort_customers, 
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then b.subscription_ID end)) as new_sub_starts,  
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then b.customer_ID end)) as new_sub_customers
        
from discovery_sandbox.ad_ca_customer_cohort_map_dimension_revised a 
inner join discovery_sandbox.ad_ca_revised_cohort_as_subs_snapshots b 
on a.customer_id = b.customer_id 
group by 1,2,3;






/* Autoship Subscription cancellations */
select to_char(snapshot_date, 'YYYY-MM') as snapshot_month, 
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,
--        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer, 
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then subscription_ID end)) as new_sub_starts,   
        count(distinct(case when status = 'Active' and start_dttm > snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) and one_time_flag = true then subscription_ID end)) as new_one_time_sub_starts,   
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date and start_dttm > dateadd(mon,-1,snapshot_date) then customer_id end)) as new_sub_customers,
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date then subscription_ID end)) as existing_subs,   
        count(distinct(case when status = 'Active' and start_dttm < snapshot_date then customer_id end)) as existing_sub_customers,
        count(distinct(case when status = 'Cancelled' and cancel_dttm is not null and cancel_dttm > dateadd(mon,-1,snapshot_date) then subscription_ID end)) as new_cancel_subs   
from discovery_sandbox.ad_ca_revised_cohort_as_subs_snapshots
group by 1,2,3;








/*Cohort level UPO, AOV and ASP stat sig measurement attributes */


select 
        cohort_name_new_composite as cohort_name, 
        reporting_month, 
        sum(total_units)/count(distinct order_id) as upo,
        stddev(total_units)/* over(partition by cohort_name, reporting_month)*/ as upo_std,
        sum(sfw_revenue)/count(distinct order_id) as aov,
        stddev(sfw_revenue) /*over(partition by cohort_name, reporting_month)*/ as aov_std,
        sum(sfw_revenue)/sum( total_units) as ASP,
        stddev(order_ASP) /*over(partition by cohort_name, reporting_month)*/ as asp_std,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as ordering_customers
from 
(select  
--        cohort_name_new,
        case 
                when cohort_name_new in ('1.Variant - purchased wet/dry food(PDP) & Attach item on SS','5.Variant - purchased wet/dry food(PDP) & SS click only') then '1. Variant purchased wet/dry food(PDP) with SS engagement'
                when cohort_name_new in ('2.Variant - purchased wet/dry food(PDP) & NO SS engagement') then '2.Variant - purchased wet/dry food(PDP) & NO SS engagement'
                when cohort_name_new in ('3.Control - purchased wet/dry food adding from both PDP & SS','6.Control - purchased wet/dry food(PDP) & SS click only') then '3.Control - purchased wet/dry food with SS engagement'
                when cohort_name_new in ('4.Control - purchased wet/dry food(PDP) & NO SS engagement') then '4.Control - purchased wet/dry food(PDP) & NO SS engagement'
         end as cohort_name_new_composite,       
        to_char(order_date,'YYYY-MM') as reporting_month, 
        case when business_channel_name = 'AutoReorder' /*OR (business_channel_name = 'Web' and order_first_auto_reorder_order_placed_flag)*/ then 'Autoship - system generated'              
                else 'Non-Autoship' end as order_type_composite,        
--        case when previous_purchase_count is null then 'New' else 'Existing' end as new_existing_customer,                
--        case when previous_purchase_count is null then 'New' else 'Existing'||'-'||previous_purchase_count end as new_existing_expand_customer,   
        order_id,
        customer_id,             
        sum(revenue)::numeric(38,4) as SFW_Revenue, 
--        count(distinct a.customer_ID) as customer_count,
--        count(distinct order_id) as order_count,
        sum(order_line_quantity) as total_units,
        sum(revenue)::numeric(38,4)/sum(order_line_quantity) as order_ASP
        
from order_base a 
where cohort_name_new is not null
--and order_date < '2023-06-23'
group by 1,2,3,4,5
)
where order_type_composite = 'Non-Autoship'
group by 1,2
order by 2,1;


