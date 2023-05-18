USE role discovery_developer;
USE DATABASE edldb;
USE schema discovery_sandbox;


/* To validate the current logic to identify duplicates as in https://chewyinc.atlassian.net/browse 
/SF-3162
holds true
create or replace temp table x as
(select a.search_id, count(a.search_id) as cnt
from discovery_sandbox.search_performance  a
where a.session_date > '2023-05-01'
group by 1
having  count(a.search_id)= 1
);


create temp table x_all as
(select a.*
from discovery_sandbox.search_performance  a
inner join x b
on a.search_id = b.search_id
where a.session_date > '2023-04-30'
);


SELECT
COUNT(*)
, COUNT(DISTINCT search_id)
FROM x_all
WHERE
search_term = next_search_term
;


create or replace temp table y as
(select a.search_id, count(a.search_id) as cnt
from discovery_sandbox.search_performance  a
where a.session_date > '2023-05-01'
and search_term = next_search_term
group by 1
having  count(a.search_id)= 1
);


select
select top 10 x.*
from x
left join y
on x.search_id = y.search_id
where y.search_id is null ;
select 'old' , count(*) from x
union
select 'new' , count(*) from y;


select * from discovery_sandbox.search_performance where session_date >= '2023-05-01' and search_id 
= '202305090000007454913731082258621168367279620';

select * from DISCOVERY_SANDBOX.search_performance where session_date = '2023-05-09' and session_id 
= '2023050900000074549137310822586211683672796';

select * from ga.ga_sessions_hits
where ga_sessions_date = '2023-05-09'
and unique_visit_id = '2023050900000074549137310822586211683672796'
order by hit_number;
*/


/*Creating a base table of all search_IDs that are duplicates and need to be removed */
CREATE OR REPLACE TABLE
  discovery_sandbox.prd_f_d_dis_search_event_duplicates AS
  ( SELECT
      a.search_id
      , a.session_date
    FROM discovery_sandbox.search_performance a
    WHERE
      a.session_date >= '2021-12-01'
    AND search_term = next_search_term
    GROUP BY 1,2
    HAVING COUNT(a.search_id)= 1
  )
;
COMMIT;




CREATE VIEW
  SEARCH_PERFORMANCE_DEDUP
  (
    SESSION_DATE
    , SESSION_AUTH_FLAG
    , NEW_CUSTOMER_FLAG
    , ACTIVE_AUTOSHIP_FLAG
    , DEVICE_CATEGORY
    , CHANNEL
    , PET_PROFILE_FLAG
    , SESSION_ID
    , PERSONALIZATION_ID
    , CUSTOMER_ID
    , HIT_ID
    , HIT_NUMBER
    , PLP_IMPRESSION_HIT_FLAG
    , PLP_PRODUCT_CLICK_HIT_FLAG
    , EVENT_CATEGORY
    , EVENT_ACTION
    , EVENT_LABEL
    , SEARCH_ID
    , SEARCH_TERM
    , WORD_COUNT
    , SEARCH_CATEGORY
    , SEARCH_EXPERIENCE_TYPE
    , SEARCH_REDIRECT_FLAG
    , SEARCH_REFORMULATION_FLAG
    , SEARCH_PURCHASE_FLAG
    , NEXT_SEARCH_ID
    , NEXT_SEARCH_TERM
    , SEARCH_NONCLICK_FLAG
    , PAGEVIEW_ID
    , IS_ENTRANCE_FLAG
    , IS_EXIT_FLAG
    , IS_BOUNCE_FLAG
    , PAGE_TYPE
    , LIST_CATEGORY
    , PAGE_NUMBER
    , PAGE_1_FLAG
    , WIDGET_CATEGORY
    , WIDGET_POSITION
    , WIDGET_TYPE
    , WIDGET_NAME
    , FACET_TYPE
    , FACETS_APPLIED
    , ASSET_PARENT_GROUP
    , PRODUCT_LIST_POSITION
    , PRODUCT_ID
    , PRICE
    , LIST_PRICE
    , PART_NUMBER
    , PARENT_PART_NUMBER
    , MC1
    , MC2
    , MC3
    , CATEGORY_LEVEL1
    , CATEGORY_LEVEL2
    , CATEGORY_LEVEL3
    , PURCHASE_BRAND
    , PROPRIETARY_BRAND_FLAG
    , ON_DEAL_FLAG
    , MANUALDEAL
    , IN_STOCK_FLAG
    , PRODUCT_IMPRESSION
    , HIT_IMPRESSION
    , PRODUCT_CLICK
    , HIT_CLICK
    , ALLOCATED_ATC
    , ALLOCATED_ORDER
    , ALLOCATED_UNIT
    , ALLOCATED_REVENUE
    , ALLOCATED_GROSS_MARGIN
    , RECIPROCAL_RANK
    , RECIPROCAL_RANK_PRODUCT_CLICK
  )
  AS
  ( SELECT
      -- session dimensions
      t1.session_date
      , t1.session_auth_flag
      , t1.new_customer_flag
      , t1.active_autoship_flag
      , t1.device_category
      , t1.channel
      , t1.pet_profile_flag
      , t1.session_id
      , t1.personalization_id
      , t1.customer_id
      ,
      -- hit dimension
      t1.hit_id
      , t1.hit_number
      , t1.plp_impression_hit_flag
      , t1.plp_product_click_hit_flag
      , (
      CASE WHEN t1.event_category = 'Search Form'
        THEN NULL
        ELSE t1.event_category
      END)
      AS event_category
      , (
      CASE WHEN t1.event_category = 'Search Form'
        THEN NULL
        ELSE t1.event_action
      END)
      AS event_action
      , (
      CASE WHEN t1.event_category = 'Search Form'
        THEN NULL
        ELSE t1.event_label
      END)
      AS event_label
      ,
      -- derived search_id and search_id dimensions
      t1.search_id
      , t1.search_term
      , t1.word_count
      , t1.search_category
      , t1.search_experience_type
      , t1.search_redirect_flag
      , t1.search_reformulation_flag
      , t1.search_purchase_flag
      , t1.next_search_id
      , t1.next_search_term
      , t1.search_nonclick_flag
      ,
      -- derived pageview id and page dimensions
      t1.pageview_id
      , t1.is_entrance_flag
      , t1.is_exit_flag
      , t1.is_bounce_flag
      , t1.page_type
      , t1.list_category
      , t1.page_number
      , t1.page_1_flag
      ,
      -- widget and asset dimensions
      t1.widget_category
      , t1.widget_position
      , t1.widget_type
      , t1.widget_name
      , t1.facet_type
      , t1.facets_applied
      , t1.asset_parent_group
      ,
      -- product dimensions
      t1.product_list_position
      , t1.product_id
      , t1.price
      , t1.list_price
      , t1.part_number
      , t1.parent_part_number
      , t1.mc1
      , t1.mc2
      , t1.mc3
      , t1.category_level1
      , t1.category_level2
      , t1.category_level3
      , t1.purchase_brand
      , t1.proprietary_brand_flag
      , t1.on_deal_flag
      , t1.manualdeal
      , t1.in_stock_flag
      ,
      -- metrics
      t1.product_impression
      , t1.hit_impression
      , t1.product_click
      , t1.hit_click
      , t1.allocated_atc
      , t1.allocated_order
      , t1.allocated_unit
      , t1.allocated_revenue
      , t1.allocated_gross_margin
      , t1.reciprocal_rank
      , t1.reciprocal_rank_product_click
    FROM discovery_sandbox.prd_f_d_dis_search_event_base AS t1
      -- temp solution to filter out new bots
      -- will remove once we backfill search_base table
      -- this filters session to ONLY "web" + nonbot sessions
    JOIN ecom.ecom_ga_bot_traffic AS t2
    ON t1.session_date = t2.ga_sessions_date
    AND t1.session_id = t2.unique_visit_id
    AND t2.bot_type = 'Not Bot'
    LEFT JOIN discovery_sandbox.prd_f_d_dis_search_event_duplicates b
    ON t1.session_date = b.session_date
    AND t1.search_id = b.search_id
    WHERE 
      b.search_id IS NULL
  )
;
COMMIT;
