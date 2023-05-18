
use role discovery_developer;
use database edldb;
use schema discovery_sandbox;


/** Here using the de-duped search performance view, creating a trimmed version of search performance 
aggregated at a search_ID level by imputing NULL searchIDs with session_ID||'-'||search_term 

THe nUll searchIDs are from cases where there wasnt a search event form for the search results shown to customer 
and hence in existing prod logic, these search queries dont get assigned a search_ID
So as a proxy, used session_ID||'-'||search_term instead

Also found out that NULL searchIDs had search_purchase_flag set to true because of the window function 
logic defined for that flag.

**/ 


create table discovery_sandbox.ad_search_performance_qbr 
cluster by (session_date)
as 
select search_id,
        session_id, 
        session_date,
        search_term, lead(search_term,1) over(partition by session_id order by hit_number) as next_search_term,
        product_impression,
        product_click,
        allocated_atc,
        allocated_order,
        is_exit 
from 
(select search_id,
        session_id, 
        session_date,
        search_term, 
        max(hit_number) as hit_number,
        sum(product_impression) as product_impression,
        sum(product_click) as product_click,
        sum(allocated_atc) as allocated_atc,
        sum(allocated_order) as allocated_order,
       case when sum(is_exit)>0 then 1 else 0 end is_exit
from 
        (select  coalesce(search_id, session_id||'-'||search_term) as search_id, 
                session_id, 
                session_date,
                search_term,
                search_experience_type,
                hit_number,
                hit_id,
                product_impression,
                product_click,
                allocated_atc,
                allocated_order,
                case when is_exit_flag = true then 1 else 0 end as is_exit
                
        from discovery_sandbox.search_performance_dedup t1
        where session_date >= '2023-01-01'
        )
group by 1,2,3,4); commit;

/** The table was backfilled by running above queries for time 01/01/22 - 05/15/23 
**/ 



/** Below query to summarize search metrics based on this new search level table 

*/ 


select *, 
        search_volume/distinct_session_w_search::float as searches_per_session,
        sessions_with_purchase_search/distinct_session_w_search::float as session_level_cvr,
        searches_with_product_impression/search_volume::float as searches_with_product_impression,
        searches_with_product_click/search_volume::float as searches_product_click_rate,
        searches_with_attributed_atc/search_volume::float as searches_with_ATC,
        searches_with_attributed_order/search_volume::float as searches_cvr,
        searches_with_exit/search_volume::float as exit_rate,
        re_search_volume/search_volume::float as research_rate,
        re_search_diff_keyword_volume/search_volume::float   as re_search_diff_keyword_rate   
from 
(
SELECT 
  cd.financial_calendar_reporting_year AS financial_year 
  , financial_calendar_reporting_period AS financial_period 
  , COUNT(DISTINCT t1.session_id) AS distinct_session_w_search 
  , COUNT(DISTINCT t1.search_id) AS search_volume 
  , COUNT(DISTINCT CASE WHEN product_impression > 0 THEN t1.search_id END) AS searches_with_product_impression 
  , COUNT(DISTINCT CASE WHEN product_click > 0 THEN t1.search_id END) AS searches_with_product_click 
  , COUNT(DISTINCT CASE WHEN allocated_atc > 0 THEN t1.search_id END) AS searches_with_attributed_atc 
  , COUNT(DISTINCT CASE WHEN allocated_order > 0 THEN t1.search_id END) AS searches_with_attributed_order
  , COUNT(DISTINCT CASE WHEN allocated_order > 0 THEN t1.session_id END) AS sessions_with_purchase_search
  , COUNT(DISTINCT CASE WHEN is_exit > 0 THEN t1.search_id END) AS searches_with_exit
  , count(distinct case when search_term != next_search_term and product_click = 0 then t1.search_id end) as re_search_diff_keyword_volume 
  , count(distinct case when next_search_term is not null and product_click = 0 then t1.search_id end) as re_search_volume   
FROM discovery_sandbox.ad_search_performance_qbr t1
INNER JOIN cdm.common_date cd
ON t1.session_date = cd.common_date_dttm
WHERE 1=1 
--  T1.SESSION_DATE >= '2023-01-01' 
--AND t1.session_date <= '2023-05-04'
AND cd.financial_calendar_reporting_year IN (2022,2023)
GROUP BY 1,2 
) ;












/*** Created a new version of SEARCH_PERFORMANCE_DEDUPed 
with the NULL search IDs fixed for the trend analysis to uncover what 
changes in 2022 P04-> P05

***/ 



CREATE TABLE 
  discovery_sandbox.ad_search_performance_qbr_ext cluster BY 
  ( 
    session_date 
  )
  AS
SELECT 
  * 
  , CASE WHEN search_id_null_flag = 1 
    THEN fixed_null_search_experience_type WHEN search_id_null_flag = 0 
    THEN search_experience_type 
  END AS search_experience_type_new
FROM ( SELECT 
      * 
      , COALESCE(search_id, session_id||'-'||search_term) AS fixed_search_id 
      , CASE WHEN search_id IS NULL 
        THEN 1 
        ELSE 0 
      END AS search_id_null_flag 
      , CASE WHEN search_id_null_flag = 1 
        THEN first_value(search_experience_type ignore nulls) over ( 
                                                                  PARTITION BY 
                                                                    fixed_search_id 
                                                                  ORDER BY hit_number ASC) 
      END AS fixed_null_search_experience_type 
      , cd.financial_calendar_reporting_year AS financial_year 
      , cd.financial_calendar_reporting_period AS financial_period
    FROM discovery_sandbox.search_performance_dedup t1
    JOIN cdm.common_date cd
    ON t1.session_date = cd.common_date_dttm
    WHERE 
      session_date BETWEEN '2022-04-01' AND '2022-08-31'
    AND cd.financial_calendar_reporting_period IN ('P03','P04','P05','P06','P07') ) 
;
COMMIT;
