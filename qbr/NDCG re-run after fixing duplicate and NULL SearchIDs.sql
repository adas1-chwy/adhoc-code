
/** Snippet below computes the MRR & NDCG for the experiment */ 
use role discovery_developer;
use database edldb;
use schema discovery_sandbox;
set max_hits = 36;


/*  Code forked from the Search LEvel NDCG table transform 
modified with the new imputed searchID 

Created a new static table at the new Search ID level with the NDCG and MRR measures to be aggregated on 
**/ 


DROP TABLE 
  IF EXISTS search_performance_sub 
;
CREATE temp TABLE search_performance_sub AS
( SELECT 
    a.search_id 
    , search_term 
    , session_id 
    , session_date 
    , WIDGET_NAME 
    , product_list_position 
    , hit_number 
    , page_number 
    , part_number 
    , product_click 
    , allocated_atc 
    , allocated_revenue 
    , search_redirect_flag
    , COALESCE(search_id, session_id||'-'||search_term) AS search_id_new
  FROM discovery_sandbox.search_performance_dedup a
  WHERE 
    a.session_date BETWEEN '2022-08-01' AND '2023-10-31'
--    a.session_date = $run_date
);


/* Step1: Identify searches where no products were shown (no product_list_position values) */ 
drop table if exists no_products_query;
create or replace temp table no_products_query
as
(
    select SEARCH_ID_NEW,
           session_date,
--           search_redirect_flag,
           count_if(WIDGET_NAME in ('search-results','browse','brand-page','deals') and (not product_list_position is null)) as product_list_count
    from search_performance_sub
    where 1=1
          and (not SEARCH_ID_NEW is null)
    group by SEARCH_ID_NEW,session_date
--    ,search_redirect_flag
    having product_list_count = 0);

/* Step2: identify first product assignment for each page_number and product_list_position */
drop table if exists position_rank_filter;
create or replace temp table position_rank_filter 
as
(
    select SEARCH_ID_NEW,
           --SEARCH_REDIRECT_FLAG,
           session_date,
           hit_number,
           page_number,
           product_list_position,
           part_number,
           dense_rank() over (partition by SEARCH_ID_NEW, page_number, product_list_position order by hit_number asc) as position_rank
    from search_performance_sub
    where 1=1
          and (WIDGET_NAME in ('search-results','browse','brand-page','deals'))
--          and (search_redirect_flag = false)
          and (not ((SEARCH_ID_NEW is null)
                    or (hit_number is null)
                    or (page_number is null)
                    or (product_list_position is null)
                    or (part_number is null)))
     QUALIFY position_rank = 1               
                    );



/* Step3: identify first page_number and product_list_position assignment for each product */
drop table if exists part_rank_filter;
create or replace temp table part_rank_filter 
as
(    select SEARCH_ID_NEW,
            session_date,
--           search_redirect_flag,
           hit_number,
           page_number,
           product_list_position,
           part_number,
           dense_rank() over (partition by SEARCH_ID_NEW, part_number order by hit_number asc) as part_rank
    from position_rank_filter
    qualify part_rank = 1
    );
    
    
/* Step 4: assign observed_rank to each product (part_number)*/
drop table if exists observed_rank_query;
create or replace temp table observed_rank_query
as
(  
    select SEARCH_ID_NEW,
           session_date,
           --SEARCH_REDIRECT_FLAG,
           part_number,
           dense_rank() over (partition by SEARCH_ID_NEW order by page_number asc, product_list_position asc) as observed_rank
    from part_rank_filter);
    
    
/* Step5: ignore product listings beyond the desired menu size */ 
drop table if exists observed_rank_filter;
create or replace temp table observed_rank_filter
as
(
    select 
        SEARCH_ID_NEW, 
        --SEARCH_REDIRECT_FLAG, 
        session_date,
        part_number, 
        observed_rank
    from observed_rank_query
    where observed_rank <= $max_hits
    group by SEARCH_ID_NEW
 --   , SEARCH_REDIRECT_FLAG
 , session_date, part_number, observed_rank
    );
    
    
/* Step6: assign relevance levels for clicks, adds (add-to-cart events), and purchases */
drop table if exists relevance_query;
create or replace temp table relevance_query
as
(
    select SEARCH_ID_NEW,
           session_date,
           --SEARCH_REDIRECT_FLAG,
           part_number,
           max((product_click > 0)::int) as click_relevance,
           max((allocated_atc > 0)::int) as add_relevance,
           max((allocated_revenue > 0)::int) as purchase_relevance,
           case when purchase_relevance > 0 then 3
                when add_relevance > 0 then 2
                when click_relevance > 0 then 1
                else 0 end as multilevel_relevance
    from search_performance_sub
    where 1=1
          and (not ((SEARCH_ID_NEW is null) or (part_number is null)))
          and ((product_click > 0) or (allocated_atc > 0) or (allocated_revenue > 0))
    group by SEARCH_ID_NEW, part_number ,session_date
    --,search_redirect_flag
    );
    
    
    
/* Step7: find the divisor for average_precision metrics */
drop table if exists relevance_totals;
create or replace temp table relevance_totals
as
(
    select SEARCH_ID_NEW,
           session_date,
           --SEARCH_REDIRECT_FLAG,
           case when sum(click_relevance) > $max_hits then $max_hits else sum(click_relevance) end as click_relevance_count,
           case when sum(add_relevance) > $max_hits then $max_hits else sum(add_relevance) end as add_relevance_count,
           case when sum(purchase_relevance) > $max_hits then $max_hits else sum(purchase_relevance) end as purchase_relevance_count
    from relevance_query
    group by SEARCH_ID_NEW, session_date
  --  ,search_redirect_flag
    );
    
    
/*Step8: find the ideal rankings for click, add, purchase, and multilevel relevance 
 (events without clicks do not contribute to cumulative discounted gain) */
drop table if exists ideal_rank_query;
create or replace temp table ideal_rank_query
as
(
    select SEARCH_ID_NEW,
           session_date,
 --          search_redirect_flag,
           click_relevance,
           add_relevance,
           purchase_relevance,
           multilevel_relevance,
           row_number() over (partition by SEARCH_ID_NEW order by click_relevance desc) as click_ideal_rank,
           row_number() over (partition by SEARCH_ID_NEW order by add_relevance desc) as add_ideal_rank,
           row_number() over (partition by SEARCH_ID_NEW order by purchase_relevance desc) as purchase_ideal_rank,
           row_number() over (partition by SEARCH_ID_NEW order by multilevel_relevance desc) as multilevel_ideal_rank
    from relevance_query);
    
    
/* Step9: compute ideal cumulative discounted gain for each search */
drop table if exists ideal_dcg_query;
create or replace temp table ideal_dcg_query
as
(
    select SEARCH_ID_NEW,
        session_date,
 --       search_redirect_flag,
           sum(case when click_ideal_rank <= $max_hits then (pow(2, click_relevance) - 1) / log(2, click_ideal_rank + 1)
                    else 0 end) as click_ideal_dcg,
           sum(case when add_ideal_rank <= $max_hits then (pow(2, add_relevance) - 1) / log(2, add_ideal_rank + 1)
                    else 0 end) as add_ideal_dcg,
           sum(case when purchase_ideal_rank <= $max_hits then (pow(2, purchase_relevance) - 1) / log(2, purchase_ideal_rank + 1)
                    else 0 end) as purchase_ideal_dcg,
           sum(case when multilevel_ideal_rank <= $max_hits then (pow(2, multilevel_relevance) - 1) / log(2, multilevel_ideal_rank + 1)
                    else 0 end) as multilevel_ideal_dcg
    from ideal_rank_query
    group by SEARCH_ID_NEW, session_date
    --,search_redirect_flag
    );
    
    
/*Step 10: join the ranked products to the corresponding relevance values */
drop table if exists relevance_join;
create or replace temp table relevance_join
as
(
    select observed_rank_filter.SEARCH_ID_NEW,
           observed_rank_filter.session_date,
     --      observed_rank_filter.search_redirect_flag,
           observed_rank_filter.part_number,
           observed_rank,
           case when click_relevance is null then 0 else click_relevance end as click_relevance,
           case when add_relevance is null then 0 else add_relevance end as add_relevance,
           case when purchase_relevance is null then 0 else purchase_relevance end as purchase_relevance,
           case when multilevel_relevance is null then 0 else multilevel_relevance end as multilevel_relevance
    from observed_rank_filter
    left outer join relevance_query
         on relevance_query.SEARCH_ID_NEW = observed_rank_filter.SEARCH_ID_NEW and relevance_query.part_number = observed_rank_filter.part_number);

/* Step 11: compute cumulative click, add, and purchase counts for average_precision metrics */
drop table if exists cumulative_count_query;
create or replace temp table cumulative_count_query
as
(
    select SEARCH_ID_NEW,
           session_date,
       --    search_redirect_flag,
           part_number,
           observed_rank,
           click_relevance,
           add_relevance,
           purchase_relevance,
           multilevel_relevance,
           sum(click_relevance) over(partition by SEARCH_ID_NEW order by observed_rank asc) as click_cumulative_count,
           sum(add_relevance) over(partition by SEARCH_ID_NEW order by observed_rank asc) as add_cumulative_count,
           sum(purchase_relevance) over(partition by SEARCH_ID_NEW order by observed_rank asc) as purchase_cumulative_count
    from relevance_join);
    
    
/*Step 12: compute summary statistics for each search */
drop table if exists relevance_summary_query;
create or replace temp table relevance_summary_query
as
(
    select SEARCH_ID_NEW,
           session_date,
   --        search_redirect_flag,
           max(click_relevance / observed_rank) as click_reciprocal_rank,
           max(add_relevance / observed_rank) as add_reciprocal_rank,
           max(purchase_relevance / observed_rank) as purchase_reciprocal_rank,
           sum(case when click_relevance > 0 then click_cumulative_count / observed_rank else 0 end) as click_precision_sum,
           sum(case when add_relevance > 0 then add_cumulative_count / observed_rank else 0 end) as add_precision_sum,
           sum(case when purchase_relevance > 0 then purchase_cumulative_count / observed_rank else 0 end) as purchase_precision_sum,
           sum((pow(2, click_relevance) - 1) / log(2, observed_rank + 1)) as click_observed_dcg,
           sum((pow(2, add_relevance) - 1) / log(2, observed_rank + 1)) as add_observed_dcg,
           sum((pow(2, purchase_relevance) - 1) / log(2, observed_rank + 1)) as purchase_observed_dcg,
           sum((pow(2, multilevel_relevance) - 1) / log(2, observed_rank + 1)) as multilevel_observed_dcg
    from cumulative_count_query
    group by SEARCH_ID_NEW, session_date
    --,search_redirect_flag
    );
    
    
/*Step 13: compute metrics for searches where products were shown */
drop table if exists relevance_metrics_join;
create or replace temp table relevance_metrics_join
as
(

    select relevance_summary_query.SEARCH_ID_NEW,
           relevance_summary_query.session_date,
   --        relevance_summary_query.search_redirect_flag,
           click_reciprocal_rank,
           add_reciprocal_rank,
           purchase_reciprocal_rank,
           case when coalesce(click_relevance_count, 0) > 0 then click_precision_sum / click_relevance_count::float else 0 end as click_average_precision,
           case when coalesce(add_relevance_count, 0) > 0 then add_precision_sum / add_relevance_count::float else 0 end as add_average_precision,
           case when coalesce(purchase_relevance_count, 0) > 0 then purchase_precision_sum / purchase_relevance_count::float else 0 end as purchase_average_precision,
           case when coalesce(click_ideal_dcg, 0) > 0 then click_observed_dcg / click_ideal_dcg::float else 0 end as click_ndcg,
           case when coalesce(add_ideal_dcg, 0) > 0 then add_observed_dcg / add_ideal_dcg::float else 0 end as add_ndcg,
           case when coalesce(purchase_ideal_dcg, 0) > 0 then purchase_observed_dcg / purchase_ideal_dcg::float else 0 end as purchase_ndcg,
           case when coalesce(multilevel_ideal_dcg, 0) > 0 then multilevel_observed_dcg / multilevel_ideal_dcg::float else 0 end as multilevel_ndcg
    from relevance_summary_query 
    left outer join relevance_totals  on relevance_totals.SEARCH_ID_NEW = relevance_summary_query.SEARCH_ID_NEW
    left outer join ideal_dcg_query  on ideal_dcg_query.SEARCH_ID_NEW = relevance_summary_query.SEARCH_ID_NEW);


/*Step 14: combine metrics for searches without products and searches with products */
--insert into discovery_sandbox.prd_s_d_SEARCH_ID_NEW_level_ndcg
--CREATE OR REPLACE TABLE DISCOVERY_SANDBOX.ad_search_performance_qbr_QUALITY 
--cluster by (session_date) as 
INSERT INTO  DISCOVERY_SANDBOX.ad_search_performance_qbr_QUALITY 
(
    select SEARCH_ID_NEW,
           session_date,
    --       search_redirect_flag,
           0 as click_reciprocal_rank,
           0 as add_reciprocal_rank,
           0 as purchase_reciprocal_rank,
           0 as click_average_precision,
           0 as add_average_precision,
           0 as purchase_average_precision,
           0 as click_ndcg,
           0 as add_ndcg,
           0 as purchase_ndcg,
           0 as multilevel_ndcg
    from no_products_query
    union
    select SEARCH_ID_NEW,
           session_date,
   --        search_redirect_flag,
           click_reciprocal_rank,
           add_reciprocal_rank,
           purchase_reciprocal_rank,
           click_average_precision,
           add_average_precision,
           purchase_average_precision,
           click_ndcg,
           add_ndcg,
           purchase_ndcg,
           multilevel_ndcg
    from relevance_metrics_join);
commit;






/* Summarize for financial period */


SELECT 
  cd.financial_calendar_reporting_year AS financial_year 
  , financial_calendar_reporting_period AS financial_period 
  , sum(click_reciprocal_rank) as click_reciprocal_rank_sum
  , sum(purchase_reciprocal_rank) as purchase_reciprocal_rank_sum
  , sum(multilevel_ndcg) as multilevel_ndcg_sum

FROM discovery_sandbox.ad_search_performance_qbr_QUALITY t1
INNER JOIN cdm.common_date cd
ON t1.session_date = cd.common_date_dttm
WHERE 1=1 
--  T1.SESSION_DATE >= '2023-01-01' 
--AND t1.session_date <= '2023-05-04'
AND cd.financial_calendar_reporting_year IN (2022,2023)
GROUP BY 1,2
order by 1,2 ;


select session_date, count(*)
FROM discovery_sandbox.ad_search_performance_qbr_QUALITY
group by 1 
order by 1 ;
