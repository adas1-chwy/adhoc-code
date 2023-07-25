use schema discovery_sandbox;
use role discovery_developer;
use database edldb;



/*Comparing new table discovery_sandbox.search_performance_eplp_fix with the old table
discovery_sandbox.search_performance_eplp_fix is the latest table with the fix for the EPLP pages not carrying forward search term through clicks on CMS widgets 
breaking attribution for search */
SELECT 
  a.session_date 
  , a.rowcount AS new_row_count 
  , b.rowcount 
  , a.sessions AS new_sessions 
  , b.sessions 
  , a.searches AS new_searches 
  , b.searches
  , a.searches_with_purchase as newsearches_with_purchase
  , b.searches_with_purchase
FROM ( SELECT 
      session_date 
      , COUNT(*) AS rowcount 
      , COUNT(DISTINCT session_ID) AS sessions 
      , COUNT(DISTINCT search_ID) AS searches
      , count(distinct(case when allocated_order>0 then search_ID end)) as searches_with_purchase
    FROM discovery_sandbox.search_performance_eplp_fix
    WHERE 
      session_Date BETWEEN '2023-06-01' AND '2023-07-10'
    GROUP BY 1) a
LEFT JOIN
  ( SELECT 
      session_date 
      , COUNT(*) AS rowcount 
      , COUNT(DISTINCT session_ID) AS sessions 
      , COUNT(DISTINCT search_ID) AS searches
      , count(distinct(case when allocated_order>0 then search_ID end)) as searches_with_purchase
    FROM discovery_sandbox.search_performance
    WHERE 
      session_Date BETWEEN '2023-06-01' AND '2023-07-10'
    GROUP BY 1) b
ON a.session_date = b.session_date
ORDER BY 1 
;

/* Metrics we need for 2022 and 2023 
TOTAL_SESSIONS	
TOTAL_SEARCH_SESSIONS	
total Searches	
Total Session CVR - Of all sessions, how many had a purchase  	
Total Search Session CVR - Of all sessions that had a search, how many such sessions had a purchase (may or may not be attributed to search) 	
Total Search Session Search CVR	- Of all sessions that had a search, how many such sessions had a purchase attributed to Search	
Total Search term CVR - Of all unique searches, how many lead to a conversion (had an attributed purchase) */


--create or replace table discovery_sandbox.ad_search_degradation_all_sessions as 
--select 
--        ga_sessions_date as session_date,
--        unique_visit_id as session_id,           
--        device_category,
--        new_customer_flag,
--        active_autoship_flag,
--        channel,
--        max(case when event_category = 'Search Form' then 1 else 0 end) as search_form_event,
--        max(case when event_action = 'purchase' then 1 else 0 end) as is_purchase                      
--from
--        ga.ga_sessions_hits_products_union 
--where 1=1
--        and ga_sessions_date between '2022-01-01' and '2022-06-30'
--        and dataset = 'web'
--        and is_bot = 'false'
--        and bot_type = 'Not Bot'
--group by 1,2,3,4,5,6        
--; commit;


/*Getting session level data from clickstream base for all session along with flag for purchase 
We have flagged search event form but it doesnt capture all searches (NULL search ID issue from non-search form searches)
so we'll complement that with latest search performance for search sessions */
create or replace table discovery_sandbox.ad_search_degradation_session_attr as 
select 
        session_date,
        session_id,           
        device_category,
        new_customer_flag,
        active_autoship_flag,
        channel,
        max(case when event_category = 'Search Form' then 1 else 0 end) as search_form_event,
        max(case when event_action = 'purchase' then 1 else 0 end) as is_purchase                      
from
discovery_sandbox.prd_f_d_ga_clickstream_base
where 1=1
        and session_date between '2022-01-01' and '2023-07-30'
group by 1,2,3,4,5,6      ;
commit;



--select a.session_date, 
--        count(distinct a.session_id) as sessions, 
--        count(distinct(case when a.search_form_event>0 then a.session_id end)) as search_sessions_base,  
--        count(distinct(case when a.search_form_event>0 and b.session_Id is not null then a.session_id end)) as search_sessions_map,  
--        count(distinct(case when a.search_form_event=0 and b.session_Id is not null then a.session_id end)) as search_sessions_not_in_base
--from 
--discovery_sandbox.ad_search_degradation_session_attr a 
--left join 
--        (select session_ID from discovery_sandbox.search_performance_eplp_fix 
--        where session_Date between '2023-07-01' and '2023-07-10' 
--        and search_id is not null 
--        group by 1) b 
--on a.session_ID = b.session_ID    
--where a.session_date between '2023-07-01' and '2023-07-10' 
--group by 1;


--- combining search session and search level from new EPLP fixed search performance view
create or replace table discovery_sandbox.ad_search_degradation_session_andsearch_attr as 
(
select cd.financial_calendar_reporting_year AS financial_year 
  , financial_calendar_reporting_period AS financial_month
  , cd.financial_calendar_reporting_year||'-'||lpad(cd.financial_calendar_reporting_period,2,'0') as financial_period
  , t1.session_date,
        t1.session_id,           
        t1.device_category,
        t1.new_customer_flag,
        t1.active_autoship_flag,
        t1.channel,
        t1.search_form_event,
        t1.is_purchase,
        t2.search_id,
        t2.search_attributed_purchase
FROM discovery_sandbox.ad_search_degradation_session_attr t1
INNER JOIN cdm.common_date cd
ON t1.session_date = cd.common_date_dttm
LEFT JOIN 
(select 
        session_ID, 
        search_id, 
        search_term, 
        max(case when allocated_order>0 then 1 else 0 end) as search_attributed_purchase
from discovery_sandbox.search_performance_eplp_fix 
where search_id is not null 
group by 1,2,3
) t2 
on t1.session_id = t2.session_ID
WHERE 1=1 
AND cd.financial_calendar_reporting_year IN (2022,2023)
); commit;




--- Final summary 
select financial_period,
        financial_year, 
        financial_month,  
        new_customer_flag,
        device_category,
        channel,
        count(distinct session_ID) as sessions,
        count(distinct (case when is_purchase = 1 then session_ID end)) as sessions_with_purchase,
        count(distinct(case when search_id is not null then session_ID end)) as total_search_sessions, 
        count(distinct(case when search_id is not null and is_purchase = 1  then session_ID end)) as total_search_sessions_w_purchase, 
        count(distinct(case when search_id is not null and is_purchase = 1 and search_attributed_purchase = 1 then session_ID end)) as total_search_sessions_w_purchase_attr_to_search,
        count(distinct search_ID) as total_searches,
        count(distinct( case when search_attributed_purchase = 1 then search_ID end)) as total_searches_purchase        
from 
discovery_sandbox.ad_search_degradation_session_andsearch_attr
group by 1,2,3,4,5,6;



----- checking for session with search but no purchase attributed to search 
select session_ID
from 
discovery_sandbox.ad_search_degradation_session_andsearch_attr
where is_purchase=1 
group by 1 
having sum(search_attributed_purchase)= 0
limit 10;

----- checking for session not converted but have a converted search -- Duh!!
select top 10 * from 
discovery_sandbox.ad_search_degradation_session_andsearch_attr
where is_purchase=0 and search_attributed_purchase > 0;




/*First touch attribution split for search sessions with purchase */

---getting all search sessions with purchase 
create or replace temp table search_sessions_with_purchase as 
(select financial_period,
        financial_year, 
        financial_month,
        session_DATE,
        session_ID,
        channel
from discovery_sandbox.ad_search_degradation_session_andsearch_attr
where is_purchase = 1 and search_id is not null
); 

---getting all search sessions with purchase 
create or replace temp table search_sessions_with_purchase_not_attr_search as 
(select financial_period,
        financial_year, 
        financial_month,
        session_DATE,
        session_ID,
        channel
from discovery_sandbox.ad_search_degradation_session_andsearch_attr
where is_purchase = 1 and search_id is not null
group by 1,2,3,4,5,6
having sum(search_attributed_purchase) = 0
); 



--- getting all transactions in above sessions 

create or replace temp table search_purchase_sessions_attribution as 
(select a.financial_period,
        a.financial_year, 
        a.financial_month,
        a.session_DATE,
        a.session_ID,
        b.transaction_ID, 
        b.part_number, 
        b.quantity,
        b.revenue,
        b.attributed_widget_id, 
        b.attributed_widget_parent_group,
        b.page_type
from search_sessions_with_purchase a
left join discovery_sandbox.prd_f_d_first_touch_attribution  b
on a.session_Id = b.transaction_session_id
and b.attribution_type = 'in-session'
);

----concatenating all first attributed widgets for items in transaction
create or replace temp table search_purchase_session_attribution_list as 
(select financial_year||'-'||financial_month as financial_period, 
        session_date,
        session_ID,
        listagg(distinct attributed_widget_parent_group,' | ') as attributed_widget_parent_group,
        listagg(distinct page_type,' | ') as attributed_page_type
from search_purchase_sessions_attribution
group by 1,2,3)
;





--- widget type breakdown 
select financial_period, 
        case 
                when attributed_widget_parent_group like '%search%' then 'Search attributed to atleast 1 item' 
        else attributed_widget_parent_group end as widget_attribution , count(distinct session_ID) as sessions 
from 
search_purchase_session_attribution_list
group by 1,2;

---page-type breakdown based on aggregated list 
SELECT 
  financial_period 
  , CASE WHEN lower(attributed_page_type) LIKE '%search%' 
    THEN 'Search Page attributed to atleast 1 item'
    ELSE attributed_page_type 
  END AS attributed_page_type 
  , COUNT(DISTINCT session_ID) AS sessions
FROM search_purchase_session_attribution_list
GROUP BY 1,2 
;


---- page level breakdown 
SELECT 
  financial_year||'-'||financial_month AS financial_period 
  , page_type 
  , COUNT(DISTINCT session_ID) AS purchase_sessions
FROM search_purchase_sessions_attribution
GROUP BY 1,2 
;


---- getting page level breakdown for search sessions with no purchase attributed to search 

--SELECT 
--  a.financial_year||'-'||a.financial_month AS financial_period 
--  , a.page_type 
--  , COUNT(DISTINCT a.session_ID) AS purchase_sessions
--FROM search_purchase_sessions_attribution a 
--inner join search_sessions_with_purchase_not_attr_search b 
--on a.session_Id = b.session_ID 
--GROUP BY 1,2 
--order by 1,2
--;



---- getting page level breakdown for search sessions with no purchase attributed to search broken by channel

SELECT 
  a.financial_year||'-'||a.financial_month AS financial_period 
  , a.page_type 
  , b.channel
  , COUNT(DISTINCT a.session_ID) AS purchase_sessions
FROM search_purchase_sessions_attribution a 
inner join search_sessions_with_purchase_not_attr_search b 
on a.session_Id = b.session_ID 
GROUP BY 1,2,3 
order by 1,2,3
;

/*For Search Session with no purchase attributed to search and broken by channel, 
further breaking down by entrance page type 
Hypothesis being, we are seeing majority of these purchases are attributed to PDP which has increased over time
So want to understand, where are those customers purchasing from PDP landing to, when they come to chewy */


CREATE 
OR 
REPLACE temp TABLE search_sessions_with_purchase_not_attr_search_entrance_page AS
( SELECT 
    a.session_ID 
    , a.session_DATE
    , a.financial_year||'-'||a.financial_month AS financial_period 
    , a.channel 
    , b.page_type AS attributed_purchase_page_type 
    , c.page_type AS entrace_page_type 
    , c.page_path AS entrance_page_path 
  FROM search_purchase_sessions_attribution b
  INNER JOIN search_sessions_with_purchase_not_attr_search a
  ON a.session_Id = b.session_ID
  LEFT JOIN discovery_sandbox.page_performance c
  ON a.session_Id = c.session_ID
  AND c.entrance_pageview = 1 
  AND c.session_date >'2022-01-01');
  
--- persisting the table in database
create or replace table discovery_sandbox.search_sessions_with_purchase_not_attr_search_entrance_page as 
(select * from search_sessions_with_purchase_not_attr_search_entrance_page); commit;



---- getting page level breakdown for search sessions with no purchase attributed to search broken by channel

SELECT 
  a.financial_period 
  , a.attributed_purchase_page_type 
  , a.channel
  , a.entrace_page_type
  , COUNT(DISTINCT a.session_ID) AS purchase_sessions
FROM discovery_sandbox.search_sessions_with_purchase_not_attr_search_entrance_page a
where attributed_purchase_page_type = 'PDP'
GROUP BY 1,2,3,4
order by 1,2,3
;




SELECT 
  a.financial_year||'-'||a.financial_month AS financial_period 
  , a.page_type 
  , a.attributed_widget_parent_group
--  , b.channel
  , COUNT(DISTINCT a.session_ID) AS purchase_sessions
FROM search_purchase_sessions_attribution a 
inner join search_sessions_with_purchase_not_attr_search b 
on a.session_Id = b.session_ID 
where a.page_type = 'PDP'
GROUP BY 1,2,3 
order by 1,2,3, 4 desc
;
