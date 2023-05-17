
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
