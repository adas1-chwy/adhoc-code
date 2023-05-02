use role discovery_developer;
set start_date = '2023-02-14'; -- start of test
set end_date = '2023-02-24'; -- end of test
   


--- original base table removing spillovers 
create or replace temp table exp_base as 
(
  select
  t1.experiment,
  t1.session_date,
  t1.session_id,
  t1.personalization_id,
  t1.test_arm

  from discovery_sandbox.prd_d_d_dis_expr_pool as t1

  where t1.session_date between $start_date AND $end_date
  and t1.experiment = 'ATTACH_SMARTSHELF_02'

  -- remove spill over
  qualify count(distinct t1.test_arm)
  over (partition by t1.personalization_id) = 1
);



  

--- here we are including all traffic inclusive of spillovers 
create or replace temp table exp_base_unfiltered as 
(
  select
  t1.experiment,
  t1.session_date,
  t1.session_id,
  t1.personalization_id,
  t1.test_arm

  from discovery_sandbox.prd_d_d_dis_expr_pool as t1

  where t1.session_date between $start_date AND $end_date
  and t1.experiment = 'ATTACH_SMARTSHELF_02'

);

   
--- for spillovers, we are listaggregating the test arm to get the granularity at session level 
create or replace temp table pid_assign as 
select experiment, session_date,  session_id, personalization_id, listagg(test_arm, '->') within group (order by session_date) as test_arm
from exp_base_unfiltered 
group by 1,2,3,4;
--where session_id = '2023022000000051457477829745578181676922490'


    
-- getting the GA hits for key event actions using the above session level test_arm assignment
create or replace table discovery_sandbox.ad_exp_ga_base_unfiltered as 
(select b.test_arm, b.session_id, b.session_id, b.personalization_id, b.experiment, a.type, page_title, view_type, hit_number, event_action, event_label, a.traffic_source, a.user_id, a.authentication_state, 
a.active_autoship_flag, a.new_customer_flag, a.device_category
from pid_assign b 
inner join ga.ga_sessions_hits a
ON 
    b.session_date = a.ga_sessions_date
AND a.unique_visit_id = b.session_id
where a.event_action IN ('productClick','addToCart','addToAutoship','select','click','sign-in','ATTACH_SMARTSHELF_02'));
commit;

--- this is primarily to get the bias dimensions we care about for each session 
create or replace temp table session_metrics as 
SELECT 
    a.personalization_id as exp_PID, 
    a.test_arm, 
    a.experiment, 
    b.*
FROM 
    exp_base_unfiltered a
INNER JOIN 
    discovery_sandbox.prd_f_d_expr_sess_metrics b
ON 
    a.session_date = b.session_date
AND a.session_id = b.session_id
;

    
       
-- getting distinct dimensions 
--- granularity is sessionID
CREATE OR REPLACE temp TABLE x AS
SELECT 
    DISTINCT exp_pid, 
    session_id, 
    session_auth_flag, 
    new_customer_flag, 
    active_autoship_flag, 
    device_category, 
    channel 
FROM 
    session_metrics;
   
--- final table list agg of the event action in sequence 
-- untill activation of the experiment, as customer journey within a session 
create or replace table discovery_sandbox.ad_exp_base_unfiltered_journey as
(select a.*, 
        x.session_auth_flag, 
        x.new_customer_flag, 
        x.active_autoship_flag, 
        x.device_category, 
        x.channel 
from 
        (select a.test_arm, 
                a.personalization_ID, 
                a.session_ID ,
                listagg( a.event_action, '->') within group (order by a.hit_number) as event_action_trail
        from discovery_sandbox.ad_exp_ga_base_unfiltered a
        join (select session_id, max(hit_number) as max_hit 
                from discovery_sandbox.ad_exp_ga_base_unfiltered 
                where event_action = 'ATTACH_SMARTSHELF_02' 
                group by 1) b 
        on a.session_id = b.session_id
        and a.hit_number <= b.max_hit
        group by 1,2,3
        ) a 
        left join x 
        on a.session_id = x.session_id
);
commit;

      

--- temp table to identify the max_hit to consider for the customer journey untill activation of the experiment 
-- for clean assignment, this is until activation 
-- for spillovers, this is untill the second treatment assignment 
create or replace temp table session_hit_window as 
(
select session_id, min(hit_number) as max_hit 
from discovery_sandbox.ad_exp_ga_base_unfiltered 
where event_action = 'ATTACH_SMARTSHELF_02' 
and test_arm IN ('CONTROL','VARIANT_01','VARIANT_02','FALLBACK')
group by 1

UNION 

select session_id, min(hit_number) as max_hit 
from 
        (select session_id, event_action, hit_number, event_label, lag(event_label,1) over(partition by session_id order by hit_number) as prev_label,
                case when event_label <> prev_label then 1 else 0 end as island_help
        from discovery_sandbox.ad_exp_ga_base_unfiltered 
        where event_action = 'ATTACH_SMARTSHELF_02' 
        and test_arm NOT IN ('CONTROL','VARIANT_01','VARIANT_02')
        )
where island_help = 1
group by 1
);


    
---solving using gaps and islands, building customer journey based on unique event_actions  
create or replace table discovery_sandbox.ad_exp_base_unfiltered_journey_distinct as
select a.*, 
        x.session_auth_flag, 
        x.new_customer_flag, 
        x.active_autoship_flag, 
        x.device_category, 
        x.channel 
from        
        (select test_arm, personalization_id, session_id,listagg( event_action, '->') within group (order by island) as event_action_trail
        from 
                (select distinct test_arm, personalization_id, session_id, island, event_action
                from 
                        (select *, sum(island_counter) over(partition by session_id order by hit_number) as island
                        from 
                                (select a.test_arm, 
                                                a.personalization_ID, 
                                                a.session_ID ,
                                                a.hit_number, 
--                                                case when a.event_action in ('addToCart','addToAutoship') then a.event_action||'-'||a.event_label else a.event_action end as event_action,
                                                a.event_action,
                                                lag(a.event_action,1) over(partition by a.session_id order by hit_number) as previous_action, 
                                                case when event_action = previous_action then 0 else 1 end as island_counter
                                 from discovery_sandbox.ad_exp_ga_base_unfiltered a 
                                 -- the below subquery ensures only events until activation are considered, in case of spillover its until the second activation 
                                 join session_hit_window b 
                                        on a.session_id = b.session_id
                                        and a.hit_number <= b.max_hit
--                                 where a.event_action IN ('productClick','addToCart','addToAutoship','sign-in','ATTACH_SMARTSHELF_02')  
                                 where a.event_action IN ('addToCart','addToAutoship','sign-in','ATTACH_SMARTSHELF_02')       
                                )
                        )
                )
         group by 1,2,3
        ) a 
        left join x 
        on a.session_id = x.session_id;
commit;        


  
    

create or replace table discovery_sandbox.ad_exp_base_unfiltered_journey_distinct_2
as 
(select b.session_date, a.*
from discovery_sandbox.ad_exp_base_unfiltered_journey_distinct a 
left join exp_base_unfiltered   b 
on a.session_id = b.session_id
);
commit;


  
    

select test_arm, session_auth_flag, new_customer_flag, active_autoship_flag, event_action_trail, count(distinct session_id)  as sessions 
--ratio_to_report(sessions) over(partition by 
from discovery_sandbox.ad_exp_base_unfiltered_journey_distinct
where test_arm NOT IN ('CONTROL', 'VARIANT_01','VARIANT_02')
and test_arm like ('VARIANT_02->%')

group by 1,2,3,4,5; 

--summarizing 
--
--select test_arm, max(ratio), count(distinct event_action_trail), sum(sessions)
--from 
--(
--select *, ratio_to_report(sessions) over(partition by test_arm order by sessions desc) as ratio
--from 
--(select test_arm, event_action_trail, count(distinct session_id) as sessions 
--from discovery_sandbox.ad_exp_base_unfiltered_journey_distinct
--where test_arm IN ('CONTROL','VARIANT_01','VARIANT_02','FALLBACK')
--group by 1,2)
--)
--group by 1
--;

  
select test_arm, event_action_trail, count(distinct session_id) as sessions 
from discovery_sandbox.ad_exp_base_unfiltered_journey_distinct
where test_arm IN ('CONTROL','VARIANT_01','VARIANT_02','FALLBACK')
group by 1,2;
   
     
select count(*) from (
select test_arm, event_action_trail, count(distinct session_id) as sessions 
from discovery_sandbox.ad_exp_base_unfiltered_journey_distinct
where test_arm like 'VARIANT_02->%'
group by 1,2
);
  
     
     
select * from discovery_sandbox.ad_exp_ga_base_unfiltered where session_id = '2023021400000016389622362490940251676383060';

   
      
      
         
select * from discovery_sandbox.ad_exp_base_unfiltered_journey_distinct limit 10;
where event_action_trail = 'addToCart->ATTACH_SMARTSHELF_02->addToCart->ATTACH_SMARTSHELF_02->addToCart->ATTACH_SMARTSHELF_02->addToCart->ATTACH_SMARTSHELF_02->addToCart->ATTACH_SMARTSHELF_02->addToCart->sign-in->addToCart->ATTACH_SMARTSHELF_02';




order by hit_number

UNION 

select unique_visit_id, personalization_id, type, page_title, hit_number, event_action, event_label  
from ga.ga_sessions_hits where 
ga_sessions_date = '2023-02-21'
AND unique_visit_id = '2023022100000052818108436728395091676992190'
and hit_number >= 900
order by hit_number
;
