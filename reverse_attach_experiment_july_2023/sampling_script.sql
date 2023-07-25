use schema discovery_sandbox;
use schema discovery_sandbox;
use role discovery_developer;
use database edldb;

set experiment = 'REVERSE_ATTACH_EXPERIMENT_01';
set start_date = '2023-06-24'; -- start of test (1st day)
set end_date = '2023-07-23'; 

/*********************************************************************
***************** Latest run on 7/24 based on data uptill 7/23 
*********************************************************************/
CREATE TABLE 
  discovery_sandbox.backup_dis_expr_pool_0724_pre_sampling AS
  ( SELECT 
      * 
    FROM discovery_sandbox.prd_d_d_dis_expr_pool 
  ) 
;
COMMIT;


/*********************************************************************
*****************  Cleaning up from prior sampling *****************
*********************************************************************/

select distinct experiment 
FROM discovery_sandbox.prd_d_d_dis_expr_pool 
where experiment like '%REVERSE%';

/*removing synthetic parallel inserts for experiment*/ 
--delete from discovery_sandbox.prd_d_d_dis_expr_pool where experiment = 'REVERSE_ATTACH_EXPERIMENT_01_WO_SAMPLING';commit;
--delete from discovery_sandbox.prd_d_d_dis_expr_pool where experiment = 'REVERSE_ATTACH_EXPERIMENT_01_SWITCHED';commit;

/* removing the test_arm = sample_OUT records from prior sampling */
select distinct test_arm from discovery_sandbox.prd_d_d_dis_expr_pool where experiment = 'REVERSE_ATTACH_EXPERIMENT_01';
--delete from discovery_sandbox.prd_d_d_dis_expr_pool where experiment = 'REVERSE_ATTACH_EXPERIMENT_01' and test_arm = 'SAMPLE_OUT';commit;



/*********************************************************************
*****************  Sampling for the latest date range *****************
*********************************************************************/

--- removing spillovers this already accounts for the artificial spillovers induced for activations into CONTROL during promo period 7/3 - 7/9
create or replace temp table experiment_base as 
(
  select
  t1.experiment,
  t1.session_date,
  t1.session_id,
  t1.personalization_id,
  t1.test_arm
  from discovery_sandbox.prd_d_d_dis_expr_pool as t1
  where 
    t1.session_date between $start_date AND $end_date
  and t1.experiment = 'REVERSE_ATTACH_EXPERIMENT_01'
--  and test_arm !='SAMPLE_OUT'
  -- remove spill over
  qualify count(distinct t1.test_arm)
  over (partition by t1.personalization_id) = 1
);



/*Getting the PIDs that are mapped with other PIDs to a single session. These are same session PID spillover 
and we will not include these PIDs in measurement of the experiment */

create or replace temp table session_ID_multiple_PIDs as 
select session_ID from experiment_Base group by 1 having count(distinct personalization_ID)>1;

create or replace temp table session_multi_PIds as 
(select distinct personalization_ID
from experiment_base 
where session_ID IN (select session_ID from experiment_Base group by 1 having count(distinct personalization_ID)>1)
); 

--select * from experiment_base where session_ID = '2023062700000012925819070465459181687901151';
--select count(*) from session_multi_PIds;   -- 55,945 PIDs
-- select count(*) from (select session_ID from experiment_Base group by 1 having count(distinct personalization_ID)>1); -- 24,5412 session_IDs

--select test_arm, count(distinct session_ID) from experiment_base where personalization_ID IN (select * from session_multi_pids)
--group by 1; 
-- EXCLUDE_CONTROL_ACT	4244
--VARIANT_01	13779
--CONTROL	13553
--VARIANT_02	16265 

/* removing  PIDs that were ever mapped to a session with other PIDs */
CREATE 
OR 
REPLACE temp TABLE experiment_base2 AS
( SELECT
    t1.experiment 
    , t1.session_date 
    , t1.session_id 
    , t1.personalization_id 
    , t1.test_arm
FROM experiment_base AS t1
  WHERE 
    t1.personalization_ID NOT IN 
    ( SELECT personalization_ID FROM session_multi_PIds) 
-- QUALIFY COUNT(DISTINCT t1.personalization_id) over ( PARTITION BY  t1.session_id) = 1 
);



/* row numbers for selecting first session for each PID
 excluding the control activations which may not have had a second session to get filtered out as spillovers */
create or replace temp table experiment_base3 as 
(
  select
  t1.experiment,
  t1.session_date,
  t1.session_id,
  t1.personalization_id,
  t1.test_arm,
  row_number() over(partition by  t1.personalization_id order by SESSION_DATE) as rnum
  from experiment_base2 as t1 
  where test_arm !=  'EXCLUDE_CONTROL_ACT'
);

--SELECT A.TEST_ARM, 
--        count(distinct a.personalization_ID), 
--        count(distinct(a.session_ID)) as sessions, 
--        count(distinct(case when sess_add_to_cart = 1 then a.session_ID end)) as atc_sessions, 
--        count(distinct(case when order_cnt > 0 and sess_add_to_cart = 1 then a.session_ID end)) as atc_purchase_sessions,
--        count(distinct(case when order_cnt > 0 and sess_add_to_cart != 1 then a.session_ID end)) as non_atc_purchase_sessions
--FROM 
--    experiment_base3 a
--INNER JOIN 
--    discovery_sandbox.prd_f_d_expr_sess_metrics b
--ON 
--    a.session_date = b.session_date
--AND a.session_id = b.session_id 
--group by 1;



/* only keep session where sess_add_to_cart = TRUE and TEST_ARM is not 'FALLBACK' */
create or replace temp table experiment_base_filtered as 
SELECT 
    a.*
    , b.SESSION_AUTH_FLAG
    , b.NEW_CUSTOMER_FLAG
    , b.ACTIVE_AUTOSHIP_FLAG
    , b.DEVICE_CATEGORY
    , b.CHANNEL
    , CONCAT_WS(',', SESSION_AUTH_FLAG, NEW_CUSTOMER_FLAG, ACTIVE_AUTOSHIP_FLAG, DEVICE_CATEGORY, CHANNEL) as segment
FROM 
    experiment_base3 a
INNER JOIN 
    discovery_sandbox.prd_f_d_expr_sess_metrics b
ON 
    a.session_date = b.session_date
AND a.session_id = b.session_id 
WHERE 
    b.sess_add_to_cart = 1 --- this is redundant as its the activation criteria but leaving it here just to be more exact
and a.rnum= 1   --- selecting only first session a.k.a activation of the PID into experiment, for segment stratification
;




/* get segment sampling count: segments which have non-empty PID from all test arms*/
create or replace temp table segment_sample_count as 
with tmp as 
(
select
     TEST_ARM
    ,segment
    ,count(distinct PERSONALIZATION_ID) as segment_n_pid
from experiment_base_filtered
group by 1,2
order by 2,1
)
select 
    segment
    , count(distinct TEST_ARM) as n_test_group
    , min(segment_n_pid) as threshold
from tmp
group by 1
having n_test_group = 3
;


--select count(*), sum(threshold)*3 from segment_sample_count;
-- 224	1885110



/*-- sampling based on min threshold per segment */
create or replace temp table experiment_pid_sampled as 
select
      e.*
from experiment_base_filtered as e
join SEGMENT_SAMPLE_COUNT as c
on e.SEGMENT = c.SEGMENT
qualify DENSE_RANK() OVER (PARTITION BY e.TEST_ARM,e.SEGMENT ORDER BY random()) <= c.threshold
;




--SELECT 
--  test_arm 
--  , COUNT(DISTINCT personalization_ID) 
----  , COUNT(DISTINCT session_ID) 
--FROM experiment_pid_sampled 
--GROUP BY 1 
--;

--VARIANT_01	628370
--VARIANT_02	628370
--CONTROL	628370

--select a.test_arm, 
--        count(distinct a.session_ID ) as sessions,
--        count(distinct(case when order_cnt > 0 then a.session_ID end)) as purchase_sessions,
--        purchase_sessions/sessions::numeric(38,6) as session_cvr 
--from experiment_pid_sampled p 
--inner join experiment_base a
--on a.personalization_Id = p.personalization_ID 
--INNER JOIN 
--    discovery_sandbox.prd_f_d_expr_sess_metrics b
--ON 
--    a.session_date = b.session_date
--AND a.session_id = b.session_id 
--group by 1;
--
--
--
--select a.test_arm, 
--        count(distinct a.session_ID ) as sessions,
--        count(distinct(case when order_cnt > 0 then a.session_ID end)) as purchase_sessions,
--        purchase_sessions/sessions::numeric(38,6) as session_cvr 
--from /*experiment_pid_sampled p 
--inner join*/ experiment_base a
--/*on a.personalization_Id = p.personalization_ID*/ 
--INNER JOIN 
--    discovery_sandbox.prd_f_d_expr_sess_metrics b
--ON 
--    a.session_date = b.session_date
--AND a.session_id = b.session_id 
--where a.personalization_ID not in (select distinct personalization_ID from session_multi_PIds)
--group by 1;



/*- final PIDs that need to be in Sample stored in table */
CREATE or REPLACE TABLE 
  discovery_sandbox.reverse_attach_insample_pids_0724 AS
  ( SELECT 
      * 
    FROM experiment_pid_sampled 
  ) 
;
COMMIT;

---final sessions that will be in sample 
--select
--    a.TEST_ARM
--    ,count(distinct a.PERSONALIZATION_ID)
--    ,count(distinct a.SESSION_ID)
--from experiment_base3 a 
--inner join experiment_pid_sampled b 
--on a.personalization_id = b.personalization_id
--group by 1
--order by 1
--;

/*********************************************************************
*****************  Updates to the pool table for sampling *****************
*********************************************************************/
create or replace temp table x as
(
  select
  t1.experiment,
  t1.session_date,
  t1.session_id,
  t1.personalization_id,
  t1.test_arm
  from discovery_sandbox.prd_d_d_dis_expr_pool as t1
  where 
    t1.session_date between $start_date AND $end_date
  and t1.experiment = $experiment
--  and test_arm !='SAMPLE_OUT'
);



/* In the pool table, we need a SESSION_ID to be inserted, assigning rownumbers to pick the first session_ID 
during the timeperiod for each "To be excluded" PID **/ 
/*discovery_sandbox.reverse_attach_insample_pids_0719 table created above containing in sample PIDs*/
CREATE or replace temp TABLE pid_sessions_sampling_status AS
( SELECT 
    x.* ,  row_number() over(partition by x.personalization_ID order by x.session_Date) as rnum,
    case when b.personalization_ID is not null then 'IN' else 'OUT' end as sampling_status
  FROM x 
  LEFT JOIN (select distinct personalization_id from  discovery_sandbox.reverse_attach_insample_pids_0724) b 
   on 1= 1
   and x.personalization_id = b.personalization_id       
);


--select sampling_status, test_arm, count(distinct personalization_ID), count(*), count(distinct session_ID)
-- from pid_sessions_sampling_status  group by 1,2 order by 1,2;

/*-- final rows to be inserted with a different test_arm value to appear as spillover in dashboard */
create or replace temp table insert_values as 
select session_date, personalization_ID, experiment, 'SAMPLE_OUT' as test_arm, session_id, null as customer_id 
from pid_sessions_sampling_status
where rnum = 1
and sampling_status = 'OUT';


--select experiment, test_arm, count(distinct personalization_ID) from insert_values group by 1,2;
select test_arm, count(distinct personalization_ID), count(distinct session_ID) from pid_sessions_sampling_status where sampling_status = 'IN' group by 1;
select session_ID from pid_sessions_sampling_status where sampling_status = 'IN' group by 1 having count(distinct personalization_ID)>1 limit 10;

select * from pid_sessions_sampling_status where session_ID = '2023062700000012925819070465459181687901151';

select session_ID from pid_sessions_sampling_status  group by 1 having count(distinct sampling_status)>1 limit 10;
select test_arm, count(distinct personalization_ID), count(distinct session_ID) from pid_sessions_sampling_status where session_ID in 
(select session_ID from pid_sessions_sampling_status where sampling_status = 'IN') group by 1;

/*creating backup of prod clean table JUST IN CASE */
CREATE OR REPLACE TABLE 
  discovery_sandbox.prd_d_d_dis_expr_pool_prodbackup_0725 AS 
  ( SELECT 
      * 
    FROM discovery_sandbox.prd_d_d_dis_expr_pool 
  ) 
;
COMMIT;




/*************************************************************
For testing before the final insert into prod table 
*****************************************************************/

--drop table prd_d_d_dis_expr_pool_backup_0719_test; commit;
CREATE OR REPLACE TABLE 
  discovery_sandbox.prd_d_d_dis_expr_pool_backup_0724_test AS 
  ( SELECT 
      * 
    FROM discovery_sandbox.prd_d_d_dis_expr_pool 
  ) 
;
COMMIT;


insert into discovery_sandbox.prd_d_d_dis_expr_pool_backup_0724_test
(select session_date, personalization_ID, experiment, test_arm, session_id, customer_id from insert_values); commit;

--- removing spillovers this already accounts for the artificial spillovers induced for activations into CONTROL during promo period 7/3 - 7/9
create or replace temp table experiment_base_test as 
(
  select
  t1.experiment,
  t1.session_date,
  t1.session_id,
  t1.personalization_id,
  t1.test_arm
--  row_number() over(partition by  t1.personalization_id order by SESSION_DATE) as rnum
  from discovery_sandbox.prd_d_d_dis_expr_pool_backup_0724_test as t1
  where 
    t1.session_date between $start_date AND $end_date
  and t1.experiment = 'REVERSE_ATTACH_EXPERIMENT_01'
  qualify count(distinct t1.test_arm)
  over (partition by t1.personalization_id) = 1

);

--select test_arm, count(distinct personalization_ID), count(distinct session_id), count(*)
--from 
--experiment_base_test
--group by 1;

--VARIANT_01	628370	749723	749723
--CONTROL	628370	750765	750765
--VARIANT_02	628370	750218	750218



/*************************************************************
Final INSERT into the PRODUCTION POOL table 
**************************************************************/
insert into discovery_sandbox.prd_d_d_dis_expr_pool
(select session_date, personalization_ID, experiment, test_arm, session_id, customer_id from insert_values); commit;

select count(distinct personalization_ID) from discovery_sandbox.prd_d_d_dis_expr_pool
where test_arm = 'SAMPLE_OUT'
;



----check for count after insert 
--- final check creating experiment base removing spillovers and comparing counts by test_arm with sampling
create or replace temp table a as 
(
  select
  t1.experiment,
  t1.session_date,
  t1.session_id,
  t1.personalization_id,
  t1.test_arm
--  row_number() over(partition by  t1.personalization_id order by SESSION_DATE) as rnum
  from discovery_sandbox.prd_d_d_dis_expr_pool as t1
  where 
    t1.session_date between $start_date AND $end_date
  and t1.experiment = 'REVERSE_ATTACH_EXPERIMENT_01'
  qualify count(distinct t1.test_arm)
  over (partition by t1.personalization_id) = 1

);


--select test_arm, count(distinct personalization_ID), count(distinct session_id), count(*)
--from 
--a
--group by 1;


