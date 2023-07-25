use schema discovery_sandbox;
use schema discovery_sandbox;
use role discovery_developer;
use database edldb;

set experiment = 'REVERSE_ATTACH_EXPERIMENT_01';
set start_date = '2023-06-24'; -- start of test (1st day)
set end_date = '2023-07-23'; 
  /*evaluating on 7/19 based on data uptill 7/18*/


CREATE OR REPLACE TABLE 
  discovery_sandbox.d_exp_pool_reverse_attach_original AS
  ( SELECT 
      * 
    FROM discovery_sandbox.prd_d_d_dis_expr_pool
    WHERE 
      experiment = 'REVERSE_ATTACH_EXPERIMENT_01'
    AND session_date BETWEEN $start_date AND $end_date
  ) 
;
COMMIT;


----- final check creating experiment base removing spillovers and comparing counts by test_arm with sampling
--create or replace temp table a as 
--(
--  select
--  t1.experiment,
--  t1.session_date,
--  t1.session_id,
--  t1.personalization_id,
--  t1.test_arm
----  row_number() over(partition by  t1.personalization_id order by SESSION_DATE) as rnum
--  from discovery_sandbox.d_exp_pool_reverse_attach_original as t1
--  where 
--    t1.session_date between $start_date AND $end_date
--  and t1.experiment = 'REVERSE_ATTACH_EXPERIMENT_01'
--  qualify count(distinct t1.test_arm)
--  over (partition by t1.personalization_id) = 1
--
--);
--
--
--select test_arm, count(distinct personalization_ID), count(distinct session_id), count(*)
--from 
--a
--group by 1;


---change experiment name
update discovery_sandbox.d_exp_pool_reverse_attach_original
set experiment = 'REVERSE_ATTACH_EXPERIMENT_01_WO_SAMPLING'
where experiment = 'REVERSE_ATTACH_EXPERIMENT_01';commit;


---insert into onboarding table for this experiment to be picked up by dashboard 
--delete from discovery_sandbox.prd_d_n_dis_expr_onboard where experiment_tag= 'REVERSE_ATTACH_EXPERIMENT_01_WO_SAMPLING'; commit;
insert into discovery_sandbox.prd_d_n_dis_expr_onboard values('REVERSE_ATTACH_EXPERIMENT_01_WO_SAMPLING','2023-06-24','2023-07-23',false, 'ANON_PID'); commit;


--- removing SAMPLE_OUT records to remove sampling 
delete from discovery_sandbox.d_exp_pool_reverse_attach_original where test_arm = 'SAMPLE_OUT'; commit;

--final insert into pool table 
insert into discovery_sandbox.prd_d_d_dis_expr_pool 
(select * from discovery_sandbox.d_exp_pool_reverse_attach_original); commit;



/***************************************************************
Below section for switching the VARIANT AND CONTROL Groups from the prod experiment to be able to compare V1 vs V2 
***************************************************************/
/*
Current 	New
CONTROL 	VARIANT1
VARIANT1	CONTROL
VARIANT2	VARIANT2
*/
/*Code to insert records switching the CONTROL -> VARIANT1 and VARIANT1 -> CONTROL */


---insert into onboarding table for this experiment to be picked up by dashboard 
--delete from discovery_sandbox.prd_d_n_dis_expr_onboard where experiment_tag= 'REVERSE_ATTACH_EXPERIMENT_01_SWITCHED'; commit;
insert into discovery_sandbox.prd_d_n_dis_expr_onboard values('REVERSE_ATTACH_EXPERIMENT_01_SWITCHED','2023-06-24','2023-07-23',false, 'ANON_PID'); commit;


---getting all records with original sampling for experiment
CREATE OR REPLACE TABLE 
  discovery_sandbox.d_exp_pool_reverse_attach_w_sampling AS
  ( SELECT 
      * 
    FROM discovery_sandbox.prd_d_d_dis_expr_pool
    WHERE 
      experiment = 'REVERSE_ATTACH_EXPERIMENT_01'
    AND session_date BETWEEN $start_date AND $end_date
  ) 
;
COMMIT;


---temp table updating the test_arms for the switch 
CREATE OR REPLACE TABLE discovery_sandbox.d_exp_pool_reverse_attach_w_sampling_switched as 
SELECT 
  session_date 
  , personalization_ID 
  , 'REVERSE_ATTACH_EXPERIMENT_01_SWITCHED' AS experiment 
  , CASE WHEN test_arm = 'CONTROL' THEN 'VARIANT_01' 
        WHEN test_arm = 'VARIANT_01' THEN 'CONTROL'
    ELSE test_arm END AS test_arm 
  , session_ID 
  , customer_ID
FROM discovery_sandbox.d_exp_pool_reverse_attach_w_sampling
;
commit;




--- checking if the switch reflects in counts 
select * from 
(select 'old', test_arm, count(*), count(distinct session_ID), count(distinct personalization_ID)
from discovery_sandbox.d_exp_pool_reverse_attach_w_sampling
group by 1,2
union
select 'new', test_arm, count(*), count(distinct session_ID), count(distinct personalization_ID)
from discovery_sandbox.d_exp_pool_reverse_attach_w_sampling_switched
group by 1,2
UNION 

select 'original', test_arm, count(*), count(distinct session_ID), count(distinct personalization_ID)
from discovery_sandbox.prd_d_d_dis_expr_pool where experiment = 'REVERSE_ATTACH_EXPERIMENT_01'
AND session_date BETWEEN $start_date AND $end_date
group by 1,2
)
order by 1,2 ;




--final insert into pool table 
insert into discovery_sandbox.prd_d_d_dis_expr_pool 
(select * from discovery_sandbox.d_exp_pool_reverse_attach_w_sampling_switched); commit;



SELECT 
  experiment 
  , test_arm 
  , COUNT(*) 
  , COUNT(DISTINCT session_ID) 
  , COUNT(DISTINCT personalization_ID)
FROM discovery_sandbox.prd_d_d_dis_expr_pool 
WHERE 
  experiment IN ('REVERSE_ATTACH_EXPERIMENT_01','REVERSE_ATTACH_EXPERIMENT_01_SWITCHED')
AND session_date BETWEEN $start_date AND $end_date
GROUP BY 1,2
ORDER BY 2,1;


--select * from 
--discovery_sandbox.prd_d_n_dis_expr_onboard
--WHERE 
--  experiment_tag IN ('REVERSE_ATTACH_EXPERIMENT_01','REVERSE_ATTACH_EXPERIMENT_01_SWITCHED')




----- final check creating experiment base removing spillovers and comparing counts by test_arm with sampling
create or replace temp table a1 as 
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
  and t1.experiment IN ('REVERSE_ATTACH_EXPERIMENT_01'/*,'REVERSE_ATTACH_EXPERIMENT_01_SWITCHED'*/)
  qualify count(distinct t1.test_arm)
  over (partition by t1.personalization_id) = 1

);

--select distinct experiment, test_arm from a
--
--
select experiment,test_arm, count(distinct personalization_ID), count(distinct session_id), count(*)
from a1
group by 1,2
order by 2,1;


