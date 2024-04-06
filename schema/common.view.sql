create or replace view pgpro_redefinition.by_min as
select s.configuration_name
     , s.job_type
     , date_trunc('min', s.ts_finish) ts_min
     , count(1) as count
     , sum(s.dest_selected) as sum_dest_selected
     , sum(s.dest_inserted) as sum_dest_inserted
     , avg((s.ts_finish - s.ts_start)) as avg_duration
     , min((s.ts_finish - s.ts_start)) as min_duration
     , max((s.ts_finish - s.ts_start)) as max_duration
from pgpro_redefinition.inc_stat s
group by s.configuration_name, s.job_type, ts_min
order by ts_min desc;

create or replace view pgpro_redefinition.by_hour as
select s.configuration_name
     , s.job_type
     , date_trunc('hour', s.ts_finish) ts_min
     , count(1) as count
     , sum(s.dest_selected) as sum_dest_selected
     , sum(s.dest_inserted) as sum_dest_inserted
     , avg((s.ts_finish - s.ts_start)) as avg_duration
     , min((s.ts_finish - s.ts_start)) as min_duration
     , max((s.ts_finish - s.ts_start)) as max_duration
from pgpro_redefinition.inc_stat s
group by s.configuration_name, s.job_type, ts_min
order by ts_min desc;


create or replace view pgpro_redefinition.all_schedule_jobs as
select  *
from (
    select s.*
         , rank() OVER (PARTITION BY id ORDER BY attempt DESC) as rank
      from schedule.all_job_status s
) q
where q.rank = 1;

create or replace view pgpro_redefinition.actual_redef_schedule_jobs as
    select jobs.job_type
         , s.*
      from pgpro_redefinition.all_schedule_jobs s
      join(
           select unnest(schedule_jobs_copy_id) as job_id
                , pgpro_redefinition._job_type_redef_data() as job_type
             from pgpro_redefinition.redef_table r
            union all
            select unnest(schedule_jobs_apply_id) as job_id
                , pgpro_redefinition._job_type_apply_data() as job_type
                  from pgpro_redefinition.redef_table r
            union all
            select value::bigint, null
              from pgpro_redefinition.redefinition_config
              where param = pgpro_redefinition._param_service_job_id()
            ) jobs on jobs.job_id = s.id
