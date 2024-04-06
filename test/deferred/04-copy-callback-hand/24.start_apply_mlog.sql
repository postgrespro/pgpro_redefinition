call pgpro_redefinition.start_apply_mlog(
    configuration_name => '04-copy-callback-hand'
);

do $$
declare
    conf_name varchar(63) default '04-copy-callback-hand';
begin
   if not exists(
        select *
        from pgpro_redefinition.actual_redef_schedule_jobs j
        join pgpro_redefinition.redef_table r on r.schedule_jobs_apply_id[1] = j.id
        where true
          and j.job_type = pgpro_redefinition._job_type_apply_data()
          and j.canceled = false
          and j.error is null
          and j.status in ('processing','submitted')
          and r.configuration_name = '04-copy-callback-hand'
          and r.status_apply = pgpro_redefinition._status_apply_started()
   ) then
        raise exception '% : error to start apply data',conf_name;
   end if;
end;
$$;
