call pgpro_redefinition.start_redef_table(
    configuration_name => '05-any-callback-hand'
);


select pg_sleep(5);

do $$
declare
    conf_name varchar(63) default '05-any-callback-hand';
begin
   if not exists(
        select *
        from pgpro_redefinition.actual_redef_schedule_jobs j
        join pgpro_redefinition.redef_table r on r.schedule_jobs_copy_id[1] = j.id
        where true
          and j.job_type = pgpro_redefinition._job_type_redef_data()
          and j.canceled = false
          and j.error is null
          and j.status in ('processing','submitted')
          and r.configuration_name = '05-any-callback-hand'
          and r.status_redef = pgpro_redefinition._status_redef_started()
   ) then
        raise exception '% : error to copy data',conf_name;
   end if;
end;
$$;
