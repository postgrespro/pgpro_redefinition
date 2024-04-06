call pgpro_redefinition.start_redef_table(
    configuration_name => 'test01_table01_any'
);

select pg_sleep(5);

do $$
declare
    conf_name varchar(63) default 'test01_table01_any';
    cnt         bigint;
begin
   if not exists(
        select *
        from pgpro_redefinition.actual_redef_schedule_jobs j
        join pgpro_redefinition.redef_table r on r.schedule_jobs_copy_id[1] = j.id
        where true
          and j.canceled = false
          and j.error is null
          and j.status in ('processing','submitted')
          and r.configuration_name = 'test01_table01_any'
          and r.status_redef = pgpro_redefinition._status_redef_started()
   ) then
        raise exception '% : error to copy data',conf_name;
   end if;

   select count(*) into cnt
     from (
          select *
            from pgpro_redefinition.inc_stat
            where configuration_name = 'test01_table01_any'
              and job_type = pgpro_redefinition._job_type_redef_data()
              and dest_inserted <> 0
              and dest_selected <>0
            order by ts_finish desc
        ) q;
    if cnt < 1 then
        raise exception '% : error to add statistic into inc_stat',conf_name;
    end if;

     select count(*) into cnt
       from (
          select *
            from pgpro_redefinition.inc_stat
            where configuration_name = 'test01_table01_any'
              and job_type = pgpro_redefinition._job_type_redef_data()
              and dest_inserted <> 0
              and dest_selected <>0
            order by ts_finish desc
      ) q;
    if cnt < 1 then
        raise exception '% : error to add rows into mlog table', conf_name;
    end if;
end;
$$;

select pg_sleep(10);
