call pgpro_redefinition.pause_redef_table(
    configuration_name => '03-copy-callback-auto'
);


do $$
declare
    conf_name varchar(63) default '03-copy-callback-auto';
    cnt_befor   bigint;
    cnt_after   bigint;
begin
   if not exists(
        select *
        from pgpro_redefinition.redef_table r
        where true
          and r.configuration_name = '03-copy-callback-auto'
          and r.status_redef = pgpro_redefinition._status_redef_pause()
          and r.schedule_jobs_copy_id is null
   ) then
        raise exception '% : error to pause redef table',conf_name;
   end if;
    perform pg_sleep(2);

    select count(*) into cnt_befor
       from (
        select *
         from pgpro_redefinition.mlog m
         join pgpro_redefinition.redef_table r on r.id = m.config_id
        where true
          and r.configuration_name = '03-copy-callback-auto'
          and m.before is null
      ) q;

       select count(*) into cnt_after
       from (
        select *
         from pgpro_redefinition.mlog m
         join pgpro_redefinition.redef_table r on r.id = m.config_id
        where true
          and r.configuration_name = '03-copy-callback-auto'
          and m.before is null
      ) q;

    if cnt_befor <> cnt_after then
        raise exception '% : error to pause redef table',conf_name;
   end if;

end;
$$;


