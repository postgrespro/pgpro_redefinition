-- далее начинаем переливать данные в стороннюю таблицу
-- этот пункт можно пропустить

call pgpro_redefinition.pause_apply_mlog(
    configuration_name => '03-copy-callback-auto'
);

do $$
declare
    conf_name varchar(63) default '03-copy-callback-auto';
begin
   if not exists(
        select *
        from pgpro_redefinition.redef_table r
        where true
          and r.configuration_name = '03-copy-callback-auto'
          and r.status_apply = pgpro_redefinition._status_apply_pause()
          and r.schedule_jobs_apply_id is null
   ) then
        raise exception '% : error to pause apply mlog',conf_name;
   end if;
end;
$$;
