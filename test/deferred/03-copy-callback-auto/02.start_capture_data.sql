call pgpro_redefinition.start_capture_data(
    configuration_name                  => '03-copy-callback-auto'
);

--test
do $$
declare
    conf_name varchar(63) default '03-copy-callback-auto';
begin
    if not exists(
        select  *
        from pgpro_redefinition.redef_table rt
       where true
         and rt.configuration_name = '03-copy-callback-auto'
         and rt.type = 'deferred'
         and rt.kind = 'redef'
         and rt.status = 'config_registered'
         and rt.callback_generated = true
         and rt.captured = true
         and rt.status_redef is null
         and rt.status_apply is null
    ) then
        raise exception '% : error in capture_data',conf_name;
    end if;
end;
$$;

insert into src.table_copy_callback_auto
select id1, id2,  id2::text as type, (id1 * id2)::text
from generate_series (-10,-1) as id1
,   generate_series (-1,-1) as id2;

update src.table_copy_callback_auto
   set data = 'updated'
 where id1 = 1;

delete from src.table_copy_callback_auto
where id1 = 2;

do $$
declare
    conf_name varchar(63) default '03-copy-callback-auto';
    cnt         integer;
begin
    select count(*) into cnt
      from (
        select  *
        from pgpro_redefinition.mlog m
        join pgpro_redefinition.redef_table r on r.id = m.config_id
       where true
         and r.configuration_name = '03-copy-callback-auto'
         and m. before is null
         and m.after is not null
   ) q
    ;

    if cnt <> 10 then
        raise exception '% : error capture data - insert ',conf_name;
    end if;

    select count(*) into cnt
      from (
        select  *
        from pgpro_redefinition.mlog m
        join pgpro_redefinition.redef_table r on r.id = m.config_id
       where true
         and r.configuration_name = '03-copy-callback-auto'
         and m. before is not null
         and m.after is not null
    ) q;

    if cnt <> 10 then
        raise exception '% : error capture data - update ',conf_name;
    end if;
    select count(*) into cnt
      from (
      select *
        from pgpro_redefinition.mlog m
        join pgpro_redefinition.redef_table r on r.id = m.config_id
       where true
         and r.configuration_name = '03-copy-callback-auto'
         and m. before is not null
         and m.after is null
    ) q;
    if cnt <> 10 then
        raise exception '% : error capture data - delete ',conf_name;
    end if;
end;
$$;


