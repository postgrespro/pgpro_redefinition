call pgpro_redefinition.start_capture_data(
    configuration_name                  => 'test01_table01'
);

do $$
declare
    conf_name varchar(63) default 'test01_table01';
begin
    if not exists(
        select  *
        from pgpro_redefinition.redef_table rt
       where true
         and rt.configuration_name = 'test01_table01'
         and rt.type = pgpro_redefinition._type_online()
         and rt.kind = pgpro_redefinition._kind_redef()
         and rt.status = pgpro_redefinition._status_config_registered()
         and rt.callback_generated = true
         and rt.captured = true
         and rt.status_redef is null
         and rt.status_apply is null
    ) then
        raise exception '% : error in capture_data',conf_name;
    end if;
end;
$$;


insert into source_any.test01_table01
select (round(random()*10000000)::int)
       , (round(random()*10000000)::int),
       (round(random())::int)::boolean as type, random()::text
  from generate_series(1,100);

update source_any.test01_table01
set data = random()::text
where id1 =1;

delete from source_any.test01_table01 where id1 = 2;
