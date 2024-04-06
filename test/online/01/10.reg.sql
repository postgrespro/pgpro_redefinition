call pgpro_redefinition._start_service_job();

call pgpro_redefinition.register_table(
    configuration_name                  => 'test01_table01_any'::text
,   type                                => pgpro_redefinition._type_online()
,   kind                                => pgpro_redefinition._kind_any()
,   source_table_name                   => 'test01_table01'::text
,   source_schema_name                  => 'source_any'::text
,   dest_table_name                     => null::text
,   dest_schema_name                    => null::text
,   callback_name                       => 'callback__test01_table01_true_false'::text
,   callback_schema_name                => 'dest_any'::text
,   loop_redef                          => 25
,   loop_apply                          => 25
,   rows_redef                          => 1000
,   rows_apply                          => 1000
,   sleep_redef                         => 10
);

do $$
declare
    conf_name varchar(63) default 'test01_table01_any';
begin
    if not exists(
        select  *
        from pgpro_redefinition.redef_table rt
       where true
         and rt.configuration_name = 'test01_table01_any'
         and rt.type = pgpro_redefinition._type_online()
         and rt.kind = pgpro_redefinition._kind_any()
         and rt.status = pgpro_redefinition._status_config_registered()
         and rt.callback_generated = false
         and rt.captured = false
         and rt.status_redef is null
         and rt.status_apply is null
    ) then
        raise exception '% : error in register_table',conf_name;
    end if;
end;
$$;
