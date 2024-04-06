\set ON_ERROR_STOP  true
call pgpro_redefinition._start_service_job();

call pgpro_redefinition.register_table(
    configuration_name                  => '03-copy-callback-auto'
,   type                                => pgpro_redefinition._type_deferred()
,   kind                                => pgpro_redefinition._kind_redef()
,   source_table_name                   => 'table_copy_callback_auto'
,   source_schema_name                  => 'src'
,   dest_table_name                     => 'table_copy_callback_auto'
,   dest_schema_name                    => 'dst'
,   callback_name                       => null::varchar
,   callback_schema_name                => null::varchar
,   dest_pkey                           => null
,   loop_redef                          => 25
,   loop_apply                          => 25
,   rows_redef                          => 500
,   rows_apply                          => 1000
);

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
         and rt.captured = false
         and rt.status_redef is null
         and rt.status_apply is null
    ) then
        raise exception '% : error in register_table',conf_name;
    end if;
end;
$$;
