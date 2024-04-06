\set ON_ERROR_STOP  true
call pgpro_redefinition._start_service_job();

call pgpro_redefinition.register_table(
    configuration_name                  => '06-copy-fdw-callback-auto'
,   type                                => pgpro_redefinition._type_deferred()
,   kind                                => pgpro_redefinition._kind_redef()
,   source_table_name                   => 'table_fdw_copy_callback_auto'
,   source_schema_name                  => 'src'
,   dest_table_name                     => 'table_fdw_copy_callback_auto'
,   dest_schema_name                    => 'fdw_dst'
,   callback_name                       => null::varchar
,   callback_schema_name                => null::varchar
,   dest_pkey                           => '[{"pos": 1, "name": "id1", "type": "bigint"}, {"pos": 2, "name": "id2", "type": "bigint"}]'::jsonb
,   loop_redef                          => 25
,   loop_apply                          => 25
,   sleep_apply                         => 100
,   sleep_redef                         => 100
,   rows_redef                          => 500
,   rows_apply                          => 1000
)
;

do $$
declare
    conf_name varchar(63) default '06-copy-fdw-callback-auto';
begin
    if not exists(
        select  *
        from pgpro_redefinition.redef_table rt
       where true
         and rt.configuration_name = '06-copy-fdw-callback-auto'
         and rt.type = pgpro_redefinition._type_deferred()
         and rt.kind = pgpro_redefinition._kind_redef()
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
