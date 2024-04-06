call pgpro_redefinition._start_service_job();

call pgpro_redefinition.register_table(
    configuration_name                  => '05-any-callback-hand'
,   type                                => 'deferred'
,   kind                                => 'any'
,   source_table_name                   => 'table_any_callback_hand'
,   source_schema_name                  => 'src'
,   dest_table_name                     => null
,   dest_schema_name                    => null
,   callback_name                       => 'callback__any_hand'::varchar
,   callback_schema_name                => 'public'::varchar
,   dest_pkey                           => null
,   loop_redef                          => 25
,   loop_apply                          => 25
,   rows_redef                          => 500
,   rows_apply                          => 1000
);

do $$
declare
    conf_name varchar(63) default '05-any-callback-hand';
begin
    if not exists(
        select  *
        from pgpro_redefinition.redef_table rt
       where true
         and rt.configuration_name = '05-any-callback-hand'
         and rt.type = 'deferred'
         and rt.kind = 'any'
         and rt.status = 'config_registered'
         and rt.callback_generated = false
         and rt.captured = false
         and rt.status_redef is null
         and rt.status_apply is null
    ) then
        raise exception '% : error in register_table',conf_name;
    end if;
end;
$$;
