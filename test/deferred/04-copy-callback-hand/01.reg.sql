call pgpro_redefinition._start_service_job();

call pgpro_redefinition.register_table(
    configuration_name                  => '04-copy-callback-hand'
,   type                                => pgpro_redefinition._type_deferred()
,   kind                                => pgpro_redefinition._kind_redef()
,   source_table_name                   => 'table_copy_callback_hand'
,   source_schema_name                  => 'src'
,   dest_table_name                     => 'table_copy_callback_hand'
,   dest_schema_name                    => 'dst'
,   callback_name                       => 'callback__copy_hand'::varchar
,   callback_schema_name                => 'public'::varchar
,   dest_pkey                           => null
,   loop_redef                          => 25
,   loop_apply                          => 25
,   rows_redef                          => 500
,   rows_apply                          => 1000
);

--------------

do $$
declare
    conf_name varchar(63) default '04-copy-callback-hand';
begin
    if not exists(
        select  *
        from pgpro_redefinition.redef_table rt
       where true
         and rt.configuration_name = '04-copy-callback-hand'
         and rt.type = pgpro_redefinition._type_deferred()
         and rt.kind = pgpro_redefinition._kind_redef()
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
