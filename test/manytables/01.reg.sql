\set ON_ERROR_ROLLBACK  true
--------------
do
$$
declare
    i int;
    schemaname  text;
    tablename   text;
begin
    for schemaname, tablename in (
        select t.schemaname, t.tablename
        from pg_tables t
        where t.schemaname = 'src'
          and t.tablename ~ 'table'
          and t.tablename !~ '_'
    ) loop
        call pgpro_redefinition.register_table(
            configuration_name                  => 'manytables-copy-callback-auto-'||tablename
        ,   type                                => 'deferred'
        ,   kind                                => 'copy'
        ,   source_table_name                   => tablename
        ,   source_schema_name                  => 'src'
        ,   dest_table_name                     => tablename
        ,   dest_schema_name                    => 'dst'
        ,   callback_name                       => null::varchar
        ,   callback_schema_name                => null::varchar
        ,   dest_pkey                           => null
        ,   loop                                => 10
        ,   rows                                => 10
        );
    end loop;
end;
$$;

select * from pgpro_redefinition.redef_table;

