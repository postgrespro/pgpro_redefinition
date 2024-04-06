call pgpro_redefinition.finish_table(
    configuration_name => '05-any-callback-hand'
);

do $$
declare
    conf_name varchar(63) default '05-any-callback-hand';
begin
   if  exists(
        select *
        from pgpro_redefinition.redef_table r
        where true
          and r.configuration_name = '05-any-callback-hand'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

   if not exists(
        select *
        from pgpro_redefinition.redef_table_archive r
        where true
          and r.configuration_name = '05-any-callback-hand'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select *
          from src.table_any_callback_hand
          where true
            and type_data is null
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;
end;
$$;

/*
call pgpro_redefinition.abort_table(
    configuration_name => '05-any-callback-hand'
);
*/
