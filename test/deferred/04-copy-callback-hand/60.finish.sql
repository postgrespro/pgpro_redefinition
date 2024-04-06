call pgpro_redefinition.finish_table(
    configuration_name => '04-copy-callback-hand'
);

do $$
declare
    conf_name varchar(63) default '04-copy-callback-hand';
begin
   if exists(
        select *
        from pgpro_redefinition.redef_table r
        where true
          and r.configuration_name = '04-copy-callback-hand'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

   if not exists(
        select *
        from pgpro_redefinition.redef_table_archive r
        where true
          and r.configuration_name = '04-copy-callback-hand'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select *, type || ' - ' ||data from src.table_copy_callback_hand
        except
        select * from dst.table_copy_callback_hand
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select * from dst.table_copy_callback_hand
        except
        select *, type || ' - ' ||data from src.table_copy_callback_hand
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;
end;
$$;

/*
call pgpro_redefinition.abort_table(
    configuration_name => '04-copy-callback-hand'
);
*/
