call pgpro_redefinition.finish_table(
    configuration_name =>'03-copy-callback-auto'
);

do $$
declare
    conf_name varchar(63) default '03-copy-callback-auto';
begin
   if exists(
        select *
        from pgpro_redefinition.redef_table r
        where true
          and r.configuration_name = '03-copy-callback-auto'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

   if not exists(
        select *
        from pgpro_redefinition.redef_table_archive r
        where true
          and r.configuration_name = '03-copy-callback-auto'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select * from src.table_copy_callback_auto
        except
        select * from dst.table_copy_callback_auto
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select * from dst.table_copy_callback_auto
        except
        select * from src.table_copy_callback_auto
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

end;
$$;

/*
call pgpro_redefinition.abort_table(
    configuration_name =>'03-copy-callback-auto'
);
 */