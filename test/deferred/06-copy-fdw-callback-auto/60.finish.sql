call pgpro_redefinition.finish_table(
    configuration_name => '06-copy-fdw-callback-auto'
);

do $$
declare
    conf_name varchar(63) default '06-copy-fdw-callback-auto';
begin
   if exists(
        select *
        from pgpro_redefinition.redef_table r
        where true
          and r.configuration_name = '06-copy-fdw-callback-auto'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

   if not exists(
        select *
        from pgpro_redefinition.redef_table_archive r
        where true
          and r.configuration_name = '06-copy-fdw-callback-auto'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select * from src.table_fdw_copy_callback_auto
        except
        select * from fdw_dst.table_fdw_copy_callback_auto
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select * from fdw_dst.table_fdw_copy_callback_auto
        except
        select * from src.table_fdw_copy_callback_auto
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

end;
$$;
/*
select *
from pgpro_redefinition.inc_stat order by ts_start desc ;
call pgpro_redefinition.abort_table(
    configuration_name => '06-copy-fdw-callback-auto'
);

 */