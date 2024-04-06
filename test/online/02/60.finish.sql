call pgpro_redefinition.finish_table(
    configuration_name => 'test01_table01'
);

do $$
declare
    conf_name varchar(63) default 'test01_table01';
begin
   if exists(
        select *
        from pgpro_redefinition.redef_table r
        where true
          and r.configuration_name = 'test01_table01'
   ) then
        raise exception '% : error to finish finish_table',conf_name;
   end if;

   if not exists(
        select *
        from pgpro_redefinition.redef_table_archive r
        where true
          and r.configuration_name = 'test01_table01'
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select * from source.test01_table01
        except
        select * from dest.test01_table01
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;

    if exists(
        select * from source.test01_table01
        except
        select * from dest.test01_table01
   ) then
        raise exception '% : error to start finish_table',conf_name;
   end if;
end;
$$;

/*
call pgpro_redefinition.abort_table(
    configuration_name => 'test01_table01_any'
);
 */
