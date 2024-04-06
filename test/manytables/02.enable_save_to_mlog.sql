do
$$
declare
    i int;
    configuration_name  text;
begin
    for configuration_name in (
        select rt.configuration_name from pgpro_redefinition.redef_table rt where rt.configuration_name ~ 'manytables-copy-callback-auto'
    ) loop
        call pgpro_redefinition.enable_save_to_mlog(
            configuration_name                  => configuration_name
        );
    end loop;
end;
$$;

/*

select rt.configuration_name, count(m.*)
from pgpro_redefinition.redef_table rt
left join pgpro_redefinition.mlog m on m.config_id = rt.id
where rt.configuration_name ~ 'manytables-copy-callback-auto'
group by rt.configuration_name
order by 2 desc ;

*/
