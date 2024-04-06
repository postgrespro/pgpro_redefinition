/*create or replace function get_count_rows(
) returns integer as
$body$
    select 2;
$body$ language sql;*/

call ins_data (insert_to_random_table => true);

/*

select rt.configuration_name, count(m.*)
from pgpro_redefinition.redef_table rt
left join pgpro_redefinition.mlog m on m.config_id = rt.id
where rt.configuration_name ~ 'manytables-copy-callback-auto'
group by rt.configuration_name
order by 2 desc ;

*/