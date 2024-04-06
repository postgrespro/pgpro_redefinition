create schema if not exists src ;
drop table if exists src.table_any_callback_hand cascade ;
create table src.table_any_callback_hand (
    id1      bigint
,   id2      bigint
,   type    text
,   data    text
,   type_data text
,   primary key (id1,id2)
);
insert into src.table_any_callback_hand
select id1, id2,  id2::text as type, random()::text
from generate_series (1,1000) as id1
,   generate_series (1,10) as id2;

-------------------------------------
-- процедура преобразования данных при копировании
create or replace function public.callback__any_hand (
    source_old        src.table_any_callback_hand
,   source_new        src.table_any_callback_hand
) returns void as
$redef$
declare
begin
    if source_old is null then
        update src.table_any_callback_hand
           set type_data = coalesce(source_new.type, ' - ') || coalesce(source_new.data, ' - ')
            where id1 = source_new.id1 and id2 = source_new.id2;
    elsif source_new is null then

    elsif source_new <> source_old then
            update src.table_any_callback_hand
            set type_data = coalesce(source_new.type, ' - ') || coalesce(source_new.data, ' - ')
            where id1 = source_old.id1 and id2 = source_old.id2;
    end if;
end;
$redef$
language plpgsql;