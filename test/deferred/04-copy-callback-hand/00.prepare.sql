
drop function if exists callback__copy_hand(src.table_copy_callback_hand,src.table_copy_callback_hand) ;

create schema if not exists src ;
drop table if exists src.table_copy_callback_hand;
create table src.table_copy_callback_hand (
    id1      bigint
,   id2      bigint
,   type    text
,   data    text
,   primary key (id1,id2)
);
insert into src.table_copy_callback_hand
select id1, id2,  id2::text as type, random()::text
from generate_series (1,1000) as id1
,   generate_series (1,10) as id2;

------------------------------------------
-- создаем таблицу приемник
create schema if not exists dst;
drop table if exists dst.table_copy_callback_hand;
create table dst.table_copy_callback_hand (
    id1             bigint
,   id2             bigint
,   type            text
,   data            text
,   type_data       text
,   primary key (id2,id1)
) partition by hash (id2);
create table dst.table_copy_callback_hand_part01 partition of dst.table_copy_callback_hand for values with (modulus 2, remainder 0);
create table dst.table_copy_callback_hand_part02 partition of dst.table_copy_callback_hand for values with (modulus 2, remainder 1);

-------------------------------------
create or replace function public.callback__copy_hand (
    source_old        src.table_copy_callback_hand
,   source_new        src.table_copy_callback_hand
,   dest_old out      dst.table_copy_callback_hand
,   dest_new out      dst.table_copy_callback_hand
) as
$redef$
declare
begin
    dest_old.id1 := source_old.id1;
    dest_old.id2 := source_old.id2;
    dest_old.type := source_old.type;
    dest_old.data := source_old.data;
    dest_old.type_data := source_old.type || ' - ' ||source_old.data;

    dest_new.id1 := source_new.id1;
    dest_new.id2 := source_new.id2;
    dest_new.type := source_new.type;
    dest_new.data := source_new.data;
    dest_new.type_data := source_new.type || ' - ' ||source_new.data;
end;
$redef$
language plpgsql;