create schema if not exists src ;
drop table if exists src.table_copy_callback_auto;
create table src.table_copy_callback_auto (
    id1      bigint
,   id2      bigint
,   type    text
,   data    text
,   primary key (id1,id2)
);
insert into src.table_copy_callback_auto
select id1, id2,  id2::text as type, random()::text
from generate_series (1,1000) as id1
,   generate_series (1,10) as id2;

------------------------------------------
create schema if not exists dst;
drop table if exists dst.table_copy_callback_auto;

create table dst.table_copy_callback_auto (
    id1             bigint
,   id2             bigint
,   type            text
,   data            text
,   primary key (id2,id1)
) partition by hash (id2);
create table dst.table_copy_callback_auto_part01 partition of dst.table_copy_callback_auto for values with (modulus 2, remainder 0);
create table dst.table_copy_callback_auto_part02 partition of dst.table_copy_callback_auto for values with (modulus 2, remainder 1);
