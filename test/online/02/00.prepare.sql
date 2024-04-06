drop schema if exists source cascade;
drop schema if exists dest cascade;

create schema source;
create schema dest;

create table source.test01_table01 (
    id      bigint
,   type    varchar(1)
,   data    text
);
insert into source.test01_table01  select id, id%10, id::text from generate_series (1,10000) as id;
alter table source.test01_table01 add primary key (id);

----------------------
create table dest.test01_table01 (
    id      bigint
,   type    varchar(1)
,   data    text
);
alter table dest.test01_table01 add primary key (id);

