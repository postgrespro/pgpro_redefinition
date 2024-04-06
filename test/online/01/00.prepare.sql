
drop schema if exists source_any cascade;
drop schema if exists dest_any cascade;

create schema source_any;
create schema dest_any;

create table source_any.test01_table01 (
    id1      bigint
,   id2      bigint
,   type    boolean
,   data    text
);

insert into source_any.test01_table01
select id1, id2,  (round(random())::int)::boolean as type, random()::text
from generate_series (1,10000) as id1
,   generate_series (1,10) as id2;

alter table source_any.test01_table01 add primary key (id1,id2);

----------------------
create table dest_any.test01_table01_true (
    guid    uuid
,   id1      bigint
,   id2      bigint
,   data    text
);
alter table dest_any.test01_table01_true add primary key (guid);
alter table dest_any.test01_table01_true add unique (id1, id2);

----------------------
create table dest_any.test01_table01_false (
    guid    uuid
,   id1      bigint
,   id2      bigint
,   data    text
);
alter table dest_any.test01_table01_false add primary key (guid);
alter table dest_any.test01_table01_false add unique (id1, id2);
----

create or replace function dest_any.callback__test01_table01_true_false(
    in out source_old source_any.test01_table01
,   in out source_new source_any.test01_table01
)
as
$$
declare
begin
    if source_old is null then
        if source_new.type = true then
            insert into dest_any.test01_table01_true
                values (uuid_in(md5(random()::text || random()::text)::cstring), source_new.id1, source_new.id2,source_new.data)
                on conflict (id1,id2) do update
                        set id1 = source_new.id1
                        ,   id2 = source_new.id2
                        ,   data = source_new.data;
        else
            insert into dest_any.test01_table01_false
                values (uuid_in(md5(random()::text || random()::text)::cstring), source_new.id1, source_new.id2,source_new.data)
                on conflict (id1,id2) do update
                        set id1 = source_new.id1
                        ,   id2 = source_new.id2
                        ,   data = source_new.data;
        end if;
    elsif source_new is null then
        if source_old.type = true then
            delete from dest_any.test01_table01_true where id1 = source_old.id1 and id2 = source_old.id2;
        else
            delete from dest_any.test01_table01_false where id1 = source_old.id1 and id2 = source_old.id2;
        end if;
    elsif source_new <> source_old then
        if source_old.type = true then
            update dest_any.test01_table01_true
            set id1 = source_new.id1
            ,   id2 = source_new.id2
            ,   data = source_new.data
            where id1 = source_old.id1 and id2 = source_old.id2;
        else
            update dest_any.test01_table01_false
            set id1 = source_new.id1
            ,   id2 = source_new.id2
            ,   data = source_new.data
            where id1 = source_old.id1 and id2 = source_old.id2;
        end if;
    end if;
    source_old.data = 'updated_old';
    source_new.data = 'updated_new';
end;
$$ language plpgsql;
