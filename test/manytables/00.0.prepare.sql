--todo in progress
-- функционал и тест по работе с большим количеством таблиц не готов
create schema if not exists src ;
create extension if not exists pgcrypto ;
-- generate tables
create or replace function get_random_json(
) returns json as
$body$
    select array_to_json(array_agg(row_to_json(t)))
    from (select * from pg_class limit (round(random()*10000))) t
;
$body$ language sql;

create or replace function get_count_tables(
) returns integer as
$body$
    select 10;
$body$ language sql;


do
$$
declare
    n       integer default get_count_tables();
    sql     text;
begin
    for i in 1 ..n loop
        execute format('drop table if exists src.table%s', i::text);

        sql := format('create table src.table%s (
    id          bigserial primary key
,   data        text default random ()::text
,   b           bytea default gen_random_bytes(round(random()*1000)::int+1)
,   j           jsonb default get_random_json()
)'
    ,   i::text
    );
    execute sql;
    end loop;

end;
$$;

create schema if not exists dst;
do
$$
declare
    n       integer default get_count_tables();
    sql     text;
begin
    for i in 1 ..n loop
        execute format('drop table if exists dst.table%s', i::text);

        sql := format('create table dst.table%s (
    id          bigint primary key
,   data        text
,   b           bytea
,   j           jsonb
)'
    ,   i::text
    );
    execute sql;
    end loop;
end;
$$;

create or replace function get_count_rows(
) returns integer as
$body$
    select 1000;
$body$ language sql;

create or replace procedure ins_data (
    insert_to_random_table        boolean default false
) as
$$
declare
    n       integer default get_count_tables();
    sql     text;
    r       int;
    c       bigint default 0;
    min     integer default 1;
begin
    if insert_to_random_table then
        min = round(random()*(n-1))+1;
        n=min;
    end if;
    for i in min ..n loop
        begin autonomous
            r := round((random()*get_count_rows())+1);
            c:= c+ (r);
            raise info 'i %, r %, c %',i,r,c;
            sql := format(
                'insert into src.table%s  select  from generate_series(1,%s) id;'
            ,   i::text
            ,   r::text
            );
        end;
    execute sql;
    end loop;
end;
$$ language plpgsql;

call ins_data (insert_to_random_table => false);

create or replace function get_count_rows(
) returns integer as
$body$
    select 2;
$body$ language sql;