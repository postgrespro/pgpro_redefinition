create extension if not exists pgcrypto;
create schema if not exists src;
drop table if exists src.table_fdw_copy_callback_auto cascade ;
create table src.table_fdw_copy_callback_auto (
    id1      bigint
,   id2      bigint
,   type    text
,   data    text
,   f       bytea
,   primary key (id1,id2)
);

insert into src.table_fdw_copy_callback_auto
select id1, id2,  id2::text as type, random()::text, 'gen_random_bytes(1000)' as f
from generate_series (1,10000) as id1
,   generate_series (1,10) as id2;

------------------------------------------
create schema if not exists dst;
drop table if exists dst.table_fdw_copy_callback_auto cascade;

create table dst.table_fdw_copy_callback_auto (
    id1             bigint
,   id2             bigint
,   type            text
,   data            text
,   f               bytea
,   primary key (id2,id1)
) partition by hash (id2);
create table dst.table_fdw_copy_callback_auto_part01 partition of dst.table_fdw_copy_callback_auto for values with (modulus 2, remainder 0);
create table dst.table_fdw_copy_callback_auto_part02 partition of dst.table_fdw_copy_callback_auto for values with (modulus 2, remainder 1);

-----------
\c postgres

create extension if not exists postgres_fdw;
drop server if exists dest_server cascade ;

do
$$
declare
    create_fdw_server text default $sert$create server dest_server
        foreign data wrapper postgres_fdw
        options (host %1$L, port %2$L, dbname %3$L);
$sert$;
    create_fdw_user text default $user$create user mapping for %1$s
        server dest_server
        options (user %2$L, password %3$L);
$user$;
begin
    create_fdw_server := format(create_fdw_server, 'localhost', current_setting('port'), current_database());
    raise info '%', create_fdw_server;
    execute create_fdw_server;

    create_fdw_user:= format(create_fdw_user, "current_user"(),"current_user"(),"current_user"()  );
    raise info '%', create_fdw_user;
    execute create_fdw_user;
end;
$$;

create schema if not exists fdw_dst;
drop foreign table if exists fdw_dst.table_fdw_copy_callback_auto;
create foreign table fdw_dst.table_fdw_copy_callback_auto (
        id1             bigint
    ,   id2             bigint
    ,   type            text
    ,   data            text
    ,   f               bytea
)
server dest_server
options (schema_name 'dst', table_name 'table_fdw_copy_callback_auto');

---
