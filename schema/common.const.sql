
create or replace function pgpro_redefinition._default_schema(
) returns text as
$body$
    select 'pgpro_redefinition';
$body$ language sql immutable;

create or replace function pgpro_redefinition._default_jobs_apply_count(
) returns integer as
$body$
    select 5;
$body$ language sql immutable;

create or replace function pgpro_redefinition._default_jobs_redef_count(
) returns integer as
$body$
    select 5;
$body$ language sql immutable;

create or replace function pgpro_redefinition._default_lsn_partition_multiplier(
) returns integer as
$body$
    select 1;
$body$ language sql immutable;

create or replace function pgpro_redefinition._default_create_next_sub_part(
) returns integer as
$body$
    select 3;
$body$ language sql immutable;

create or replace function pgpro_redefinition._default_lsn_step(
) returns bigint as
$body$
    select 0x5fffffff;
$body$ language sql immutable;

create type pgpro_redefinition.commit_type as enum (
    'commit'
,   'autonomous'
);

create or replace function pgpro_redefinition._commit_type_commit(
) returns pgpro_redefinition.commit_type as
$body$
    select 'commit'::pgpro_redefinition.commit_type;
$body$ language sql immutable;

create or replace function pgpro_redefinition._commit_type_autonomous(
) returns pgpro_redefinition.commit_type as
$body$
    select 'commit'::pgpro_redefinition.commit_type;
$body$ language sql;

create type pgpro_redefinition.type as enum (
    'online'    -- онлайн
,   'deferred'  -- отложенная
);

create or replace function pgpro_redefinition._type_online(
) returns pgpro_redefinition.type as
$body$
    select 'online'::pgpro_redefinition.type;
$body$ language sql immutable;

create or replace function pgpro_redefinition._type_deferred(
) returns pgpro_redefinition.type as
$body$
    select 'deferred'::pgpro_redefinition.type;
$body$ language sql immutable;

create type pgpro_redefinition.kind as enum (
    'redef'      -- таблица приемник должна существовать
,   'any'       -- любое действие с данными таблицы, можно копировать, можно разбить на 2 и более таблиц. Таблица приемник не устанавливается
);

create or replace function pgpro_redefinition._kind_redef(
) returns pgpro_redefinition.kind as
$body$
    select 'redef'::pgpro_redefinition.kind;
$body$ language sql immutable;

create or replace function pgpro_redefinition._kind_any(
) returns pgpro_redefinition.kind as
$body$
    select 'any'::pgpro_redefinition.kind;
$body$ language sql immutable;

create type pgpro_redefinition.job_type as enum (
    'redef_data'
,   'apply_data'
);

create or replace function pgpro_redefinition._job_type_redef_data(
) returns pgpro_redefinition.job_type as
$body$
    select 'redef_data'::pgpro_redefinition.job_type;
$body$ language sql immutable;

create or replace function pgpro_redefinition._job_type_apply_data(
) returns pgpro_redefinition.job_type as
$body$
    select 'apply_data'::pgpro_redefinition.job_type;
$body$ language sql immutable;

create type pgpro_redefinition.flow_type as enum (
    'flow_type_separate'
,   'flow_type_general'
);

create or replace function pgpro_redefinition._flow_type_separate(
) returns pgpro_redefinition.flow_type as
$body$
    select 'flow_type_separate'::pgpro_redefinition.flow_type;
$body$ language sql immutable;

create or replace function pgpro_redefinition._flow_type_general(
) returns pgpro_redefinition.flow_type as
$body$
    select 'flow_type_general'::pgpro_redefinition.flow_type;
$body$ language sql immutable;

create or replace function pgpro_redefinition._default_on_error_stop(
) returns boolean as
$body$
    select true;
$body$ language sql immutable;

create or replace function pgpro_redefinition._param_service_job_id(
) returns text as
$body$
    select 'service_job_id';
$body$ language sql immutable;

create or replace function pgpro_redefinition._param_service_resubmit_interval_sec(
) returns integer as
$body$
    select 60;
$body$ language sql immutable;

create or replace function pgpro_redefinition._prepared_statement_name_template(
) returns text as
$body$
    select 'redef_query_%1$s';
$body$ language sql immutable;
