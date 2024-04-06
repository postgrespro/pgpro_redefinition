create sequence pgpro_redefinition.redef_id_seq start 1 as bigint;

create table pgpro_redefinition.redef_table(
    id                                                  bigint default nextval('pgpro_redefinition.redef_id_seq') not null
,   configuration_name                                  varchar(63) not null
,   type                                                pgpro_redefinition.type not null
,   kind                                                pgpro_redefinition.kind
,   status                                              varchar(100) not null
,   source_table_name                                   varchar(63) not null
,   source_schema_name                                  varchar(63) not null
,   source_table_oid                                    oid
,   dest_table_name                                     varchar(63)
,   dest_schema_name                                    varchar(63)
,   callback_name                                       varchar(63)
,   callback_schema_name                                varchar(63)
,   callback_generated                                  boolean
,   source_trigger_name                                 varchar(63)
,   source_trigger_function_name                        varchar(63)
,   source_trigger_schema_function_name                 varchar(63)
,   source_pkey                                         jsonb
,   dest_pkey                                           jsonb
,   jobs                                                integer default 1
,   sleep_redef                                         integer not null default 100
,   sleep_apply                                         integer not null default 100
,   rows_redef                                          integer not null default 100
,   rows_apply                                          integer not null default 100
,   loop_redef                                          integer not null default 30
,   loop_apply                                          integer not null default 30
,   captured                                            boolean default false not null
,   schedule_wait_stop                                  integer default 10
,   redef_proc_name                                     varchar(63)
,   redef_proc_schema_name                              varchar(63)
,   redef_loop_proc_name                                varchar(63)
,   redef_loop_proc_schema_name                         varchar(63)
,   schedule_jobs_copy_id                               bigint[]
,   status_redef                                        varchar(100)
,   apply_mlog_row_proc_name                            varchar(63)
,   apply_mlog_row_proc_schema_name                     varchar(63)
,   apply_proc_name                                     varchar(63)
,   apply_proc_schema_name                              varchar(63)
,   apply_loop_proc_name                                varchar(63)
,   apply_loop_proc_schema_name                         varchar(63)
,   schedule_jobs_apply_id                              bigint[]
,   status_apply                                        varchar(100)
,   weight                                              integer not null
,   mlog_part_table_name                                varchar(63)
,   constraint rt_configuration_name_pk                 primary key (configuration_name)
,   constraint rt_sourde_dest_unq                       unique (source_table_name, source_schema_name, dest_table_name, dest_schema_name)
,   constraint rt_id_unq                                unique (id)
,   constraint rt_configuration_name_not_empty_ck       check (configuration_name <> '')
,   constraint rt_source_table_name_not_empty_ck        check (source_table_name <> '')
,   constraint rt_source_schema_name_not_empty_ck       check (source_schema_name <> '')
,   constraint rt_dest_table_name_not_empty_ck          check (dest_table_name <> '')
,   constraint rt_dest_schema_name_not_empty_ck         check (dest_schema_name <> '')
,   constraint rt_jobs_ck                               check (jobs in(0,1))
,   constraint rt_sleep_redef_ck                        check (sleep_redef >= 0)
,   constraint rt_sleep_apply_ck                        check (sleep_apply >= 0)
,   constraint rt_callback_name_ck                      check (callback_name <> '')
,   constraint rt_rows_redef_more_0_ck                  check (rows_redef >= 0)
,   constraint rt_rows_apply_more_0_ck                  check (rows_apply >= 0)
,   constraint rt_loop_redef_more_10_and_less_60_ck     check (loop_redef >= 1 and loop_redef <=60)
,   constraint rt_loop_apply_more_10_and_less_60_ck     check (loop_apply >= 1 and loop_apply <=60)
);

create table pgpro_redefinition.redef_table_archive (
    like            pgpro_redefinition.redef_table
,   ts              timestamp default now()
);

create table pgpro_redefinition.redef_last_pkey(
    configuration_name              varchar(63) not null
,   pkey                       jsonb not null
,   primary key                     (configuration_name)
,   foreign key                     (configuration_name) references pgpro_redefinition.redef_table
);

create table pgpro_redefinition.redef_last_pkey_archive (
    like            pgpro_redefinition.redef_last_pkey
,   ts              timestamp default now()
);

create table pgpro_redefinition.redef_highest_pkey(
    configuration_name              varchar(63) not null
,   pkey                            jsonb not null
,   primary key                     (configuration_name)
,   foreign key                     (configuration_name) references pgpro_redefinition.redef_table
);

create table pgpro_redefinition.redef_highest_pkey_archive (
    like            pgpro_redefinition.redef_highest_pkey
,   ts              timestamp default now()
);

create table pgpro_redefinition.inc_stat (
    configuration_name              varchar(63) not null
,   job_type                        pgpro_redefinition.job_type not null
,   dest_selected                   integer
,   dest_inserted                   integer
,   ts_start                        timestamp
,   ts_finish                       timestamp
,   primary key                     (configuration_name, job_type, ts_start, ts_finish)
,   foreign key                     (configuration_name) references pgpro_redefinition.redef_table
);
create index inc_stat_ts_ndx    on pgpro_redefinition.inc_stat(ts_start,ts_finish);

create table pgpro_redefinition.inc_stat_archive (
    like            pgpro_redefinition.inc_stat
,   ts              timestamp default now()
);

create table pgpro_redefinition.mlog(
    config_id           bigint not null
,   table_oid           oid not null
,   lsn                 pg_lsn not null
,   pos                 bigint generated always as identity
,   before              json
,   after               json
,   primary key         (config_id, lsn, pos)
) partition by list (config_id);
create table pgpro_redefinition.mlog_part_default partition of pgpro_redefinition.mlog default;

create table pgpro_redefinition.mlog_last_applied_pkey(
    config_id                       bigint not null
,   lsn                             pg_lsn not null
,   pos                             bigint
,   primary key                     (config_id)
,   foreign key                     (config_id) references pgpro_redefinition.redef_table(id)
);

create table pgpro_redefinition.mlog_last_applied_pkey_archive (
    like            pgpro_redefinition.mlog_last_applied_pkey
,   ts              timestamp default now()
);

create sequence pgpro_redefinition.sub_part_desc_id_seq start 1 as bigint;
create table pgpro_redefinition.sub_part_mlog_desc(
    id                              bigint primary key
,   config_id                       bigint not null
,   sub_part_table_name             varchar(63) not null
,   lsn_from                        pg_lsn not null
,   lsn_to                          pg_lsn not null
,   is_deleted                      boolean default false
,   deleted_ts                      timestamp
,   num_sub_part                    integer not null
,   foreign key (config_id) references pgpro_redefinition.redef_table(id)
,   unique (config_id,num_sub_part)
,   unique (sub_part_table_name)
);
create index on pgpro_redefinition.sub_part_mlog_desc(id,lsn_to) where is_deleted = false;

create table pgpro_redefinition.sub_part_mlog_desc_archive (
    like            pgpro_redefinition.sub_part_mlog_desc
,   ts              timestamp default now()
);

create table pgpro_redefinition.redef_weight(
    config_id               bigint primary key
,   job_type                pgpro_redefinition.job_type not null
,   locked                  boolean not null default false
,   weight                  integer not null
);

create table pgpro_redefinition.general_jobs(
    job_type                pgpro_redefinition.job_type not null
,   job_id                  bigint                      not null
);

create table pgpro_redefinition.redefinition_config(
    param                text primary key
,   value                text
);

