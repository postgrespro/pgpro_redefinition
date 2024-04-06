create or replace procedure pgpro_redefinition._lock_table(
    table_name                  varchar(63)
,   schema_name                 varchar(63)
,   lock_mode                   text default 'ACCESS EXCLUSIVE'
,   lock_timeout                integer default 10
) as
$body$
declare
    timeout                 text default 'set lock_timeout to %1$L;';
    lock                    text default 'lock table %1$I.%2$I in %3$s mode;';
    locks_table_mode            text[] default array [
        'ACCESS SHARE'
    ,   'ROW SHARE'
    ,   'ROW EXCLUSIVE'
    ,   'SHARE UPDATE EXCLUSIVE'
    ,   'SHARE'
    ,   'SHARE ROW EXCLUSIVE'
    ,   'EXCLUSIVE'
    ,   'ACCESS EXCLUSIVE'
    ];
begin
    lock_mode:= upper(trim(lock_mode));
    if lock_mode <> all(locks_table_mode) then
        call pgpro_redefinition._error(
            errcode         => 'DFE28'
        ,   message         => format('Lock mode %1$s is bad.'
                                ,   lock_mode
                               )
        ,   hint            => format('Use one of mode: %1$s'
                                ,   array_to_string(locks_table_mode, ', ')
                               )
        );
    end if;
    timeout := format(
        timeout
    ,   lock_timeout::text ||'s'
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM52'
    ,   message         => format('Sql to set timeout: %1$s'
                                ,   timeout
                           )
    );
    call pgpro_redefinition._execute_sql (sql => timeout);

    "lock" := format(
        "lock"
    ,   schema_name
    ,   table_name
    ,   lock_mode
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM53'
    ,   message         => format('Sql to set lock: %1$s'
                                ,   "lock"
                           )
    );
    call pgpro_redefinition._execute_sql (sql => "lock");
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._is_table_exists(
    table_name        varchar(63)
,   schema_name       varchar(63)
) returns boolean as
$body$
    select exists(
        select 1
          from pg_catalog.pg_tables t
         where t.tablename = table_name
           and t.schemaname = schema_name);
$body$ language sql;

create or replace function pgpro_redefinition._get_pkey_by_table(
    table_name                              varchar(63)
,   schema_name                             varchar(63)
) returns jsonb as
$body$
    --    [{'name':'lsn','type':'pg_lsn','value':'77/8139DB20'},{'name':'pos','type':'bigint','value':'100'}]
declare
    pkey    jsonb;
begin

    select  jsonb_agg(js)
    from (
        select
                    kcol.ordinal_position as pos
                 ,  col.column_name as name
                 ,  col.data_type as type
          from information_schema.columns col
          join information_schema.key_column_usage kcol on kcol.table_name = col.table_name
                                                       and kcol.table_schema = col.table_schema
                                                       and kcol.column_name = col.column_name
          join information_schema.table_constraints tcon on tcon.table_name = kcol.table_name
                                                       and tcon.table_schema = kcol.table_schema
                                                       and tcon.constraint_name = kcol.constraint_name
                                                       and tcon.constraint_type = 'PRIMARY KEY'
         where true
           and col.table_name = _get_pkey_by_table.table_name
           and col.table_schema = _get_pkey_by_table.schema_name
      order by kcol.ordinal_position
    ) as js into pkey;
    return pkey;

end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._check_pgpro_scheduler(
) as
$body$
declare
    pgpro_scheduler_ver             text;
begin
    if not lower(current_setting('shared_preload_libraries', true)) ~ 'pgpro_scheduler' then
        call pgpro_redefinition._error(
            errcode     => 'DFE01'
        ,   message     => 'shared_preload_libraries parameter does not contain pgpro_scheduler value'
        ,   hint        => 'Set: shared_preload_libraries = pgpro_scheduler in file postgresql.conf or using'
             'commands alter system set shared_preload_libraries = pgpro_scheduler;. '
             'This param need to restart postgres.'
        );
    end if;

    select e.installed_version into pgpro_scheduler_ver
    from pg_available_extensions e
    where name = 'pgpro_scheduler';
    if pgpro_scheduler_ver is null then
        call pgpro_redefinition._error(
            errcode     => 'DFE02'
        ,   message     => 'Extension pgpro_scheduler is not installed.'
        ,   hint        => 'Use command: create extension pgpro_scheduler; to install this extension'
                           ' in current database.'
        );
    end if;

    if not current_setting('schedule.database', true) ~ current_database() then
        call pgpro_redefinition._error(
            errcode     => 'DFE03'
        ,   message     => 'Param schedule.database does not contain current database'
        ,   hint        => format('Set: schedule.database = %1$s in file postgresql.conf or '
                                  'use command: alter system set schedule.database = %1$s; '
                                  'and select pg_reload_conf()', current_database())
        );
    end if;

    if not schedule.is_enabled() then
        call pgpro_redefinition._error(
            errcode     => 'DFE04'
        ,   message     => 'pgpro_scheduler is disabled. '
        ,   hint        => 'To enable pgpro_scheduler use command select schedule.enable() or '
                            'use command: alter system set schedule.auto_enabled = true; '
                            'and select pg_reload_conf()'
        );
    end if;
    -- todo check max_worker_processes
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._is_table_empty(
    table_name                  varchar(63)
,   schema_name                 varchar(63)
) returns boolean as
$body$
declare
    data_exists                 boolean;
begin
    execute format('select not exists(select * from %1$I.%2$I)', schema_name, table_name)
    into data_exists;
    return data_exists;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._schedule_submit_job(
    query                       text
,   name                        text
,   comments                    text
,   resubmit_limit              bigint default 1000000000
) returns bigint as
$body$
declare
    schedule_job_id                     bigint;
begin autonomous
     schedule_job_id := schedule.submit_job(
            query               => query
        ,   name                => name
        ,   comments            => comments
        ,   resubmit_limit      => resubmit_limit
    );
    return schedule_job_id;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._scheduler_cancel_job(
    job_id                  bigint
) as
$body$
declare
begin autonomous
    call pgpro_redefinition._info(
        errcode         => 'DFE74'
    ,   message         => format('Start to cancel job, job_id - %s.'
                           ,    job_id::text
                           )
    );
    perform schedule.cancel_job(job_id);

    call pgpro_redefinition._info(
        errcode         => 'DFE75'
    ,   message         => format('Cancel job sent, job_id - %s.'
                           ,    job_id::text
                           )
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._cancel_job(
    job_id                  bigint
,   schedule_wait_stop      bigint default 10000000
) as
$body$
declare
    max_wait_scheduler_stop_ts          timestamp;
    job_stopped                         boolean default false;
begin
            call pgpro_redefinition._debug(
            errcode         => 'DFM54'
        ,   message         => format('Job %1$s before cancel - %2$s '
                                    ,    job_id::text
                                    ,   clock_timestamp()
                               )
        );
    call pgpro_redefinition._scheduler_cancel_job(job_id => _cancel_job.job_id);
            call pgpro_redefinition._debug(
            errcode         => 'DFM54'
        ,   message         => format('Job %1$s after cancel - %2$s '
                                    ,    job_id::text
                                    ,   clock_timestamp()
                               )
        );
    max_wait_scheduler_stop_ts := clock_timestamp()
                    + (schedule_wait_stop::text || ' sec')::interval;

    while max_wait_scheduler_stop_ts >= clock_timestamp() and job_stopped = false  loop
        call pgpro_redefinition._debug(
            errcode         => 'DFM54'
        ,   message         => format('Job %1$s is working - %2$s '
                                    ,    job_id::text
                                    ,   clock_timestamp()
                               )
        );
        select (canceled is true and error is not  null) into job_stopped
        from schedule.all_job_status js
        where id = any (array [job_id])
        order by id desc, attempt desc
        limit (1); -- todo запрос не работает для 2 и более job-ов
        perform pg_sleep(0.1);
    end loop;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._drop_procedure(
    name                varchar(63)
,   schema              varchar(63)
,   params              varchar(63)[] default null
) as
$body$
declare
    sql                 text default 'drop procedure if exists %1$I.%2$I;';
begin
    if params is not null then
        call pgpro_redefinition._error(
            errcode     => 'DFE05'
        ,   message     => 'drop with param not implemented'
        );
    end if;
    sql := format(
        sql
    ,   schema
    ,   name
    );
    call pgpro_redefinition._debug(
        errcode     => 'DFM79'
    ,   message     => format('SQL to drop procedure: %s', sql)
    );
    call pgpro_redefinition._execute_sql (sql => sql);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._drop_function(
    name                varchar(63)
,   schema              varchar(63)
,   params              varchar(63)[] default null
) as
$body$
declare
    sql                 text default 'drop function if exists %1$I.%2$I;';
begin
    if params is not null then
        call pgpro_redefinition._error(
            errcode     => 'DFE05'
        ,   message     => 'drop with param not implemented'
        );
    end if;
    sql := format(
        sql
    ,   schema
    ,   name
    );
    call pgpro_redefinition._debug(
        errcode     => 'DFM78'
    ,   message     => format('SQL to drop function: %s', sql)
    );
    call pgpro_redefinition._execute_sql (sql => sql);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.register_table(
    configuration_name                  varchar(63)
,   type                                pgpro_redefinition.type
,   kind                                pgpro_redefinition.kind
,   source_table_name                   varchar(63)
,   source_schema_name                  varchar(63)
,   dest_table_name                     varchar(63)
,   dest_schema_name                    varchar(63)
,   callback_name                       varchar(63) default null
,   callback_schema_name                varchar(63) default null
,   dest_pkey                           jsonb default null
,   rows_redef                          integer default 1000
,   rows_apply                          integer default 1000
,   loop_redef                          integer default 30
,   loop_apply                          integer default 30
,   sleep_redef                         integer default 100
,   sleep_apply                         integer default 100
,   weight                              integer default 1
) as
$body$
declare
    sql                             text;
    config_id                       bigint;
    table_oid                       oid;
    part_table_name                 varchar(63);
    schedule_job_id                 bigint;
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    if exists(
        select 1
          from pgpro_redefinition.redef_table rt
         where rt.configuration_name = register_table.configuration_name
    ) then
        call pgpro_redefinition._error(
            errcode                                 => 'DFE06'
        ,   message                                 => format('This configuration presents in redef tables')
        );
    end if;
    if not pgpro_redefinition._is_table_exists(table_name => source_table_name, schema_name => source_schema_name) then
        call pgpro_redefinition._error(
            errcode                                 => 'DFE07'
        ,   message                                 => format('Source table %1$I.%2$I not exists', source_schema_name, source_table_name)
        );
    end if;

    select value::bigint into schedule_job_id
      from pgpro_redefinition.redefinition_config c
     where c.param = pgpro_redefinition._param_service_job_id();
    if schedule_job_id is null then
        call pgpro_redefinition._error(
            errcode         => 'DFE57'
        ,   message         => format('Service task not started. To start task use command: call pgpro_redefinition._start_service_job(); ')
        );
    end if;

    select c.oid into table_oid
      from pg_class c
     where c.relname = register_table.source_table_name
       and c.relnamespace =  register_table.source_schema_name::regnamespace;

    insert into pgpro_redefinition.redef_table (
        configuration_name
    ,   type
    ,   kind
    ,   source_table_name
    ,   source_schema_name
    ,   dest_table_name
    ,   dest_schema_name
    ,   callback_name
    ,   callback_schema_name
    ,   rows_redef
    ,   rows_apply
    ,   loop_redef
    ,   loop_apply
    ,   sleep_redef
    ,   sleep_apply
    ,   status
    ,   weight
    ,   source_table_oid
    ) values (
        register_table.configuration_name
    ,   type
    ,   register_table.kind
    ,   register_table.source_table_name
    ,   register_table.source_schema_name
    ,   register_table.dest_table_name
    ,   register_table.dest_schema_name
    ,   register_table.callback_name
    ,   register_table.callback_schema_name
    ,   register_table.rows_redef
    ,   register_table.rows_apply
    ,   register_table.loop_redef
    ,   register_table.loop_apply
    ,   register_table.sleep_redef
    ,   register_table.sleep_apply
    ,   pgpro_redefinition._status_config_new()
    ,   register_table.weight
    ,   table_oid
    ) returning id into config_id;

    if kind = pgpro_redefinition._kind_redef() then
        if callback_name is null then
            callback_name := pgpro_redefinition._generate_callback_name(configuration_name => register_table.configuration_name);
            call pgpro_redefinition._raise_log(
                loglevel                                => pgpro_redefinition._loglevel_info()
            ,   errcode                                 => 'DFM08'
            ,   message                                 => format('Callback function not set and will be generated')
            );
            callback_schema_name := pgpro_redefinition._default_schema();
            update pgpro_redefinition.redef_table
               set callback_name        = register_table.callback_name
                ,  callback_schema_name = register_table.callback_schema_name
                ,  callback_generated   = true
             where redef_table.configuration_name = register_table.configuration_name;

            sql:= pgpro_redefinition._generate_callback_function(configuration_name => register_table.configuration_name);

            call pgpro_redefinition._raise_log(
                loglevel                                => pgpro_redefinition._loglevel_notice()
            ,   errcode                                 => 'DFM06'
            ,   message                                 => format('callback function %1$I.%2$I -  sql:: %3$I not exists'
                                                                    , source_schema_name
                                                                    , source_table_name
                                                                    , sql
                                                        )
            );
            call pgpro_redefinition._execute_sql (sql => sql);
        else
           update pgpro_redefinition.redef_table
               set callback_name        = register_table.callback_name
                ,  callback_schema_name = register_table.callback_schema_name
                ,  callback_generated   = false
            where redef_table.configuration_name = register_table.configuration_name;

            call pgpro_redefinition._raise_log(
                loglevel                                => pgpro_redefinition._loglevel_info()
            ,   errcode                                 => 'DFM05'
            ,   message                                 => format(
                                'Callback function %1$I.%2$I will be used to migrate data.'
                            ,   register_table.callback_schema_name
                            ,   register_table.callback_name
                            )
            );
        end if;
    elseif kind = pgpro_redefinition._kind_any() then
       if callback_name is null or callback_name = '' then
            call pgpro_redefinition._error(
                errcode                                 => 'DFE08'
            ,   message                                 => format(E'You must specify a callback function for kind = ''%s''. Example: /n%s'
                            ,   kind
                            ,   $$
create or replace function public.callback__table01 (
    source_old in out       source_schema.source_table
,   source_new in out       source_schema.source_table
) as
$redef$
declare
begin
--
end;
$redef$
language plpgsql;$$
                                             )
            );
       end if;
        update pgpro_redefinition.redef_table
           set  callback_generated   = false
         where redef_table.configuration_name = register_table.configuration_name;
    else
         call pgpro_redefinition._error(
            errcode                                 => 'DFE09'
        ,   message                                 => format('kind % not support. Use %'
                    ,   kind
                    ,   pgpro_redefinition._kind_redef() || ' or ' || pgpro_redefinition._kind_any()
                    )
        );
    end if;
    update pgpro_redefinition.redef_table
       set source_pkey = pgpro_redefinition._get_pkey_by_table(
                            table_name  => register_table.source_table_name
                        ,   schema_name => register_table.source_schema_name
                        )
     where redef_table.configuration_name = register_table.configuration_name;

    if kind = pgpro_redefinition._kind_redef() then
        if register_table.dest_pkey is null then
            dest_pkey := pgpro_redefinition._get_pkey_by_table(
                                    table_name  => register_table.dest_table_name
                                ,   schema_name => register_table.dest_schema_name
                                );
            if dest_pkey is null then
                if  exists(select 1 from information_schema.foreign_tables ft
                         where ft.foreign_table_name =  register_table.dest_table_name
                           and ft.foreign_table_schema = register_table.dest_schema_name
                ) then
                    call pgpro_redefinition._error(
                        errcode         => 'DFE10'
                    ,   message         => format('Destination table %s.%s is fdw table. Must specify the primary key in dest_pkey'
                                        ,   register_table.dest_schema_name
                                        ,   register_table.dest_table_name
                                )
                    ,   hint            => $$Example primary key of destination table: dest_pkey=> '[{"pos": 1, "name": "id1", "type": "bigint"}, {"pos": 2, "name": "id2", "type": "bigint"}]'::jsonb $$
                    );
                else
                    call pgpro_redefinition._error(
                        errcode         => 'DFE11'
                    ,   message         => format('Impossible to find primary key of destination table %1$I.%2$I. Use param dest_pkey'
                                        ,   register_table.dest_schema_name
                                        ,   register_table.dest_table_name
                                )
                    ,   hint            => $$Example primary key of destination: dest_pkey=> '[{"pos": 1, "name": "id1", "type": "bigint"}, {"pos": 2, "name": "id2", "type": "bigint"}]'::jsonb $$
                    );
                 end if;
            end if;
        end if;
    end if;
    update pgpro_redefinition.redef_table
       set dest_pkey = register_table.dest_pkey
    where redef_table.configuration_name = register_table.configuration_name;

    update pgpro_redefinition.redef_table
       set source_trigger_function_name = pgpro_redefinition._generate_trigger_func_name(configuration_name => register_table.configuration_name)
         , source_trigger_schema_function_name = pgpro_redefinition._default_schema()
    where redef_table.configuration_name = register_table.configuration_name;

    if register_table.type = pgpro_redefinition._type_online() then
        sql:= pgpro_redefinition._generate_trigger_func(configuration_name => register_table.configuration_name);
        call pgpro_redefinition._execute_sql (sql => sql);
    end if;

    update pgpro_redefinition.redef_table
       set source_trigger_name = pgpro_redefinition._generate_trigger_name(configuration_name => register_table.configuration_name)
     where redef_table.configuration_name = register_table.configuration_name;

    if register_table.type = pgpro_redefinition._type_online() then
        sql := pgpro_redefinition._generate_trigger(register_table.configuration_name);
    elseif register_table.type = pgpro_redefinition._type_deferred() then
        sql := pgpro_redefinition._generate_deferred_trigger(register_table.configuration_name);
    end if;
    call pgpro_redefinition._execute_sql (sql => sql);

    if register_table.type = pgpro_redefinition._type_online() then
        update pgpro_redefinition.redef_table
           set redef_proc_name = pgpro_redefinition._generate_redef_proc_name(register_table.configuration_name)
             , redef_proc_schema_name = pgpro_redefinition._default_schema()
         where redef_table.configuration_name = register_table.configuration_name;
        sql := pgpro_redefinition._generate_copy_proc(register_table.configuration_name);

    elsif  register_table.type = pgpro_redefinition._type_deferred() then
        update pgpro_redefinition.redef_table
           set redef_proc_name = pgpro_redefinition._generate_redef_to_mlog_def_proc_name(register_table.configuration_name)
             , redef_proc_schema_name = pgpro_redefinition._default_schema()
         where redef_table.configuration_name = register_table.configuration_name;
        sql := pgpro_redefinition._generate_redef_to_mlog_def_proc(register_table.configuration_name);

    end if;
    call pgpro_redefinition._execute_sql (sql => sql);

    update pgpro_redefinition.redef_table
       set redef_loop_proc_name = pgpro_redefinition._generate_redef_loop_proc_name(register_table.configuration_name)
         , redef_loop_proc_schema_name = pgpro_redefinition._default_schema()
     where redef_table.configuration_name = register_table.configuration_name;

    sql := pgpro_redefinition._generate_copy_loop_proc(register_table.configuration_name);
    call pgpro_redefinition._execute_sql (sql => sql);

    if type = pgpro_redefinition._type_deferred() then
        update pgpro_redefinition.redef_table
           set apply_mlog_row_proc_name = pgpro_redefinition._generate_deferred_apply_mlog_row_proc_name (register_table.configuration_name)
             , apply_mlog_row_proc_schema_name = pgpro_redefinition._default_schema()
        where redef_table.configuration_name = register_table.configuration_name;
        sql := pgpro_redefinition._generate_deferred_apply_mlog_row_proc(register_table.configuration_name);
        call pgpro_redefinition._execute_sql (sql => sql);

        update pgpro_redefinition.redef_table
           set apply_proc_name = pgpro_redefinition._generate_deferred_apply_proc_name (register_table.configuration_name)
             , apply_proc_schema_name = pgpro_redefinition._default_schema()
        where redef_table.configuration_name = register_table.configuration_name;
        sql := pgpro_redefinition._generate_deferred_apply_proc(register_table.configuration_name);
        call pgpro_redefinition._execute_sql (sql => sql);

        update pgpro_redefinition.redef_table
           set apply_loop_proc_name = pgpro_redefinition._generate_deferred_apply_loop_proc_name (register_table.configuration_name)
             , apply_loop_proc_schema_name = pgpro_redefinition._default_schema()
        where redef_table.configuration_name = register_table.configuration_name;

        sql := pgpro_redefinition._generate_deferred_loop_proc(register_table.configuration_name);
        call pgpro_redefinition._execute_sql (sql => sql);

        part_table_name := pgpro_redefinition._create_partition(
            main_table_name                => 'mlog'
        ,   main_schema_name               => pgpro_redefinition._default_schema()
        ,   num_part                       => config_id
        ,   prefix                         => 'part'
        );
        update pgpro_redefinition.redef_table
           set mlog_part_table_name = part_table_name
        where redef_table.configuration_name = register_table.configuration_name;

        call pgpro_redefinition._check_sub_partitions(configuration_name => register_table.configuration_name);

    end if;

    call pgpro_redefinition._set_config_status(
        configuration_name  => register_table.configuration_name
    ,   status              => pgpro_redefinition._status_config_registered()
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.start_redef_table(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    schedule_query                      text default 'call %1$I.%2$I(); select schedule.resubmit()';
    schedule_name                       text default '_redef_%1$s_job%2$s';
    schedule_comments                   text default 'pgpro_redefinition - redefinition data job № %1$s of %2$I.%3$I';
    resubmit_limit                      bigint default 1000000000;
    schedule_job_id                     bigint;
    njob                                bigint  default 1;
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => start_redef_table.configuration_name
    );
    if configuration_row.status not in (pgpro_redefinition._status_config_registered()) then
        call pgpro_redefinition._error(
            errcode         => 'DFE12'
        ,   message         => format('Configuration %s has bas status - %s.  Impossible to  start copy data.'
                            ,   configuration_row.configuration_name
                            ,   configuration_row.status
                    )
        );
    end if;
    if configuration_row.captured <> true then
        call pgpro_redefinition._error(
            errcode         => 'DFE13'
        ,   message         => format('Captured data in configuration %s is not started.'
                                ,   configuration_row.configuration_name
                                ,   configuration_row.status
                                )
        ,   hint            => format($$To start capture data use procedure call pgpro_redefinition.enable_save_to_mlog('%s'); $$
                                ,   configuration_row.configuration_name
                                )
        );
    end if;
    if configuration_row.status_redef is not null then
        if configuration_row.status_redef = pgpro_redefinition._status_redef_finished() then
            call pgpro_redefinition._error(
                errcode         => 'DFE54'
            ,   message         => format('Redefinition data finished. All data processed.')
            );
        end if;
        if configuration_row.status_redef <> pgpro_redefinition._status_redef_pause() then
            call pgpro_redefinition._error(
                errcode         => 'DFE14'
            ,   message         => format('Redefinition data already started.')
            );
        end if;
    end if;
    call pgpro_redefinition._check_pgpro_scheduler();

    if configuration_row.status_redef is null then
        if configuration_row.type = pgpro_redefinition._type_deferred() then
            call pgpro_redefinition._insert_first_row_to_mlog( configuration_name =>configuration_row.configuration_name);
        elsif configuration_row.type = pgpro_redefinition._type_online() then
            if configuration_row.kind = pgpro_redefinition._kind_redef() then
                call pgpro_redefinition._insert_first_row( configuration_name => configuration_row.configuration_name);
            elseif configuration_row.kind = pgpro_redefinition._kind_any() then
                call pgpro_redefinition._select_first_row( configuration_name => configuration_row.configuration_name);
            else
                 call pgpro_redefinition._error(
                    errcode          => 'DFE09'
                ,   message          => format('kind % not support. Use %'
                                        ,   configuration_row.kind
                                        ,   pgpro_redefinition._kind_redef() || ' or ' || pgpro_redefinition._kind_any()
                                        )
                );
            end if;
        else
             call pgpro_redefinition._error(
                errcode          => 'DFE19'
            ,   message          => format('type % not support. Use %'
                                    ,   configuration_row.type
                                    ,   pgpro_redefinition._type_deferred() || ' or ' || pgpro_redefinition._type_online()
                                    )
            );
        end if;
    end if;

    schedule_query := format(
        schedule_query
    ,   configuration_row.redef_loop_proc_schema_name
    ,   configuration_row.redef_loop_proc_name
    );
    schedule_name := format(
        schedule_name
    ,   configuration_row.configuration_name
    ,   njob::text
    );
    schedule_comments := format(
        schedule_comments
    ,   njob::text
    ,   configuration_row.redef_proc_schema_name
    ,   configuration_row.redef_proc_schema_name
    );
     schedule_job_id := schedule.submit_job(
        query               => schedule_query
    ,   name                => schedule_name
    ,   comments            => schedule_comments
    ,   resubmit_limit      => resubmit_limit
    );

    update pgpro_redefinition.redef_table rt
       set schedule_jobs_copy_id = array [schedule_job_id]
     where rt.configuration_name = start_redef_table.configuration_name;

    call pgpro_redefinition._set_redef_status(
        configuration_name  => start_redef_table.configuration_name
    ,   status              => pgpro_redefinition._status_redef_started()
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.pause_redef_table(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => pause_redef_table.configuration_name
    ,   status              => array[pgpro_redefinition._status_config_registered()]
    );
    if configuration_row.status_redef <> pgpro_redefinition._status_redef_started() then
        call pgpro_redefinition._error(
            errcode         => 'DFE15'
        ,   message         => format('Impossible to pause redefenition data. Current status %s.'
                               ,    configuration_row.status_redef
                               )
        );
    end if;
            call pgpro_redefinition._info(
            errcode         => 'DFM15'
        ,   message         => format('before  pgpro_redefinition._cancel_job(.'
                               ,    configuration_row.status_redef
                               )
        );
    call pgpro_redefinition._cancel_job(
        job_id                  => configuration_row.schedule_jobs_copy_id[1]
    ,   schedule_wait_stop      => configuration_row.schedule_wait_stop
    );
   update pgpro_redefinition.redef_table
       set schedule_jobs_copy_id = null
     where redef_table.configuration_name = pause_redef_table.configuration_name;

    call pgpro_redefinition._set_redef_status(
        configuration_name  => pause_redef_table.configuration_name
    ,   status              => pgpro_redefinition._status_redef_pause()
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.restart_redef_table(
    configuration_name                  varchar(63)
) as
$body$
declare
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    call pgpro_redefinition.start_redef_table(
        configuration_name  => restart_redef_table.configuration_name
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.start_copy_table(
    configuration_name                  varchar(63)
) as
$body$
declare
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    call pgpro_redefinition.start_redef_table(
        configuration_name  => start_copy_table.configuration_name
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.restart_copy_table(
    configuration_name                  varchar(63)
) as
$body$
declare
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    call pgpro_redefinition.start_copy_table(
        configuration_name  => restart_copy_table.configuration_name
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.abort_table(
    configuration_name                  varchar(63)
,   drop_dest_table                   boolean default false
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => abort_table.configuration_name
    );
    if configuration_row.type = pgpro_redefinition._type_deferred() then
        if configuration_row.status_apply is not null and configuration_row.status_apply <> pgpro_redefinition._status_apply_pause() then
            call pgpro_redefinition.pause_apply_mlog(abort_table.configuration_name);
        end if;
        call pgpro_redefinition._drop_procedure(
            name        => configuration_row.apply_loop_proc_name
        ,   schema      => configuration_row.apply_loop_proc_schema_name
        );
        call pgpro_redefinition._drop_procedure(
            name        => configuration_row.apply_proc_name
        ,   schema      => configuration_row.apply_proc_schema_name
        );
        call pgpro_redefinition._drop_procedure(
            name        => configuration_row.apply_mlog_row_proc_name
        ,   schema      => configuration_row.apply_mlog_row_proc_schema_name
        );
        call pgpro_redefinition._set_apply_status(
            configuration_name  => abort_table.configuration_name
        ,   status              => pgpro_redefinition._status_apply_aborted()
        );
    end if;
    if configuration_row.status_redef not in (
        pgpro_redefinition._status_redef_finished()
    ,   pgpro_redefinition._status_redef_pause()
    )  then
            call pgpro_redefinition.pause_redef_table(abort_table.configuration_name);
    end if;

    call pgpro_redefinition._drop_procedure(
        name        => configuration_row.redef_loop_proc_name
    ,   schema      => configuration_row.redef_loop_proc_schema_name
    );
    call pgpro_redefinition._drop_procedure(
        name        => configuration_row.redef_proc_name
    ,   schema      => configuration_row.redef_proc_schema_name
    );
    call pgpro_redefinition._drop_procedure(
        name        => configuration_row.source_trigger_function_name
    ,   schema      => configuration_row.source_trigger_schema_function_name
    );
    if configuration_row.callback_generated then
        call pgpro_redefinition._drop_function(
            name        => configuration_row.callback_name
        ,   schema      => configuration_row.callback_schema_name
        );
    end if;
    call pgpro_redefinition._disable_source_trigger(abort_table.configuration_name);
    call pgpro_redefinition._drop_source_trigger(abort_table.configuration_name);

    if drop_dest_table then
        call pgpro_redefinition._drop_dest_table(abort_table.configuration_name);
    end if;
    call pgpro_redefinition._set_redef_status(
        configuration_name  => abort_table.configuration_name
    ,   status              => pgpro_redefinition._status_redef_aborted()
    );
    call pgpro_redefinition._set_config_status(
        configuration_name  => abort_table.configuration_name
    ,   status              => pgpro_redefinition._status_config_aborted()
    );
    call pgpro_redefinition._move_configuration_to_archive(
        configuration_name  => abort_table.configuration_name
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.finish_table(
    configuration_name                  varchar(63)
,   replace_dest_to_source_tables       boolean default false
) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    dest_inserted               integer;
    proc                        text default 'call %1$I.%2$I()';
    last_copy                   text;
    last_apply                  text;
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => finish_table.configuration_name
    );
    if configuration_row.status not in (pgpro_redefinition._status_config_registered()) then
        call pgpro_redefinition._error(
            errcode         => 'DFE16'
        ,   message         => format('Configuration %s in status: %s. In current status impossible to finish redefinition table.'
                               ,    configuration_row.configuration_name
                               ,    configuration_row.status
                               )
        );
    end if;
    if configuration_row.status_redef is not null then
        if configuration_row.status_redef not in (
            pgpro_redefinition._status_redef_started()
        ,   pgpro_redefinition._status_redef_pause()
        ,   pgpro_redefinition._status_redef_finished()
        ) then
            call pgpro_redefinition._error(
                errcode         => 'DFE17'
            ,   message         => format('Status if copy (redefinition) data  %s in status.  '
                                          'In current status impossible to finish redefinition table.'
                                   ,    configuration_row.configuration_name
                                   ,    configuration_row.status_redef
                                   )
            );
        end if;
    end if;

    if configuration_row.captured = false then
        call pgpro_redefinition._error(
            errcode         => 'DFE57'
        ,   message         => format('Capture data not started')
        ,   hint            => 'To remove configuration use function pgpro_redefinition.abort_table'
        );
    end if;

    if configuration_row.type = pgpro_redefinition._type_deferred() then
        if configuration_row.status_redef is null then
            call pgpro_redefinition._error(
                errcode         => 'DFE47'
            ,   message         => format('Copy or apply not started.')
            ,   hint            => 'To remove configuration use function pgpro_redefinition.abort_table'
            );
        end if;
    elsif configuration_row.type = pgpro_redefinition._type_online() then
        if configuration_row.status_redef is  null then
            call pgpro_redefinition._error(
                errcode         => 'DFE47'
            ,   message         => format('Copy not started.')
            ,   hint            => 'To remove configuration use function pgpro_redefinition.abort_table'
            );
        end if;
    end if;

    if configuration_row.status_apply is not null then
        if configuration_row.status_apply not in (
            pgpro_redefinition._status_apply_started()
        ,   pgpro_redefinition._status_apply_pause()
        ) then
            call pgpro_redefinition._error(
                errcode         => 'DFE18'
            ,   message         => format('Status if apply mlog is %s .'
                                          'In current status impossible to finish redefinition table.'
                                   ,    configuration_row.configuration_name
                                   ,    configuration_row.status_apply
                                   )
            );
        end if;
    end if;

    if configuration_row.status_redef is not null then
        select s.dest_inserted into dest_inserted
        from pgpro_redefinition.inc_stat s
        where true
          and s.configuration_name = configuration_row.configuration_name
          and s.job_type = pgpro_redefinition._job_type_redef_data()
        order by ts_finish desc
        limit 1;

        if dest_inserted <> 0 then
            call pgpro_redefinition._error(
                errcode         => 'DFE19'
            ,   message         => format('Copy (redefinition) data in progress. '
                                          'Impossible to finish redefinition table.'
                                   )
            ,   hint            => 'Try to finish later.'
            );
        end if;
    end if;

    if configuration_row.status_apply is not null then
        select s.dest_inserted into dest_inserted
        from pgpro_redefinition.inc_stat s
        where true
          and s.configuration_name = configuration_row.configuration_name
          and s.job_type = pgpro_redefinition._job_type_apply_data()
        order by ts_finish desc
        limit 1;
        if dest_inserted <> 0 then
            call pgpro_redefinition._error(
                errcode         => 'DFE20'
            ,   message         => format('Apply  data in progress. '
                                          'Impossible to finish redefinition table.'
                                   )
            ,   hint            => 'Try to finish later.'
            );
        end if;
    end if;
/*
    call pgpro_redefinition._lock_table(
        table_name      => configuration_row.source_table_name
    ,   schema_name     => configuration_row.source_schema_name
    ,   lock_mode       => 'ACCESS EXCLUSIVE'
    ,   lock_timeout    => 10
    );
*/
    if configuration_row.status_redef is not null then
        if configuration_row.schedule_jobs_copy_id is not null then
            call pgpro_redefinition._cancel_job(job_id => configuration_row.schedule_jobs_copy_id[1]);
        end if;
        last_copy := format(
            proc
        ,   configuration_row.redef_proc_schema_name
        ,   configuration_row.redef_proc_name
        );

        call pgpro_redefinition._execute_sql (sql => last_copy);

        select s.dest_inserted into dest_inserted
        from pgpro_redefinition.inc_stat s
        where true
          and s.configuration_name = configuration_row.configuration_name
          and s.job_type = pgpro_redefinition._job_type_redef_data()
        order by ts_finish desc
        limit 1;

        if dest_inserted <> 0 then
            call pgpro_redefinition._error(
                errcode         => 'DFE19'
            ,   message         => format('Copy (redefinition) data in progress. '
                                          'Impossible to finish redefinition table.'
                                   )
            ,   hint            => 'Try to finish later.'
            );
        end if;
    end if;
    if configuration_row.status_apply is not null then
        call pgpro_redefinition._cancel_job(job_id => configuration_row.schedule_jobs_apply_id[1]);
        last_apply := format(
            proc
        ,   configuration_row.apply_proc_schema_name
        ,   configuration_row.apply_proc_name
        );

        call pgpro_redefinition._execute_sql (sql => last_apply);

        select s.dest_inserted into dest_inserted
        from pgpro_redefinition.inc_stat s
        where true
          and s.configuration_name = configuration_row.configuration_name
          and s.job_type = pgpro_redefinition._job_type_apply_data()
        order by ts_finish desc
        limit 1;

        if dest_inserted <> 0 then
            call pgpro_redefinition._error(
                errcode         => 'DFE19'
            ,   message         => format('Copy (redefinition) data in progress. '
                                          'Impossible to finish redefinition table.'
                                   )
            ,   hint            => 'Try to finish later.'
            );
        end if;
    end if;

    call pgpro_redefinition._disable_source_trigger(finish_table.configuration_name);
    call pgpro_redefinition._drop_source_trigger(finish_table.configuration_name);
    call pgpro_redefinition._drop_procedure(
        name        => configuration_row.redef_loop_proc_name
    ,   schema      => configuration_row.redef_loop_proc_schema_name
    );
    call pgpro_redefinition._drop_procedure(
        name        => configuration_row.redef_proc_name
    ,   schema      => configuration_row.redef_proc_schema_name
    );

    if configuration_row.callback_generated then
        call pgpro_redefinition._drop_function(
            name        => configuration_row.callback_name
        ,   schema      => configuration_row.callback_schema_name
        );
    end if;

    if configuration_row.type = pgpro_redefinition._type_deferred() then
        call pgpro_redefinition._drop_procedure(
            name        => configuration_row.apply_loop_proc_name
        ,   schema      => configuration_row.apply_loop_proc_schema_name
        );
        call pgpro_redefinition._drop_procedure(
            name        => configuration_row.apply_proc_name
        ,   schema      => configuration_row.apply_proc_schema_name
        );
        call pgpro_redefinition._drop_procedure(
            name        => configuration_row.apply_mlog_row_proc_name
        ,   schema      => configuration_row.apply_mlog_row_proc_schema_name
        );
    end if;

    if configuration_row.kind = pgpro_redefinition._kind_redef() then
        if finish_table.replace_dest_to_source_tables then
            call pgpro_redefinition._replace_dest_to_source_tables(finish_table.configuration_name);
        end if;
    end if;

    if configuration_row.status_redef is not null then
        call pgpro_redefinition._set_redef_status(
            configuration_name      => finish_table.configuration_name
        ,   status                  => pgpro_redefinition._status_redef_finished()
        );
    end if;
    if configuration_row.status_apply is not null then
        call pgpro_redefinition._set_apply_status(
            configuration_name      => finish_table.configuration_name
        ,   status                  => pgpro_redefinition._status_apply_finished()
        );
    end if;
    call pgpro_redefinition._set_config_status(
        configuration_name      => finish_table.configuration_name
    ,   status                  => pgpro_redefinition._status_config_finished()
    );
    call pgpro_redefinition._drop_mlog_partition_by_configuration_name(
        configuration_name => finish_table.configuration_name
    );
    call pgpro_redefinition._move_configuration_to_archive(
        configuration_name => finish_table.configuration_name
    );

end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._get_configuration(
    configuration_name                  varchar(63)
,   status                              text[] default null
) returns pgpro_redefinition.redef_table as
$body$
declare
    configuration_row       pgpro_redefinition.redef_table;
begin
    select * into configuration_row
    from pgpro_redefinition.redef_table rt
    where rt.configuration_name = _get_configuration.configuration_name
      and (
            rt.status = any(_get_configuration.status)
         or _get_configuration.status is null
      );

    if configuration_row is null then
        call pgpro_redefinition._error(
            errcode         => 'DFE20'
        ,   message         => format('Configuration %s in status %s not found'
                                ,   _get_configuration.configuration_name
                                ,   coalesce(array_to_string(_get_configuration.status, ', '),'null')
                               )
        );
    end if;

    return configuration_row;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_callback_name(
    configuration_name                  varchar(63)
) returns varchar(63) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    callback_name               varchar(63) default '%1$I';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_callback_name.configuration_name
    );

    callback_name:= format(callback_name
        ,   '_redef_callback_' || md5(configuration_row.configuration_name)
        );
    call pgpro_redefinition._debug(
        errcode         => 'DFM06'
    ,   message         => format('Name of callback function: %1$s'
                            ,   callback_name
                           )
    );
    return callback_name;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_callback_body(
    configuration_name                  text
) returns text as
$body$
declare
    configuration_row       pgpro_redefinition.redef_table;
    cast_array              text[];
    cast_dest_old           text;
    cast_dest_new           text;
    proc                    text default $proc$$redef$
declare
begin
    %1$s

    %2$s
end;
$redef$$proc$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_callback_body.configuration_name
    );

    cast_array := array(
        select format ('dest_old.%1$I := source_old.%1$I;', c.column_name)
          from information_schema.columns c
         where c.table_schema = configuration_row.source_schema_name
           and c.table_name = configuration_row.source_table_name
        order by ordinal_position
    );
    cast_dest_old:= array_to_string(cast_array,  E'\n    ');

    cast_array := array(
        select format ('dest_new.%1$I := source_new.%1$I;', c.column_name)
          from information_schema.columns c
         where c.table_schema = configuration_row.source_schema_name
           and c.table_name = configuration_row.source_table_name
        order by ordinal_position
    );
    cast_dest_new:= array_to_string(cast_array,  E'\n    ');

    proc:= format(
        proc
    ,   cast_dest_old
    ,   cast_dest_new
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM08'
    ,   message         => format('Body of callback function: %1$s'
                            ,   proc
                           )
    );
    return proc;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_callback_function(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row       pgpro_redefinition.redef_table;
    proc                    text default $proc$create or replace function %1$I.%2$I (
    source_old        %3$I.%4$I
,   source_new        %3$I.%4$I
,   dest_old out      %5$I.%6$I
,   dest_new out      %5$I.%6$I
) as
%7$s
language plpgsql;
$proc$;
    proc_body       text;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_callback_function.configuration_name
    );

    proc_body:= pgpro_redefinition._generate_callback_body(configuration_name => _generate_callback_function.configuration_name);

    proc := format(
        proc
    ,   configuration_row.callback_schema_name
    ,   configuration_row.callback_name
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   configuration_row.dest_schema_name
    ,   configuration_row.dest_table_name
    ,   proc_body
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM09'
    ,   message         => format('Callback function: %1$s'
                            ,   proc
                            )
    );
    return proc;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_trigger_func_name(
    configuration_name                  varchar(63)
) returns varchar(63) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    trigger_name                varchar(63) default '%1$I';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_trigger_func_name.configuration_name
    );

    trigger_name:= format(
            trigger_name
        ,   '_redef_trigger_func_' || md5(configuration_row.configuration_name)
        );
    call pgpro_redefinition._debug(
        errcode         => 'DFM10'
    ,   message         => format('Name of trigger function: %1$s'
                            ,   trigger_name
                            )
    );
    return trigger_name;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_trigger_func_body_copy(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    update_cols_array       text[];
    dest_update_cols            text;
    dest_where_pkey             text;
    trigger_func_body           text default $func$$redef$
declare
    dest_new    %1$I.%2$I;
    dest_old    %1$I.%2$I;
    rec         record;
BEGIN
    select * into rec
    from %3$I.%4$I  (
        source_old => old,
        source_new => new
    ) c;

    dest_new := rec.dest_new;
    dest_old := rec.dest_old;

    if (tg_op = 'INSERT') then
        insert into %1$I.%2$I values (dest_new.*);
    elsif (tg_op = 'UPDATE') then
        update %1$I.%2$I
           set %5$s
         where %6$s;
    elsif  (tg_op = 'DELETE') then
         delete from %1$I.%2$I
          where %6$s;
    end if;
    return null;
END;
$redef$$func$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_trigger_func_body_copy.configuration_name
    );

    update_cols_array := array(
        select format ('%1$I = dest_new.%1$I', c.column_name)
          from information_schema.columns c
         where c.table_schema = configuration_row.dest_schema_name
           and c.table_name = configuration_row.dest_table_name
        order by ordinal_position
    );
    dest_update_cols := array_to_string(update_cols_array, E'\n            ,  ');

    dest_where_pkey := (
            select format ('( %1$s ) ', string_agg(q.cond, ' and '))
            from (
                select format('%1$I.%2$I = dest_old.%2$I', configuration_row.dest_table_name, rec.name,rec.name   ) as cond
                from pgpro_redefinition.redef_table rt
                ,   jsonb_to_recordset(rt.dest_pkey) as rec( pos int, name text, type text)
                where rt.configuration_name = configuration_row.configuration_name
                order by rec.pos
                ) q
        );

    trigger_func_body := format(
            trigger_func_body
        ,   configuration_row.dest_schema_name
        ,   configuration_row.dest_table_name
        ,   configuration_row.callback_schema_name
        ,   configuration_row.callback_name
        ,   dest_update_cols
        ,   dest_where_pkey
        );
    call pgpro_redefinition._debug(
        errcode         => 'DFM12'
    ,   message         => format('Body of trigger function: %1$s'
                            ,   trigger_func_body
                            )
    );
    return trigger_func_body;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_trigger_func_body_any(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    trigger_func_body           text default $func$$redef$
declare

begin
    perform * from %1$I.%2$I  (
        source_old => old,
        source_new => new
    ) ;
    return null;
end;
$redef$$func$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_trigger_func_body_any.configuration_name
    );

    trigger_func_body := format(
            trigger_func_body
        ,   configuration_row.callback_schema_name
        ,   configuration_row.callback_name
        );
    call pgpro_redefinition._debug(
        errcode         => 'DFM12'
    ,   message         => format('Body of trigger function: %1$s'
                            ,   trigger_func_body
                            )
    );
    return trigger_func_body;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_trigger_func(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row       pgpro_redefinition.redef_table;
    body           text;
    trigger_func        text default $trigger$create or replace function %1$I.%2$I() returns trigger
as %3$s
language plpgsql$trigger$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_trigger_func.configuration_name
    );
    if configuration_row.kind = pgpro_redefinition._kind_redef() then
        body = pgpro_redefinition._generate_trigger_func_body_copy(configuration_name => _generate_trigger_func.configuration_name);
    elsif configuration_row.kind = pgpro_redefinition._kind_any() then
        body = pgpro_redefinition._generate_trigger_func_body_any(configuration_name => _generate_trigger_func.configuration_name);
    end if;
    trigger_func:= format(
        trigger_func
    ,   configuration_row.source_trigger_schema_function_name
    ,   configuration_row.source_trigger_function_name
    ,   body
    );
    call pgpro_redefinition._info(
        errcode    => 'DFM14'
    ,   message => format('Trigger function %1$I.%2$I will be used to migrate data.'
                            ,   configuration_row.source_trigger_schema_function_name
                            ,   configuration_row.source_trigger_function_name
                        )
    );

    call pgpro_redefinition._debug(
        errcode         => 'DFM13'
    ,   message         => format('Text of trigger function: %1$s'
                            ,   trigger_func
                            )
    );
    return trigger_func;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_trigger_name(
    configuration_name                  varchar(63)
) returns varchar(63) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    trigger_name                varchar(63) default '_redef_trigger_%1$s_iud_tr';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_trigger_name.configuration_name
    );

    trigger_name:= format(
            trigger_name
        ,   md5(configuration_row.configuration_name)
        );
    call pgpro_redefinition._debug(
        errcode         => 'DFM14'
    ,   message         => format('Name of trigger: %1$s'
                            ,   trigger_name
                            )
    );
    return trigger_name;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_trigger(
    configuration_name                  varchar(63)
,   drop_trigger_if_exists              boolean default true
,   disable_trigger                     boolean default true
) returns text as
$body$
declare
    sql                     text default '';
    configuration_row       pgpro_redefinition.redef_table;
    drop_trigger            text default '';
    disable_trg             text default '';
    trigger                 text default $redef$%1$s
create trigger %2$I
    after insert or update or delete on %3$I.%4$I
    for each row execute function %5$I.%6$I();
$redef$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_trigger.configuration_name
    );

    if drop_trigger_if_exists then
        drop_trigger := format(
            'drop trigger if exists %1$I on %2$I.%3$I;'
        ,   configuration_row.source_trigger_name
        ,   configuration_row.source_schema_name
        ,   configuration_row.source_table_name
        );
        sql := sql || E'\n' || drop_trigger;
    end if;
    trigger:= format(
            trigger
        ,   drop_trigger
        ,   configuration_row.source_trigger_name
        ,   configuration_row.source_schema_name
        ,   configuration_row.source_table_name
        ,   configuration_row.source_trigger_schema_function_name
        ,   configuration_row.source_trigger_function_name
        );
        sql := sql || E'\n' || trigger;

    if disable_trigger then
        disable_trg := format(
                'alter table %1$I.%2$I disable trigger %3$I;'
            ,   configuration_row.source_schema_name
            ,   configuration_row.source_table_name
            ,   configuration_row.source_trigger_name
            );
            sql := sql || E'\n' || disable_trg;
    end if;
    call pgpro_redefinition._info(
        errcode                                 => 'DFM15'
    ,   message => format('Trigger %1$s on %2I.%3$I will be used to migrate data sql: %4$s'
                    ,   configuration_row.source_trigger_name
                    ,   configuration_row.source_schema_name
                    ,   configuration_row.source_table_name
                    ,   sql
                    )

    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM15'
    ,   message         => format('Text of trigger: %1$s'
                            ,   sql
                            )
    );
    return sql;
end;
$body$ language plpgsql ;

create or replace procedure pgpro_redefinition._enable_source_trigger(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    enable_source_trigger               text default 'alter table %1$I.%2$I enable trigger %3$I;';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _enable_source_trigger.configuration_name
    );
    enable_source_trigger := format(
        enable_source_trigger
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   configuration_row.source_trigger_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM16'
    ,   message         => format('Text to enable trigger: %1$s'
                            ,   enable_source_trigger
                            )
    );
    call pgpro_redefinition._execute_sql (sql => enable_source_trigger);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._start_capture_online(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _start_capture_online.configuration_name
    );
    if configuration_row.captured then
        call pgpro_redefinition._error(
            errcode         => 'DFE31'
        ,   message         => format('Capture data already started')
        );
    end if;

    call pgpro_redefinition._enable_source_trigger(
        configuration_name => _start_capture_online.configuration_name
    );

    update pgpro_redefinition.redef_table
       set captured = true
       where redef_table.configuration_name = _start_capture_online.configuration_name;

    call pgpro_redefinition._debug(
        errcode         => 'DFM52'
    ,   message         => format('Capture data started')
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._disable_source_trigger(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    disable_source_trigger               text default 'alter table %1$I.%2$I disable trigger %3$I;';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _disable_source_trigger.configuration_name
    );
    disable_source_trigger := format(
        disable_source_trigger
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   configuration_row.source_trigger_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM17'
    ,   message         => format('Text to disable trigger: %1$s'
                            ,   disable_source_trigger
                            )
    );
    call pgpro_redefinition._execute_sql (sql => disable_source_trigger);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._drop_source_trigger(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    drop_source_trigger                 text default 'drop trigger %1$I on %2$I.%3$I;';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _drop_source_trigger.configuration_name
    );
    drop_source_trigger := format(
        drop_source_trigger
    ,   configuration_row.source_trigger_name
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM18'
    ,   message         => format('Text to drop trigger: %1$s'
                            ,   drop_source_trigger
                            )
    );
    call pgpro_redefinition._execute_sql (sql => drop_source_trigger);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._insert_first_row(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    source_pkey                 text;
    source_pkey_asc             text;
    source_pkey_desc            text;
    insert                      text default $do$with smallest_row as (
    select *
    from %1$I.%2$I
    order by %3$s
    limit 1
    for update
)
, highest_row as (
    select *
    from %1$I.%2$I
    order by %4$s
    limit 1
    for update
)
, insert_last_pk as (
    insert into pgpro_redefinition.redef_last_pkey (configuration_name, pkey)
    select %6$L
        , to_jsonb(smallest_row) as values
          from (
            select %5$s
              from smallest_row
         ) as smallest_row
)
, insert_highest_pk as (
    insert into pgpro_redefinition.redef_highest_pkey (configuration_name, pkey)
    select %6$L
        , to_jsonb(highest_row) as values
          from (
            select %5$s
              from highest_row
         ) as highest_row
)
insert into %7$I.%8$I
select (callback.dest_new).*
  from smallest_row
  ,    %9$s.%10$s(
          source_new => row(smallest_row.*)::%1$I.%2$I
       ,  source_old => null::%1$I.%2$I
       ) callback
on conflict do nothing
 ;
$do$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _insert_first_row.configuration_name
    );
    select string_agg(format('%1$I asc', name) , ', ')
         , string_agg(format('%1$I desc', name) , ', ')
         , string_agg(format('%1$I ', name) , ', ')
      into source_pkey_asc
         , source_pkey_desc
         , source_pkey
        from (
            select rec.name
              from jsonb_to_recordset(configuration_row.source_pkey) as rec( pos int, name text, type text)
            order by rec.pos
            ) q;

    "insert" := format(
        insert
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   source_pkey_asc
    ,   source_pkey_desc
    ,   source_pkey
    ,   configuration_row.configuration_name
    ,   configuration_row.dest_schema_name
    ,   configuration_row.dest_table_name
    ,   configuration_row.callback_schema_name
    ,   configuration_row.callback_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM18'
    ,   message         => format('Text to insert first row: %1$s'
                            ,   insert
                            )
    );
    call pgpro_redefinition._execute_sql (sql => insert);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._select_first_row(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    source_pkey                 text;
    source_pkey_asc             text;
    source_pkey_desc            text;
    query                       text default $do$with smallest_row as (
    select *
    from %1$I.%2$I
    order by %3$s
    limit 1
    for update
)
, highest_row as (
    select *
    from %1$I.%2$I
    order by %4$s
    limit 1
    for update
)
, insert_last_pk as (
    insert into pgpro_redefinition.redef_last_pkey (configuration_name, pkey)
    select %6$L
        , to_jsonb(smallest_row) as values
          from (
            select %5$s
              from smallest_row
         ) as smallest_row
)
, insert_highest_pk as (
    insert into pgpro_redefinition.redef_highest_pkey (configuration_name, pkey)
    select %6$L
        , to_jsonb(highest_row) as values
          from (
            select %5$s
              from highest_row
         ) as highest_row
)
select 1
  from smallest_row
  ,    %7$s.%8$s(
          source_new => row(smallest_row.*)::%1$I.%2$I
       ,  source_old => null::%1$I.%2$I
       ) callback
;
$do$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _select_first_row.configuration_name
    );
    select string_agg(format('%1$I asc', name) , ', ')
         , string_agg(format('%1$I desc', name) , ', ')
         , string_agg(format('%1$I ', name) , ', ')
      into source_pkey_asc
         , source_pkey_desc
         , source_pkey
    from (
        select rec.name
          from jsonb_to_recordset(configuration_row.source_pkey) as rec( pos int, name text, type text)
        order by rec.pos
        ) q;

    query := format(
        query
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   source_pkey_asc
    ,   source_pkey_desc
    ,   source_pkey
    ,   configuration_row.configuration_name
    ,   configuration_row.callback_schema_name
    ,   configuration_row.callback_name
    );

    call pgpro_redefinition._debug(
        errcode         => 'DFM19'
    ,   message         => format('Text to select first row: %1$s'
                            ,   query
                            )
    );
    call pgpro_redefinition._execute_sql (sql => query);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._insert_first_row_to_mlog(
    configuration_name                  varchar(63)
) as
$body$

declare
    configuration_row           pgpro_redefinition.redef_table;
    source_pkey_asc             varchar(63);
    source_pkey_desc            varchar(63);
    source_pkey                 varchar(63);
    insert                      text default $do$with smallest_row as (
        select *
        from %1$I.%2$I
        order by %3$s
        limit 1
        for update
    )
, highest_row as (
    select *
    from %1$I.%2$I
    order by %4$s
    limit 1
    for update
)
, insert_last_pk as (
    insert into pgpro_redefinition.redef_last_pkey (configuration_name, pkey)
    select %6$L
        , to_jsonb(smallest_row) as values
          from (
            select %5$s
              from smallest_row
         ) as smallest_row
)
, insert_highest_pk as (
    insert into pgpro_redefinition.redef_highest_pkey (configuration_name, pkey)
    select %6$L
        , to_jsonb(highest_row) as values
          from (
            select %5$s
              from highest_row
         ) as highest_row
)
insert into pgpro_redefinition.mlog (
    config_id
,   table_oid
,   lsn
,   before
,   after
)
select %7$L
     , %8$s
     , pg_current_wal_lsn()
     , null
     , row_to_json(smallest_row.*)
from smallest_row
returning *
 ;
$do$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _insert_first_row_to_mlog.configuration_name
    );
    select string_agg(format('%1$I asc', name) , ', ')
         , string_agg(format('%1$I desc', name) , ', ')
         , string_agg(format('%1$I', name) , ', ')
      into source_pkey_asc
         , source_pkey_desc
         , source_pkey
    from (
        select rec.name
          from jsonb_to_recordset(configuration_row.source_pkey) as rec( pos int, name text, type text)
        order by rec.pos
        ) q;

    "insert" := format(
        insert
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   source_pkey_asc
    ,   source_pkey_desc
    ,   source_pkey
    ,   configuration_row.configuration_name
    ,   configuration_row.id
    ,   configuration_row.source_table_oid
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM18'
    ,   message         => format('Text to insert first row to mlog table: %1$s'
                            ,   insert
                            )
    );
    call pgpro_redefinition._execute_sql (sql => insert);
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_redef_proc_name(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row               pgpro_redefinition.redef_table;
    proc_name                       text default '_redef_online_%1$s';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_redef_proc_name.configuration_name
    );

    proc_name:= format(
        proc_name
    ,   md5(configuration_row.configuration_name)
    );

    call pgpro_redefinition._debug(
        errcode         => 'DFM20'
    ,   message         => format('Name of procedure: %1$s'
                            ,   proc_name
                            )
    );
    return proc_name;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_copy_proc_body_copy(
    configuration_name                  varchar(63)
,   commit_type                         pgpro_redefinition.commit_type default 'autonomous'
) returns text as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    pkey                        text default '%1$s';
    pkey_desc                   text default '%1$s';
    last_pkey_from_json         text default '%1$s';
    proc_autonomous             text default '';
    proc_commit                 text default '';
    proc_body                   text default $proc$$redef$
declare
    ts_start        timestamp;
begin %11$s
    ts_start := clock_timestamp();
    with data as (
        select *
        from %1$I.%2$I
        where (%3$s) > (
            select %5$s
              from pgpro_redefinition.redef_last_pkey p
             where configuration_name = %6$L
            )
          and (%3$s) <= (
            select %5$s
              from pgpro_redefinition.redef_highest_pkey p
             where configuration_name = %6$L
            )
        order by %3$s
        limit (select rows_redef from pgpro_redefinition.redef_table where configuration_name = %6$L)
        for update
    )
    ,
    upd_currenp_pk as (
        update pgpro_redefinition.redef_last_pkey
            set pkey = coalesce(
                (
                    select to_jsonb(data) as values
                    from (
                             select %3$s
                             from data
                             order by %4$s
                             limit 1
                         ) as data
                )
            ,   pkey
            )
            where configuration_name = %6$L
    ),
    insert_dest as (
        insert into %7$I.%8$I
            select (callback.dest_new).*
            from data
            ,    %9$I.%10$I(
                    source_new => row(data.*)::%1$I.%2$I
                ,   source_old => null::%1$I.%2$I
                ) callback
            on conflict do nothing
            returning %3$s
    )
    insert into pgpro_redefinition.inc_stat(
            configuration_name
        ,   job_type
        ,   dest_selected
        ,   dest_inserted
        ,   ts_start
        ,   ts_finish
        )
        select %6$L as configuration_name
             , pgpro_redefinition._job_type_redef_data()    as job_type
             , (select count(*) from data)                  as dest_selected
             , (select count(*) from insert_dest)           as dest_inserted
             , ts_start                                     as ts_start
             , clock_timestamp()                            as ts_finish
    ;%12$s
end;
$redef$$proc$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_copy_proc_body_copy.configuration_name
    );

    select string_agg(format('%1$s desc', r.name), ', ')
         , string_agg(format('%1$s', r.name), ', ')
      into
           pkey_desc
         , pkey
          from (
                select rec.name
                from jsonb_to_recordset(configuration_row.source_pkey) as rec( pos int, name text, type text)
                order by rec.pos
          ) r;

    last_pkey_from_json := (
        select string_agg(format('(p.pkey->>%1$L)::%2$s as %1$I', r.name, r.type), E'\n                 , ')
          from (
                select rec.name, rec.type
                from jsonb_to_recordset(configuration_row.source_pkey) as rec(pos int, name text, type text)
                order by rec.pos
          ) r
    );
    if _generate_copy_proc_body_copy.commit_type = 'autonomous' then
        proc_autonomous = 'autonomous';
    elsif _generate_copy_proc_body_copy.commit_type = 'autonomous' then
        proc_commit = E'\n    commit;';
    end if;

    proc_body := format(
        proc_body
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   pkey
    ,   pkey_desc
    ,   last_pkey_from_json
    ,   configuration_row.configuration_name
    ,   configuration_row.dest_schema_name
    ,   configuration_row.dest_table_name
    ,   configuration_row.callback_schema_name
    ,   configuration_row.callback_name
    ,   proc_autonomous
    ,   proc_commit
    ,   configuration_row.kind
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM21'
    ,   message         => format('Body of procedure: %1$s'
                            ,   proc_body
                            )
    );
    return proc_body;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_copy_proc_body_any(
    configuration_name                  varchar(63)
,   commit_type                         pgpro_redefinition.commit_type default 'autonomous'
) returns text as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    pkey                        text default '%1$s';
    last_pkey_from_json         text default '%1$s';
    proc_autonomous             text default '';
    proc_commit                 text default '';
    proc_body                   text default $proc$$redef$
declare
    ts_start        timestamp;
begin %10$s
    ts_start := clock_timestamp();
    with data as (
        select *
        from %1$I.%2$I
        where (%3$s) > (
            select %4$s
              from pgpro_redefinition.redef_last_pkey p
             where configuration_name = %5$L
            )
          and (%3$s) <= (
            select %4$s
              from pgpro_redefinition.redef_highest_pkey p
             where configuration_name = %5$L
            )
        order by %3$s
        limit (select rows_redef from pgpro_redefinition.redef_table where configuration_name = %5$L)
        for update
    )
    ,
    upd_currenp_pk as (
        update pgpro_redefinition.redef_last_pkey
            set pkey = coalesce(
                (
                    select to_jsonb(data) as values
                    from (
                             select %3$s
                             from data
                             order by (%3$s) desc
                             limit 1
                         ) as data
                )
            ,   pkey
            )
            where configuration_name = %5$L
    ),
    select_callback_for_data as (
            select 1
            from data
            ,    %8$I.%9$I(
                    source_new => row(data.*)::%1$I.%2$I
                ,   source_old => null::%1$I.%2$I
                ) callback
    )
    insert into pgpro_redefinition.inc_stat(
            configuration_name
        ,   job_type
        ,   dest_selected
        ,   dest_inserted
        ,   ts_start
        ,   ts_finish
        )
        select %5$L as configuration_name
             , pgpro_redefinition._job_type_redef_data()       as job_type
             , (select count(*) from data)                      as dest_selected
             , (select count(*) from select_callback_for_data)  as dest_inserted
             , ts_start                                         as ts_start
             , clock_timestamp()                                as ts_finish
    ;%11$s
end;
$redef$$proc$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_copy_proc_body_any.configuration_name
    );

    pkey := (
        select string_agg(format('%1$s', r.name), ', ')
              from (
                    select rec.name
                    from jsonb_to_recordset(configuration_row.source_pkey) as rec( pos int, name text, type text)
                    order by rec.pos
              ) r
        );

    last_pkey_from_json := (
        select string_agg(format('(p.pkey->>%1$L)::%2$s as %1$I', r.name, r.type), E'\n                 , ')
          from (
                select rec.name, rec.type
                from jsonb_to_recordset(configuration_row.source_pkey) as rec(pos int, name text, type text)
                order by rec.pos
          ) r
    );
    if _generate_copy_proc_body_any.commit_type = 'autonomous' then
        proc_autonomous = 'autonomous';
    elsif _generate_copy_proc_body_any.commit_type = 'autonomous' then
        proc_commit = E'\n    commit;';
    end if;

    proc_body := format(
        proc_body
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   pkey
    ,   last_pkey_from_json
    ,   configuration_row.configuration_name
    ,   configuration_row.dest_schema_name
    ,   configuration_row.dest_table_name
    ,   configuration_row.callback_schema_name
    ,   configuration_row.callback_name
    ,   proc_autonomous
    ,   proc_commit
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM21'
    ,   message         => format('Body of procedure: %1$s'
                            ,   proc_body
                            )
    );
    return proc_body;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_copy_proc(
    configuration_name                  varchar(63)
,   commit_type                         pgpro_redefinition.commit_type default 'autonomous'
) returns text as
$body$
declare
    configuration_row       pgpro_redefinition.redef_table;
    proc_body               text;
    proc                    text default $proc$create or replace procedure %1$I.%2$I(
)as
%3$s
language plpgsql$proc$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_copy_proc.configuration_name
    );

    if configuration_row.kind = pgpro_redefinition._kind_redef() then
        proc_body := pgpro_redefinition._generate_copy_proc_body_copy(
            configuration_name  => _generate_copy_proc.configuration_name
        ,   commit_type         => _generate_copy_proc.commit_type
        );
    elsif configuration_row.kind = pgpro_redefinition._kind_any() then
        proc_body := pgpro_redefinition._generate_copy_proc_body_any(
            configuration_name  => _generate_copy_proc.configuration_name
        ,   commit_type         => _generate_copy_proc.commit_type
        );
    end if;

    proc:= format(
        proc
    ,   configuration_row.redef_proc_schema_name
    ,   configuration_row.redef_proc_name
    ,   proc_body
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM22'
    ,   message         => format('Text of procedure: %1$s'
                            ,   proc
                            )
    );
    call pgpro_redefinition._info(
        errcode                 => 'DFM23'
    ,   message                 => format('Redef proc %1$I.%2$I will be used to work with data.'
                                        ,   configuration_row.redef_proc_schema_name
                                        ,   configuration_row.redef_proc_name
                                    )
    );
    return proc;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_redef_loop_proc_name(
    configuration_name                  text
) returns text as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    proc_name                   text default '_redef_loop_%1$s';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_redef_loop_proc_name.configuration_name
    );

    proc_name:= format(
            proc_name
        ,   md5(configuration_row.configuration_name)
        );
    call pgpro_redefinition._debug(
        errcode                 => 'DFM24'
    ,   message                 => format('Name of procedure: %1$s '
                                        ,   proc_name
                                    )
    );
    return proc_name;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_copy_loop_proc(
    configuration_name                  text
) returns text as
$body$
declare
    configuration_row       pgpro_redefinition.redef_table;
    proc                    text default $proc$create or replace procedure %1$I.%2$I(
)as
%3$s
language plpgsql$proc$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_copy_loop_proc.configuration_name
    );

    proc:= format(
        proc
    ,   configuration_row.redef_loop_proc_schema_name
    ,   configuration_row.redef_loop_proc_name
    ,   pgpro_redefinition._generate_copy_loop_proc_body(
            configuration_name  => _generate_copy_loop_proc.configuration_name
        )
    );
    call pgpro_redefinition._debug(
        errcode                 => 'DFM26'
    ,   message                 => format('Name of procedure: %1$s '
                                        ,   proc
                                    )
    );
    return proc;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_copy_loop_proc_body(
    configuration_name                  text
) returns text as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    prepared_statement_name     text;
    proc_body     text default $proc$$redef$
declare
    configuration_row                           pgpro_redefinition.redef_table;
    n                                           integer;
    local_returned_sqlstate                     text;
    local_column_name                           text;
    local_constraint_name                       text;
    local_pg_datatype_name                      text;
    local_message_text                          text;
    local_table_name                            text;
    local_schema_name                           text;
    local_pg_exception_detail                   text;
    local_pg_exception_hint                     text;
    local_pg_exception_context                  text;
    local_pg_context                            text;
    log_id                                      bigint;
    is_err                                      boolean;
    current_query                               text;
    raise_exception                             boolean default false;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => %1$L
    );
    if exists(select 1 from pg_prepared_statements where name = %4$L) then
        execute 'deallocate %4$I' ;
    end if;

    n := configuration_row."loop_redef";
    for i in 1 .. n loop
        select * into configuration_row
        from pgpro_redefinition._get_configuration(
            configuration_name  => %1$L
        );
        begin
            call %2$s.%3$s();
        exception
            when others then
                get stacked diagnostics
                    local_returned_sqlstate = returned_sqlstate,
                    local_column_name = column_name,
                    local_constraint_name = constraint_name,
                    local_pg_datatype_name = pg_datatype_name,
                    local_message_text = message_text,
                    local_table_name = table_name,
                    local_schema_name = schema_name,
                    local_pg_exception_detail = pg_exception_detail,
                    local_pg_exception_hint = pg_exception_hint;
                --get diagnostics local_pg_context = pg_context;
                is_err = true;
                current_query := current_query();

                log_id := pgpro_redefinition._log(
                    loglevel                => pgpro_redefinition._loglevel_exception()
                ,   errcode                 => 'DFE79'::varchar(5)
                ,   message                 => 'redef_proc'::text
                ,   hint                    => current_query::text
                ,   configuration_name      => %1$L
                ,   sql                     => 'call %2$s.%3$s()'
                );

                call pgpro_redefinition._stacked_diagnostics(
                    log_id => log_id
                ,   returned_sqlstate => local_returned_sqlstate
                ,   column_name => local_column_name
                ,   constraint_name => local_constraint_name
                ,   pg_datatype_name => local_pg_datatype_name
                ,   message_text => local_message_text
                ,   table_name => local_table_name
                ,   schema_name => local_schema_name
                ,   pg_exception_detail => local_pg_exception_detail
                ,   pg_exception_hint => local_pg_exception_hint
                ,   pg_exception_context => local_pg_context
                );
                raise_exception := pgpro_redefinition._default_on_error_stop();
                if raise_exception then
                    raise exception using
                        message     = local_message_text,
                        detail      = local_message_text,
                        hint        = local_pg_exception_hint,
                        errcode     = local_returned_sqlstate,
                        column      = local_column_name,
                        constraint  = local_constraint_name,
                        datatype    = local_pg_datatype_name,
                        table       = local_table_name,
                        schema      = local_schema_name;
                end if;
        end;

        perform pg_sleep(configuration_row.sleep_redef::numeric / 1000);
        if configuration_row.schedule_jobs_copy_id is not null then
            if (    select s.canceled is true
                      from schedule.all_job_status s
                     where s.id = configuration_row.schedule_jobs_copy_id[1]
                  order by s.id desc, attempt desc
                 limit (1)
                ) then
                    return;
                end if;
        end if;
    end loop;
    if exists(select 1 from pg_prepared_statements where name = %4$L) then
        execute 'deallocate %4$I' ;
    end if;
end;
$redef$$proc$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_copy_loop_proc_body.configuration_name
    );
    prepared_statement_name := format(
        pgpro_redefinition._prepared_statement_name_template()
    ,   md5(configuration_row.configuration_name)::text
    );
    proc_body:= format(
        proc_body
    ,   configuration_row.configuration_name
    ,   configuration_row.redef_proc_schema_name
    ,   configuration_row.redef_proc_name
    ,   prepared_statement_name
    );
    call pgpro_redefinition._debug(
        errcode                 => 'DFM27'
    ,   message                 => format('Body of procedure: %1$s '
                                        ,   proc_body
                                    )
    );
    return proc_body;
end;
$body$ language plpgsql ;

create or replace procedure pgpro_redefinition._drop_dest_table(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    drop_dest_table             text default 'drop table %1$I.%2$I;';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _drop_dest_table.configuration_name
    );
    drop_dest_table:= format(
        drop_dest_table
    ,   configuration_row.dest_schema_name
    ,   configuration_row.dest_table_name
    );
    call pgpro_redefinition._info(
        errcode                 => 'DFM28'
    ,   message                 => format('Table %1$I.%2$I will be deleted.'
                                    ,   configuration_row.dest_schema_name
                                    ,   configuration_row.dest_table_name
                                   )
    );
    call pgpro_redefinition._debug(
        errcode                 => 'DFM29'
    ,   message                 => format('Text of drop table: %1$s '
                                        ,   drop_dest_table
                                    )
    );
    call pgpro_redefinition._execute_sql (sql => drop_dest_table);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._replace_dest_to_source_tables(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                       pgpro_redefinition.redef_table;
    source_backup_table_name            text;
    source_alter_schema                 text default 'alter table %1$I.%2$I set schema %3$I';
    source_rename_table                 text default 'alter table %1$I.%2$I rename to %3$I';
    dest_alter_schema                   text default 'alter table %1$I.%2$I set schema %3$I';
    dest_rename_table                   text default 'alter table %1$I.%2$I rename to %3$I';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _replace_dest_to_source_tables.configuration_name
    );
    source_backup_table_name := 'backup__' || configuration_row.source_schema_name || '__' || configuration_row.source_table_name;
    source_rename_table:= format(
        source_rename_table
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   source_backup_table_name
    );
    call pgpro_redefinition._debug(
        errcode                 => 'DFM30'
    ,   message                 => format('Text to rename table: %1$s '
                                        ,   source_rename_table
                                    )
    );
    call pgpro_redefinition._execute_sql (sql => source_rename_table);

    source_alter_schema := format(
        source_alter_schema
    ,   configuration_row.source_schema_name
    ,   source_backup_table_name
    ,   pgpro_redefinition._default_schema()
    );
    call pgpro_redefinition._debug(
        errcode                 => 'DFM31'
    ,   message                 => format('Text to alter: %1$s '
                                        ,   source_alter_schema
                                    )
    );
    call pgpro_redefinition._execute_sql (sql => source_alter_schema);
    ------------------------------------
    dest_rename_table := format(
        dest_rename_table
    ,   configuration_row.dest_schema_name
    ,   configuration_row.dest_table_name
    ,   configuration_row.source_table_name
    );
    call pgpro_redefinition._debug(
        errcode                 => 'DFM32'
    ,   message                 => format('Text to rename table: %1$s '
                                        ,   dest_rename_table
                                    )
    );
    call pgpro_redefinition._execute_sql (sql => dest_rename_table);

    dest_alter_schema := format(
        dest_alter_schema
    ,   configuration_row.dest_schema_name
    ,   configuration_row.source_table_name
    ,   configuration_row.source_schema_name
    );
    call pgpro_redefinition._debug(
        errcode                 => 'DFM33'
    ,   message                 => format('Text to alter: %1$s '
                                        ,   dest_alter_schema
                                    )
    );
    call pgpro_redefinition._execute_sql (sql => dest_alter_schema);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._move_configuration_to_archive(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    result                      boolean default false;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _move_configuration_to_archive.configuration_name
    );
    with del_row as (
        delete from pgpro_redefinition.redef_table
        where redef_table.configuration_name = _move_configuration_to_archive.configuration_name
        returning *
    ),
    del_redef_last_pkey as (
        delete from pgpro_redefinition.redef_last_pkey
        where redef_last_pkey.configuration_name = _move_configuration_to_archive.configuration_name
        returning *
    ),
    del_redef_highest_pkey as (
        delete from pgpro_redefinition.redef_highest_pkey
        where redef_highest_pkey.configuration_name = _move_configuration_to_archive.configuration_name
        returning *
    ),
    del_inc_stat as (
        delete from pgpro_redefinition.inc_stat
        where inc_stat.configuration_name = _move_configuration_to_archive.configuration_name
        returning *
    ),
    del_mlog_last_applied_pkey as (
        delete from pgpro_redefinition.mlog_last_applied_pkey
        where mlog_last_applied_pkey.config_id = configuration_row.id
        returning *
    ),
    del_sub_part_mlog_desc as (
        delete from pgpro_redefinition.sub_part_mlog_desc
        where sub_part_mlog_desc.config_id = configuration_row.id
        returning *
    ),
    insert_row_to_archive as (
        insert into pgpro_redefinition.redef_table_archive
        select * from del_row
    ),
    insert_pkey_row_to_archive as (
        insert into pgpro_redefinition.redef_last_pkey_archive
        select * from del_redef_last_pkey
    ),
    insert_highest_pkey_row_to_archive as (
        insert into pgpro_redefinition.redef_highest_pkey_archive
        select * from del_redef_highest_pkey
    ),
    insert_inc_stat_to_archive as (
        insert into pgpro_redefinition.inc_stat_archive
        select * from del_inc_stat
    ),
    insert_mlog_last_applied_pkey_archive as (
        insert into pgpro_redefinition.mlog_last_applied_pkey_archive
        select * from del_mlog_last_applied_pkey
    ),
    insert_sub_part_mlog_desc_archive as (
        insert into pgpro_redefinition.sub_part_mlog_desc_archive
        select * from del_sub_part_mlog_desc
    )
    select true into result;


    call pgpro_redefinition._info(
        errcode                 => 'DFM34'
    ,   message                 => format('Configuration %1$s moved to archive: %1$s '
                                        ,   configuration_row.configuration_name
                                    )
    );

end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._create_partition(
    main_table_name                varchar(63)
,   main_schema_name               varchar(63)
,   num_part                       bigint
,   prefix                         varchar(4) default 'part'
) returns varchar(63) as
$body$
declare
    create_part                 text default 'create table %1$I.%2$I partition of %3$I.%4$I for values in (%5$s) partition by range (lsn)';
    part_table_name             varchar(63) default '%1$s_%2$s_%3$s';
    default_part_table_name     varchar(63) default '%1$s_default';
    create_default_part         text default 'create table %1$I.%2$I partition of %3$I.%4$I default';
begin
    part_table_name := format(part_table_name, main_table_name, prefix, to_char(num_part, 'FM00000'));
    create_part := format(
        create_part
    ,   main_schema_name
    ,   part_table_name
    ,   main_schema_name
    ,   main_table_name
    ,   num_part::text
    );
    call pgpro_redefinition._info(
        errcode     => 'DFM77'
    ,   message     => create_part
    );
    call pgpro_redefinition._execute_sql (sql => create_part);

    default_part_table_name := format(
        default_part_table_name
    ,   part_table_name
    );
    create_default_part := format(
        create_default_part
    ,   main_schema_name
    ,   default_part_table_name
    ,   main_schema_name
    ,   part_table_name
    );
    call pgpro_redefinition._info(
        errcode     => 'DFM77'
    ,   message     => create_default_part
    );
    call pgpro_redefinition._execute_sql (sql => create_default_part);
    return part_table_name;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._create_sub_partition(
    main_table_name                varchar(63)
,   main_schema_name               varchar(63)
,   lsn_from                       pg_lsn
,   lsn_to                         pg_lsn
,   num_sub_part                   integer
,   prefix                         varchar(4) default 'lsn'
) returns varchar(63) as
$body$
declare
    create_table_like       text default 'create table %1$I.%2$I ( like %3$I.%4$I including all )';
    move_data               text default 'with delete as (delete
from %1$I.%2$I_default d
where d.lsn BETWEEN %3$L::pg_lsn and %4$L::pg_lsn
returning *
)
insert into %5$I.%6$I select * from delete';
    attach                  text default 'alter table %1$I.%2$I attach partition %3$I.%4$I
    for values from (%5$L::pg_lsn) to (%6$L::pg_lsn)';
    create_sub_part         text default 'create table %1$I.%2$I
    partition of %3$I.%4$I for values from (%5$L::pg_lsn) to (%6$L::pg_lsn);';

    sub_part_table_name     varchar(63) default '%1$s_%2$s_%3$s_%4$s_%5$s';
    lsn_from_text           varchar(19);
    lsn_to_text             varchar(19);
begin
    lsn_from_text := replace(lsn_from::text,'/','_');
    lsn_to_text := replace(lsn_to::text,'/','_');

    sub_part_table_name := format(
        sub_part_table_name
    ,   main_table_name
    ,   to_char(num_sub_part, 'FM00000')
    ,   prefix
    ,   lsn_from_text
    ,   lsn_to_text
    );
    create_table_like := format(
        create_table_like
    ,   main_schema_name
    ,   sub_part_table_name
    ,   main_schema_name
    ,   main_table_name
    );
    call pgpro_redefinition._info(
        errcode     => 'DFM77'
    ,   message     => create_table_like
    );
    call pgpro_redefinition._execute_sql (sql => create_table_like);
    move_data := format(
        move_data
    ,   main_schema_name
    ,   main_table_name
    ,   lsn_from::Text
    ,   lsn_to::Text
    ,   main_schema_name
    ,   sub_part_table_name
    );
    call pgpro_redefinition._info(
        errcode     => 'DFM77'
    ,   message     => move_data
    );
    call pgpro_redefinition._execute_sql (sql => move_data);

    attach := format(
        attach
    ,   main_schema_name
    ,   main_table_name
    ,   main_schema_name
    ,   sub_part_table_name
    ,   lsn_from::text
    ,   lsn_to::text
    );
    call pgpro_redefinition._info(
        errcode     => 'DFM77'
    ,   message     => attach
    );
    call pgpro_redefinition._execute_sql (sql => attach);


/*    create_sub_part := format(
        create_sub_part
    ,   main_schema_name
    ,   sub_part_table_name
    ,   main_schema_name
    ,   main_table_name
    ,   lsn_from
    ,   lsn_to
    );
    call pgpro_redefinition._info(
        errcode     => 'DFM77'
    ,   message     => create_sub_part
    );
    call pgpro_redefinition._execute_sql (sql => create_sub_part);*/

    return sub_part_table_name;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._drop_mlog_partition_by_configuration_name(
    configuration_name                varchar(63)
) as
$body$
declare
    detach_part               text default 'alter table %1$I.%2$I  detach partition %3$I.%4$I;';
    drop_part                 text default 'drop table %1$I.%2$I';

    configuration_row       pgpro_redefinition.redef_table;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _drop_mlog_partition_by_configuration_name.configuration_name
    );
    if configuration_row.type = pgpro_redefinition._type_deferred() then
        detach_part := format(
            detach_part
        ,   pgpro_redefinition._default_schema()
        ,   'mlog'
        ,   pgpro_redefinition._default_schema()
        ,   configuration_row.mlog_part_table_name
        );
        call pgpro_redefinition._execute_sql(detach_part);

        drop_part := format(
            drop_part
        ,   pgpro_redefinition._default_schema()
        ,   configuration_row.mlog_part_table_name
        );
        call pgpro_redefinition._execute_sql(drop_part);
    end if;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._check_sub_partitions(
    configuration_name             varchar(63)
) as
$body$
declare
    configuration_row               pgpro_redefinition.redef_table;
    last_sub_part_mlog_desc         pgpro_redefinition.sub_part_mlog_desc;
    current_lsn                     pg_lsn;
    num_sub_part                    integer DEFAULT 0;
    lsn_from                        pg_lsn;
    lsn_to                          pg_lsn;
    sub_part_table_name             varchar(63);
begin -- autonomous
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _check_sub_partitions.configuration_name
    );
    call pgpro_redefinition._set_session_configuration_name(configuration_row.configuration_name);
    select * into last_sub_part_mlog_desc
      from pgpro_redefinition.sub_part_mlog_desc d
     where d.config_id = configuration_row.id
    order by num_sub_part desc
    limit 1;

    current_lsn := pg_current_wal_lsn();
    if last_sub_part_mlog_desc is null then
        last_sub_part_mlog_desc.lsn_to := current_lsn - pgpro_redefinition._default_lsn_step();
        last_sub_part_mlog_desc.num_sub_part := 0;
    end if;

    while (pgpro_redefinition._default_lsn_step() * pgpro_redefinition._default_lsn_partition_multiplier() * pgpro_redefinition._default_create_next_sub_part())
              >= (coalesce(last_sub_part_mlog_desc.lsn_to, '0/0'::pg_lsn) - current_lsn)
    loop
        num_sub_part :=last_sub_part_mlog_desc.num_sub_part + 1;

        lsn_from := last_sub_part_mlog_desc.lsn_to;
        lsn_to := lsn_from + pgpro_redefinition._default_lsn_step() * pgpro_redefinition._default_lsn_partition_multiplier();

        sub_part_table_name := pgpro_redefinition._create_sub_partition(
            main_table_name                => configuration_row.mlog_part_table_name
        ,   main_schema_name               => pgpro_redefinition._default_schema()::varchar(63)
        ,   lsn_from                       => lsn_from::pg_lsn
        ,   lsn_to                         => lsn_to::pg_lsn
        ,   num_sub_part                   => num_sub_part
        );
        call pgpro_redefinition._register_sub_partition(
            config_id               => configuration_row.id
        ,   sub_part_table_name     => sub_part_table_name
        ,   lsn_from                => lsn_from::pg_lsn
        ,   lsn_to                  => lsn_to::pg_lsn
        ,   num_sub_part            => num_sub_part
        );
        select * into last_sub_part_mlog_desc
          from pgpro_redefinition.sub_part_mlog_desc d
         where d.config_id = configuration_row.id
        order by num_sub_part desc
        limit 1;
    end loop;

end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._check_all_mlog_sub_part(
) as
$body$
declare
    conf_name               text;
begin
    for conf_name in (
    select configuration_name
      from pgpro_redefinition.redef_table r
     where r.type = pgpro_redefinition._type_deferred()
    ) loop
            call pgpro_redefinition._check_sub_partitions(
                configuration_name  => conf_name
            );
    end loop;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._register_sub_partition(
    config_id               bigint
,   sub_part_table_name     varchar(63)
,   lsn_from                pg_lsn
,   lsn_to                  pg_lsn
,   num_sub_part            integer
) as
$body$
declare

begin
    insert into pgpro_redefinition.sub_part_mlog_desc (
        id
    ,   config_id
    ,   sub_part_table_name
    ,   lsn_from
    ,   lsn_to
    ,   num_sub_part
    ) values (
        nextval('pgpro_redefinition.sub_part_desc_id_seq')
    ,   _register_sub_partition.config_id
    ,   _register_sub_partition.sub_part_table_name
    ,   _register_sub_partition.lsn_from
    ,   _register_sub_partition.lsn_to
    ,   _register_sub_partition.num_sub_part
    );

end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.start_capture_data(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => start_capture_data.configuration_name
    );

    if configuration_row.type = pgpro_redefinition._type_online() then
        call pgpro_redefinition._start_capture_online(
            configuration_name => start_capture_data.configuration_name
        );
    elsif configuration_row.type = pgpro_redefinition._type_deferred() then
        call pgpro_redefinition._start_capture_deferred(
            configuration_name => start_capture_data.configuration_name
        );
    else
        call pgpro_redefinition._error(
            errcode     => 'DFE27'
        ,   message     => format ('type %s not suppport')
        );
    end if;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._service(
) as
$body$
declare
begin
        call pgpro_redefinition._check_all_mlog_sub_part();
        call pgpro_redefinition._remove_all_old_mlog_partitions();
        call pgpro_redefinition._check_all_redef_finished();
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._start_service_job(
) as
$body$
declare
    schedule_query                      text default 'call pgpro_redefinition._service(); select schedule.resubmit(%1$L::interval)';
    schedule_name                       text default '_redef_pgpro_redefinition_service';
    schedule_comments                   text default 'pgpro_redefinition - service tasts';
    resubmit_limit                      bigint default 1000000000;
    schedule_job_id                     bigint;
begin
    select value::bigint into schedule_job_id
      from pgpro_redefinition.redefinition_config c
     where c.param = pgpro_redefinition._param_service_job_id();

    call pgpro_redefinition._check_pgpro_scheduler();

    if schedule_job_id is null then
        schedule_query := format(schedule_query, pgpro_redefinition._param_service_resubmit_interval_sec());
        schedule_job_id := schedule.submit_job(
            query               => schedule_query
        ,   name                => schedule_name
        ,   comments            => schedule_comments
        ,   resubmit_limit      => resubmit_limit
        );
        insert into pgpro_redefinition.redefinition_config (
            param
        ,   value
        ) values (
            pgpro_redefinition._param_service_job_id()
        ,   schedule_job_id::text
        );
    else
        call pgpro_redefinition._info(
            errcode     => 'DFM27'
        ,   message     => format ('Service task already started. id = %s', schedule_job_id::text)
        );
    end if;

end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._stop_service_job(
) as
$body$
declare
    schedule_job_id                     bigint;
begin
    select value::bigint into schedule_job_id
      from pgpro_redefinition.redefinition_config c
     where c.param = pgpro_redefinition._param_service_job_id();

    if schedule_job_id is null then
        call pgpro_redefinition._error(
            errcode     => 'DFE27'
        ,   message     => format ('Service task not started.')
        );
    else
        call pgpro_redefinition._cancel_job(job_id => schedule_job_id);
        delete from pgpro_redefinition.redefinition_config
        where param = pgpro_redefinition._param_service_job_id();
    end if;

end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._check_redef_finished(
    configuration_name      varchar(63)
) as
$body$
declare
    configuration_row               pgpro_redefinition.redef_table;
    redef_finished                  boolean;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _check_redef_finished.configuration_name
    );

    redef_finished := exists(
        select *
        from pgpro_redefinition.redef_last_pkey lpk
        join pgpro_redefinition.redef_highest_pkey hpk on lpk.configuration_name = hpk.configuration_name
                                                      and lpk.pkey = hpk.pkey
        where lpk.configuration_name = _check_redef_finished.configuration_name
          and hpk.configuration_name = _check_redef_finished.configuration_name
    );

    if redef_finished then
        call pgpro_redefinition._raise_log(
            loglevel                                => pgpro_redefinition._loglevel_info()
        ,   errcode                                 => 'DFM88'
        ,   message                                 => format('Redefenition of configuration %1$s finished'
                                                        ,   _check_redef_finished.configuration_name
                                                       )
        );
        call pgpro_redefinition.pause_redef_table(
            configuration_name => _check_redef_finished.configuration_name
        );
        call pgpro_redefinition._set_redef_status(
            configuration_name  => _check_redef_finished.configuration_name
        ,   status              => pgpro_redefinition._status_redef_finished()
        );
    end if;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._check_all_redef_finished(
) as
$body$
declare
    conf_name               text;
    n                                           integer;
    local_returned_sqlstate                     text;
    local_column_name                           text;
    local_constraint_name                       text;
    local_pg_datatype_name                      text;
    local_message_text                          text;
    local_table_name                            text;
    local_schema_name                           text;
    local_pg_exception_detail                   text;
    local_pg_exception_hint                     text;
    local_pg_exception_context                  text;
    local_pg_context                            text;
    log_id                                      bigint;
    is_err                                      boolean;
    current_query                               text;
    raise_exception                             boolean default false;
begin
    for conf_name in (
    select configuration_name
      from pgpro_redefinition.redef_table r
    where r.status_redef in (pgpro_redefinition._status_redef_started(), pgpro_redefinition._status_apply_pause())
    ) loop
        begin
            call pgpro_redefinition._check_redef_finished(
                configuration_name  => conf_name
            );
        exception
            when others then
                get stacked diagnostics
                    local_returned_sqlstate = returned_sqlstate,
                    local_column_name = column_name,
                    local_constraint_name = constraint_name,
                    local_pg_datatype_name = pg_datatype_name,
                    local_message_text = message_text,
                    local_table_name = table_name,
                    local_schema_name = schema_name,
                    local_pg_exception_detail = pg_exception_detail,
                    local_pg_exception_hint = pg_exception_hint;
                --get diagnostics local_pg_context = pg_context;
                is_err = true;
                current_query := current_query();
                log_id := pgpro_redefinition._log(
                    loglevel                => pgpro_redefinition._loglevel_exception()
                ,   errcode                 => 'DFE79'::varchar(5)
                ,   message                 => 'redef_proc'::text
                ,   hint                    => current_query::text
                ,   configuration_name      => conf_name
                ,   sql                     => format('call pgpro_redefinition._check_redef_finished(configuration_name  => %1$L);'
                                                    , conf_name
                                               )
                );
                call pgpro_redefinition._stacked_diagnostics(
                    log_id                  => log_id
                ,   returned_sqlstate       => local_returned_sqlstate
                ,   column_name             => local_column_name
                ,   constraint_name         => local_constraint_name
                ,   pg_datatype_name        => local_pg_datatype_name
                ,   message_text            => local_message_text
                ,   table_name              => local_table_name
                ,   schema_name             => local_schema_name
                ,   pg_exception_detail     => local_pg_exception_detail
                ,   pg_exception_hint       => local_pg_exception_hint
                ,   pg_exception_context    => local_pg_context
                );
                raise_exception := pgpro_redefinition._default_on_error_stop();
                if raise_exception then
                    raise exception using
                        message     = local_message_text,
                        detail      = local_message_text,
                        hint        = local_pg_exception_hint,
                        errcode     = local_returned_sqlstate,
                        column      = local_column_name,
                        constraint  = local_constraint_name,
                        datatype    = local_pg_datatype_name,
                        table       = local_table_name,
                        schema      = local_schema_name;
                end if;
        end;
    end loop;
end;
$body$ language plpgsql;
