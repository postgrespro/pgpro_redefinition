create or replace procedure pgpro_redefinition._start_capture_deferred(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name          => _start_capture_deferred.configuration_name
    );
    if configuration_row.status <> pgpro_redefinition._status_config_registered() then
        call pgpro_redefinition._error(
            errcode         => 'DFE21'
        ,   message         => format('Impossible to start capture data. '
                                      'Bad current  status - %1$s of configuration %2$s.'
                                    ,   configuration_row.status
                                    ,   configuration_row.configuration_name
                               )
        ,   hint            => format('Current status must be %1$s'
                                    ,   pgpro_redefinition._status_config_registered()
                               )
        );
    end if;
    if configuration_row.captured then
        call pgpro_redefinition._error(
            errcode         => 'DFE22'
        ,   message         => format('Capture data already started' )
        );
    end if;
    call pgpro_redefinition._enable_mlog_trigger(
        configuration_name       => _start_capture_deferred.configuration_name
    );
    update pgpro_redefinition.redef_table
       set captured = true
     where redef_table.configuration_name =  _start_capture_deferred.configuration_name;

    call pgpro_redefinition._info(
        errcode         => 'DFM38'
    ,   message         => format('Capture data in configuration %1$s started'
                                    ,   configuration_row.configuration_name
                                )
    );
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._store_to_mlog(
) returns trigger as
$body$
BEGIN
    insert into pgpro_redefinition.mlog (
        config_id
    ,   table_oid
    ,   lsn
    ,   before
    ,   after
    ) values (
        TG_ARGV[0]::bigint
    ,   TG_RELID
    ,   pg_current_wal_lsn()
    ,   row_to_json (old)
    ,   row_to_json (new)
    );
    return null;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_deferred_trigger(
    configuration_name                  varchar(63)
,   drop_trigger_if_exists              boolean default true
,   disable_trigger                     boolean default true
) returns text as
$body$
declare
    sql                     text default '';
    configuration_row       pgpro_redefinition.redef_table;
    drop_trigger            text default '';
    disable_tr              text default '';
    trigger                 text default $redef$%1$s
create trigger %2$I
    after insert or update or delete on %3$I.%4$I
    for each row execute procedure %5$I.%6$I(%7$L);
$redef$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_deferred_trigger.configuration_name
    );

    if drop_trigger_if_exists then
        disable_tr := format(
            'drop trigger if exists %1$I on %2$I.%3$I;'
        ,   configuration_row.source_trigger_name
        ,   configuration_row.source_schema_name
        ,   configuration_row.source_table_name
        );
        sql := sql || E'\n' || disable_tr;
    end if;
    trigger:= format(
            trigger
        ,   drop_trigger
        ,   configuration_row.source_trigger_name
        ,   configuration_row.source_schema_name
        ,   configuration_row.source_table_name
        ,   'pgpro_redefinition'
        ,   '_store_to_mlog'
        ,   configuration_row.id::text
        );
        sql := sql || E'\n' || trigger;

    if disable_trigger then
        disable_tr := format(
                'alter table %1$I.%2$I disable trigger %3$I;'
            ,   configuration_row.source_schema_name
            ,   configuration_row.source_table_name
            ,   configuration_row.source_trigger_name
            );
            sql := sql || E'\n' || disable_tr;
    end if;

    call pgpro_redefinition._debug(
        errcode         => 'DFM39'
    ,   message         => format('Text of trigger: %1$s '
                                ,   sql
                           )
    );
    return sql;
end;
$body$ language plpgsql ;

create or replace procedure pgpro_redefinition._enable_mlog_trigger(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    enable_source_trigger               text default 'alter table %1$I.%2$I enable trigger %3$I;';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name          => _enable_mlog_trigger.configuration_name
    );

    enable_source_trigger := format(
        enable_source_trigger
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   configuration_row.source_trigger_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM40'
    ,   message         => format('Text of alter table: %1$s '
                                ,   enable_source_trigger
                           )
    );
    call pgpro_redefinition._execute_sql(sql => enable_source_trigger);
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_deferred_apply_mlog_row_proc_name(
    configuration_name                  varchar(63)
) returns varchar(63) as
$body$
/*

*/
declare
    configuration_row           pgpro_redefinition.redef_table;
    proc_name                   varchar(63) default '_redef_deferred_apply_mlog_row_%1$s';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_deferred_apply_mlog_row_proc_name.configuration_name
    );

    proc_name:= format(
            proc_name
        ,   md5(configuration_row.configuration_name)
        );
    call pgpro_redefinition._debug(
        errcode         => 'DFM41'
    ,   message         => format('Name of proc: %1$s '
                                ,   proc_name
                           )
    );
    return proc_name;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_deferred_apply_mlog_row_proc_body_copy(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    name_type_array                     text[];
    name_type                           text;
    where_pkey_array                    text[];
    where_pkey                          text;
    update_set_array                    text[];
    update_set                          text;
    on_conflict_pk                      text;
    deferred_apply_proc_body                text default $proc$$deferred$
declare
    dest_old            %1$I.%2$I;
    dest_new            %1$I.%2$I;
    rec                 record;
begin
    select * into rec
      from json_to_record(ml.before) rec_old(
                %3$s
            )
      ,    json_to_record(ml.after) rec_new(
                %3$s
            )
      ,     %6$I.%7$I(
                source_old => rec_old
            ,   source_new => rec_new
            ) c
        ;

    dest_new := rec.dest_new;
    dest_old := rec.dest_old;

    if ml.before is null then
        insert into %1$I.%2$I values (dest_new.*)
        --on conflict (%8$s)
        --do update
        on conflict do nothing ;
        /*set %5$s;*/
    elseif ml.after is null then
        delete from %1$I.%2$I
        where %4$s;
    else
        update %1$I.%2$I
        set %5$s
        where %4$s;
    end if;
end;
$deferred$$proc$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_deferred_apply_mlog_row_proc_body_copy.configuration_name
    );
    if configuration_row.dest_table_name is null or configuration_row.dest_schema_name is null then
        call pgpro_redefinition._error(
            errcode         => 'DFE23'
        ,   message         => format('Impossible to generate apply procedure. Destination table not set')
        );
    end if;
    name_type_array := array(
        select format ('%1$I    %2$s', c.column_name, case c.data_type
                                                        when 'ARRAY' then c.udt_name
                                                        else (
                                                                case when c.data_type = 'character'
                                                                    then c.data_type || '(' || c.character_maximum_length || ')'
                                                                    else c.data_type
                                                                end
                                                            )
                                                    end
                        )
          from information_schema.columns c
         where c.table_schema = configuration_row.source_schema_name
           and c.table_name = configuration_row.source_table_name
        order by ordinal_position
    );
    name_type := array_to_string(name_type_array, E',\n                ');

    where_pkey := (
        select format (E'(\n                %1$s            \n          )', string_agg(q.cond, E'\n           and  '))
        from (
            select format('%1$I.%2$I = dest_old.%2$I ', configuration_row.dest_table_name, rec.name) as cond
            from jsonb_to_recordset(configuration_row.source_pkey) as rec( pos int, name text, type text)
            order by rec.pos
            ) q
    );
    update_set_array := array(
        select format ('%1$I = dest_new.%1$I', c.column_name)
          from information_schema.columns c
         where c.table_schema = configuration_row.source_schema_name
           and c.table_name = configuration_row.source_table_name
        order by ordinal_position
    );
    update_set := array_to_string(update_set_array, E',\n            ');

    select string_agg(q.name, ', ') into on_conflict_pk
    from (
        select rec.name
        from jsonb_to_recordset(configuration_row.source_pkey) as rec( pos int, name text, type text)
        order by rec.pos
        ) q;

    deferred_apply_proc_body := format(
        deferred_apply_proc_body
    ,   configuration_row.dest_schema_name
    ,   configuration_row.dest_table_name
    ,   name_type
    ,   where_pkey
    ,   update_set
    ,   configuration_row.callback_schema_name
    ,   configuration_row.callback_name
    ,   on_conflict_pk
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM42'
    ,   message         => format('Body of proc: %1$s '
                                ,   deferred_apply_proc_body
                           )
    );
    return deferred_apply_proc_body;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_deferred_apply_mlog_row_proc_body_any(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    name_type_array                     text[];
    name_type                           text;
    deferred_apply_proc_body            text default $proc$$deferred$
declare
    rec                 record;
begin
    perform
      from json_to_record(ml.before) rec_old(
                %1$s
            )
      ,    json_to_record(ml.after) rec_new(
                %1$s
            )
      ,     %2$I.%3$I(
                source_old => rec_old
            ,   source_new => rec_new
            ) c
        ;

end;
$deferred$$proc$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_deferred_apply_mlog_row_proc_body_any.configuration_name
    );
    name_type_array := array(
        select format ('%1$I    %2$s', c.column_name, case c.data_type
                                                        when 'ARRAY' then c.udt_name
                                                        else (
                                                                case when c.data_type = 'character'
                                                                    then c.data_type || '(' || c.character_maximum_length || ')'
                                                                    else c.data_type
                                                                end
                                                            )
                                                    end
                        )
          from information_schema.columns c
         where c.table_schema = configuration_row.source_schema_name
           and c.table_name = configuration_row.source_table_name
        order by ordinal_position
    );
    name_type := array_to_string(name_type_array, E',\n                ');
    deferred_apply_proc_body := format(
        deferred_apply_proc_body
    ,   name_type
    ,   configuration_row.callback_schema_name
    ,   configuration_row.callback_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM43'
    ,   message         => format('Body of proc: %1$s '
                                ,   deferred_apply_proc_body
                           )
    );
    return deferred_apply_proc_body;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_deferred_apply_mlog_row_proc(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    deferred_apply_proc_body            text;
    deferred_apply_proc                 text default $proc$create or replace procedure %1$I.%2$I(
    ml              pgpro_redefinition.mlog
)
as %3$s
language plpgsql$proc$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_deferred_apply_mlog_row_proc.configuration_name
    );
    if configuration_row.kind = pgpro_redefinition._kind_redef() then
        deferred_apply_proc_body = pgpro_redefinition._generate_deferred_apply_mlog_row_proc_body_copy(_generate_deferred_apply_mlog_row_proc.configuration_name);
    elseif configuration_row.kind = pgpro_redefinition._kind_any() then
        deferred_apply_proc_body = pgpro_redefinition._generate_deferred_apply_mlog_row_proc_body_any(_generate_deferred_apply_mlog_row_proc.configuration_name);
    end if;

    deferred_apply_proc := format(
        deferred_apply_proc
    ,   configuration_row.apply_mlog_row_proc_schema_name
    ,   configuration_row.apply_mlog_row_proc_name
    ,   deferred_apply_proc_body
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM44'
    ,   message         => format('Text of proc: %1$s '
                                ,   deferred_apply_proc
                           )
    );
    return deferred_apply_proc;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_deferred_apply_proc_name(
    configuration_name                  varchar(63)
) returns varchar(63) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    proc_name                   varchar(63) default 'redef_deferred_proc_%1$s';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_deferred_apply_proc_name.configuration_name
    );

    proc_name:= format(
            proc_name
        ,   md5(configuration_row.configuration_name)
        );
    call pgpro_redefinition._debug(
        errcode         => 'DFM45'
    ,   message         => format('Name of proc: %1$s '
                                ,   proc_name
                           )
    );
    return proc_name;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_deferred_apply_proc_body(
    configuration_name                  text
) returns text as
$body$
/*   todo -- различные оптимизации
          -- не удалять сразу, удалят позднее
          -- сначала добавление всего набора стррок, потом обновление, потом удаление ?
*/
declare
    configuration_row           pgpro_redefinition.redef_table;
    deferred_proc_body          text default $proc$$redef$
declare
    configuration_row       pgpro_redefinition.redef_table;
    ml                      pgpro_redefinition.mlog;
    n                       integer;
    cnt                     integer default 0;
    ts_start                timestamp;
    last_lsn                pg_lsn;
    last_pos                bigint;
begin autonomous
    ts_start := clock_timestamp();
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => %1$L
    );
    select p.lsn, p.pos
      into last_lsn, last_pos
      from pgpro_redefinition.mlog_last_applied_pkey p
    where config_id = configuration_row.id;

    n := configuration_row.rows_apply;
    for ml in (
        select *
          from pgpro_redefinition.mlog m
         where m.config_id = configuration_row.id
           and (
                    (last_lsn is null and last_pos is null)
                    or
                    ((m.lsn, m.pos) > (last_lsn, last_pos))
                )
         order by m.lsn, m.pos
         limit n
    ) loop
        call %2$I.%3$I(ml);
        /*
        delete from pgpro_redefinition.mlog mlog
        where mlog.config_id = ml.config_id
          and mlog.lsn = ml.lsn
          and mlog.pos = ml.pos
        ;
        */
        cnt := cnt +1;
    end loop;

    if ml.config_id is not null then
        insert into pgpro_redefinition.mlog_last_applied_pkey(
                config_id
            ,   lsn
            ,   pos
            ) values (
                ml.config_id
            ,   ml.lsn
            ,   ml.pos
            ) on conflict (config_id) do update
            set lsn = coalesce(ml.lsn, mlog_last_applied_pkey.lsn)
            ,   pos = coalesce(ml.pos, mlog_last_applied_pkey.pos)
            where mlog_last_applied_pkey.config_id = ml.config_id
            ;
    end if;
    insert into pgpro_redefinition.inc_stat(
            configuration_name
        ,   job_type
        ,   dest_selected
        ,   dest_inserted
        ,   ts_start
        ,   ts_finish
        )
        values(
            %1$L
        ,   'apply_data'
        ,   n
        ,   cnt
        ,   ts_start
        ,   clock_timestamp()
        );

end;
$redef$$proc$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_deferred_apply_proc_body.configuration_name
    );
    deferred_proc_body:= format(
        deferred_proc_body
    ,   configuration_row.configuration_name
    ,   configuration_row.apply_mlog_row_proc_schema_name
    ,   configuration_row.apply_mlog_row_proc_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM46'
    ,   message         => format('Body of proc: %1$s '
                                ,   deferred_proc_body
                           )
    );
    return deferred_proc_body;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_deferred_apply_proc(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    deferred_apply_proc        text default $proc$create or replace procedure %1$I.%2$I()
as %3$s
language plpgsql$proc$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_deferred_apply_proc.configuration_name
    );
    deferred_apply_proc := format(
        deferred_apply_proc
    ,   configuration_row.apply_proc_schema_name
    ,   configuration_row.apply_proc_name
    ,   pgpro_redefinition._generate_deferred_apply_proc_body(_generate_deferred_apply_proc.configuration_name)
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM47'
    ,   message         => format('Text of proc: %1$s '
                                ,   deferred_apply_proc
                           )
    );
    return deferred_apply_proc;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_deferred_apply_loop_proc_name(
    configuration_name                  text
) returns text as
$body$
declare
    configuration_row                       pgpro_redefinition.redef_table;
    deferred_apply_loop_name                text default '_redef_deferred_apply_loop_%1$s';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _generate_deferred_apply_loop_proc_name.configuration_name
    );

    deferred_apply_loop_name:= format(
            deferred_apply_loop_name
        ,   md5(configuration_row.configuration_name)
        );
    call pgpro_redefinition._debug(
        errcode         => 'DFM48'
    ,   message         => format('Name of proc: %1$s '
                                ,   deferred_apply_loop_name
                           )
    );
    return deferred_apply_loop_name;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_deferred_loop_proc_body(
    configuration_name                  text
) returns text as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    deferred_loop_proc_body     text default $proc$$redef$
declare
    configuration_row       pgpro_redefinition.redef_table;
    n                       integer;
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
    n := configuration_row."loop_apply";
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
                ,   errcode                 => 'DFM79'::varchar(5)
                ,   message                 => 'loop_proc'::text
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

        perform pg_sleep(configuration_row.sleep_apply::numeric / 1000);
        if configuration_row.schedule_jobs_apply_id is not null then
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
end;
$redef$$proc$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_deferred_loop_proc_body.configuration_name
    );
    deferred_loop_proc_body:= format(
        deferred_loop_proc_body
    ,   configuration_row.configuration_name
    ,   configuration_row.apply_proc_schema_name
    ,   configuration_row.apply_proc_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM49'
    ,   message         => format('Body of proc: %1$s '
                                ,   deferred_loop_proc_body
                           )
    );
    return deferred_loop_proc_body;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_deferred_loop_proc(
    configuration_name                  text
) returns text as
$body$
declare
    configuration_row       pgpro_redefinition.redef_table;
    deferred_loop_proc        text default $proc$create or replace procedure %1$I.%2$I(
)as
%3$s
language plpgsql$proc$;

begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_deferred_loop_proc.configuration_name
    );

    deferred_loop_proc:= format(
        deferred_loop_proc
    ,   configuration_row.apply_loop_proc_schema_name
    ,   configuration_row.apply_loop_proc_name
    ,   pgpro_redefinition._generate_deferred_loop_proc_body(
            configuration_name  => _generate_deferred_loop_proc.configuration_name
        )
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM50'
    ,   message         => format('Text of proc: %1$s '
                                ,   deferred_loop_proc
                           )
    );
    return deferred_loop_proc;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.start_apply_mlog(
    configuration_name                  varchar(63)
,   flow_type                           pgpro_redefinition.flow_type default pgpro_redefinition._flow_type_separate()
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    schedule_query                      text default 'call %1$I.%2$I(); select schedule.resubmit()';
    schedule_name                       text default '_redef_apply_%1$s_job%2$s';
    schedule_comments                   text default 'pgpro_redefinition - apply data job № %1$s of %2$I.%3$I';
    resubmit_limit                      bigint default 1000000000;
    schedule_job_id                     bigint;
    njob                                bigint  default 1;
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => start_apply_mlog.configuration_name
    );
    if configuration_row.status not in (pgpro_redefinition._status_config_registered()) then
        call pgpro_redefinition._error(
            errcode         => 'DFE24'
        ,   message         => format('Impossible to start apply mlog. Configuration not registred.'
                                      'Current status of configuration: %1$s'
                                    ,   configuration_row.status
                               )
        ,   hint            => format('Status must be %1$s'
                                ,   pgpro_redefinition._status_config_registered()
                               )
        );
    end if;
    if configuration_row.captured <> true then
        call pgpro_redefinition._error(
            errcode         => 'DFE25'
        ,   message         => format('Captured data not started. ')
        ,   hint            => format('Use function pgpro_redefinition.enable_save_to_mlog')
        );
    end if;
    if configuration_row.status_apply is not null then
        if configuration_row.status_apply <> pgpro_redefinition._status_apply_pause() then
            call pgpro_redefinition._error(
                errcode         => 'DFE26'
            ,   message         => format('Apply status (%1$s) not empty. Apply data already started'
                                   ,    configuration_row.status_apply
                                   )
            );
        end if;
    end if;
    if configuration_row.type <> 'deferred' then
        call pgpro_redefinition._error(
            errcode         => 'DFE27'
        ,   message         => format('Apply data from mlog table only for type = %1$s.'
                                ,   pgpro_redefinition._type_deferred()
                               )
        );
    end if;
    if flow_type = pgpro_redefinition._flow_type_separate() then
        call pgpro_redefinition._check_pgpro_scheduler();
        schedule_query := format(
            schedule_query
        ,   configuration_row.apply_loop_proc_schema_name
        ,   configuration_row.apply_loop_proc_name
        );
        schedule_name := format(
            schedule_name
        ,   configuration_row.configuration_name
        ,   njob::text
        );
        schedule_comments := format(
            schedule_comments
        ,   njob::text
        ,   configuration_row.apply_loop_proc_schema_name
        ,   configuration_row.apply_loop_proc_name
        );
        schedule_job_id := schedule.submit_job(
            query               => schedule_query
        ,   name                => schedule_name
        ,   comments            => schedule_comments
        ,   resubmit_limit      => resubmit_limit
        );
        update pgpro_redefinition.redef_table rt
           set schedule_jobs_apply_id = array [schedule_job_id]
         where rt.configuration_name = start_apply_mlog.configuration_name;
    elsif flow_type = pgpro_redefinition._flow_type_general() then
        insert into pgpro_redefinition.redef_weight (
            config_id
        ,   job_type
        ,   locked
        ,   weight
        ) values (
            configuration_row.id
        ,   pgpro_redefinition._job_type_apply_data()
        ,   false
        ,   configuration_row.weight
        );
    end if;

    call pgpro_redefinition._set_apply_status(
        configuration_name  => start_apply_mlog.configuration_name
    ,   status              => pgpro_redefinition._status_apply_started()
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM51'
    ,   message         => format('Apply of configuration %1$s started'
                                ,   start_apply_mlog.configuration_name
                           )
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._start_saving_redef_to_mlog(
    configuration_name                  varchar(63)
) as
$body$
declare
begin
    call pgpro_redefinition.start_apply_mlog(configuration_name => _start_saving_redef_to_mlog.configuration_name);
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_redef_to_mlog_def_proc_name(
    configuration_name                  varchar(63)
) returns text as
$body$
declare
    configuration_row               pgpro_redefinition.redef_table;
    proc_name                       text default '%1$I';
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_redef_to_mlog_def_proc_name.configuration_name
    );

    proc_name:= format(
        proc_name
    ,   '_redef_deffered_to_mlog_' || md5(configuration_row.configuration_name)
    );

    call pgpro_redefinition._debug(
        errcode         => 'DFM50'
    ,   message         => format('Name of redef procedure to save to mlog: %1$s'
                            ,   proc_name
                            )
    );
    return proc_name;
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._generate_redef_to_mlog_def_proc_body(
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
    prepared_statement_name     text;
    proc_body                   text default $proc$$redef$
declare
    ts_start            timestamp;
    current_lsn         pg_lsn;
    rows_redef_limit    integer;
begin %7$s
    ts_start := clock_timestamp();
    current_lsn := pg_current_wal_lsn();
    select rows_redef into rows_redef_limit
      from pgpro_redefinition.redef_table
     where configuration_name = %5$L;

    if not exists(select 1 from pg_prepared_statements where name = %11$L) then

        PREPARE %11$I (int, pg_lsn, timestamp) AS
            with data as (
                select *
                from %1$I.%2$I
                where (%3$s) > (
                    select %4$s
                      from pgpro_redefinition.redef_last_pkey p
                     where configuration_name = %5$L
                    )
                order by %3$s
                limit $1
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
            insert_mlog as (
                insert into pgpro_redefinition.mlog (
                        config_id
                    ,   table_oid
                    ,   lsn
                    ,   before
                    ,   after
                    )
                    select %6$s
                         , %10$s
                         , $2
                         , null
                         , row_to_json(data.*)
                    from data
                    returning 1
            ),
            insert_inc_stat as (
                insert into pgpro_redefinition.inc_stat(
                        configuration_name
                    ,   job_type
                    ,   dest_selected
                    ,   dest_inserted
                    ,   ts_start
                    ,   ts_finish
                    )
                    select %5$L as configuration_name
                         , pgpro_redefinition._job_type_redef_data()    as job_type
                         , (select count(*) from data)                  as dest_selected
                         , (select count(*) from insert_mlog)           as dest_inserted
                         , $3                                           as ts_start
                         , clock_timestamp()                            as ts_finish
            )
        select 1;
    end if;
    EXECUTE format('EXECUTE %11$I(%%1$s,%%2$L::pg_lsn,%%3$L::timestamp)'
        , rows_redef_limit
        , current_lsn
        , ts_start
    );
    %8$s
end;
$redef$$proc$;
begin
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name => _generate_redef_to_mlog_def_proc_body.configuration_name
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
    if _generate_redef_to_mlog_def_proc_body.commit_type = 'autonomous' then
        proc_autonomous = 'autonomous';
    elsif _generate_redef_to_mlog_def_proc_body.commit_type = 'autonomous' then
        proc_commit = E'\n    commit;';
    end if;

    prepared_statement_name := format(
        pgpro_redefinition._prepared_statement_name_template()
    ,   md5(configuration_row.configuration_name)::text
    );

    proc_body := format(
        proc_body
    ,   configuration_row.source_schema_name
    ,   configuration_row.source_table_name
    ,   pkey
    ,   last_pkey_from_json
    ,   configuration_row.configuration_name
    ,   configuration_row.id
    ,   proc_autonomous
    ,   proc_commit
    ,   configuration_row.kind
    ,   configuration_row.source_table_oid::text
    ,   prepared_statement_name
    );
    call pgpro_redefinition._debug(
        errcode         => 'DFM21'
    ,   message         => format('Body of procedure to copy def body: %1$s'
                            ,   proc_body
                            )
    );
    return proc_body;
end;
$body$ language plpgsql ;

create or replace function pgpro_redefinition._generate_redef_to_mlog_def_proc(
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
        configuration_name => _generate_redef_to_mlog_def_proc.configuration_name
    );

    proc_body:= pgpro_redefinition._generate_redef_to_mlog_def_proc_body(configuration_name => _generate_redef_to_mlog_def_proc.configuration_name);

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

create or replace procedure pgpro_redefinition.restart_apply_mlog(
    configuration_name                  varchar(63)
) as
$body$
declare
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    call pgpro_redefinition.start_apply_mlog(configuration_name => restart_apply_mlog.configuration_name);
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.pause_apply_mlog(
    configuration_name                  varchar(63)
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
begin
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);
    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => pause_apply_mlog.configuration_name
    ,   status              => array[pgpro_redefinition._status_config_registered()]
    );
    if configuration_row.status_apply <> pgpro_redefinition._status_apply_started() then
        call pgpro_redefinition._error(
            errcode         => 'DFM51'
        ,   message         => format('Impossible to pause apply mlog.'
                                'Apply not started. Current apply status %1$s'
                                    ,   configuration_row.status_apply
                               )
        );
    end if;
    if configuration_row.schedule_jobs_apply_id is not null then
        call pgpro_redefinition._cancel_job(
            job_id                  => configuration_row.schedule_jobs_apply_id[1]
        ,   schedule_wait_stop      => configuration_row.schedule_wait_stop
        );
        update pgpro_redefinition.redef_table
           set schedule_jobs_apply_id = null
         where redef_table.configuration_name = pause_apply_mlog.configuration_name;
    end if;

    delete from pgpro_redefinition.redef_weight
    where config_id = configuration_row.id;

    call pgpro_redefinition._set_apply_status(
        configuration_name  => pause_apply_mlog.configuration_name
    ,   status              => pgpro_redefinition._status_apply_pause()
    );

end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._remove_old_mlog_partition(
    configuration_name                  varchar(63)
,   drop_sub_partition                  boolean default true
) as
$body$
declare
    configuration_row                   pgpro_redefinition.redef_table;
    last_applied_pk                     pgpro_redefinition.mlog_last_applied_pkey;
    part_desc                           pgpro_redefinition.sub_part_mlog_desc;
    detach                              text;
    detach_template                     text default 'alter table %1$I.%2$I detach partition %3$I.%4$I';
    drop                                text;
    drop_template                       text default 'drop table %1$I.%2$I';
begin autonomous
    call pgpro_redefinition._set_session_configuration_name(value => configuration_name);

    select * into configuration_row
    from pgpro_redefinition._get_configuration(
        configuration_name  => _remove_old_mlog_partition.configuration_name
    ,   status              => array[pgpro_redefinition._status_config_registered()]
    );
    select * into last_applied_pk
      from pgpro_redefinition.mlog_last_applied_pkey p
      join pgpro_redefinition.redef_table r on r.id = p.config_id
     where r.configuration_name = _remove_old_mlog_partition.configuration_name;

    call pgpro_redefinition._debug(
        errcode         => 'DFM80'
    ,   message         => format('Try to delete sub partition on configuration %1$s (id=%2$s)'
                                  ', where lsn < %3$L'
                                ,   configuration_row.configuration_name
                                ,   configuration_row.id
                                ,   last_applied_pk.lsn
                           )
    );

    for part_desc in (select *
                      from pgpro_redefinition.sub_part_mlog_desc d
                      where d.config_id = configuration_row.id
                        and d.lsn_to < last_applied_pk.lsn
                        and d.is_deleted = false
                      order by d.lsn_from
    ) loop
        detach :=format(
            detach_template
        ,   pgpro_redefinition._default_schema()
        ,   configuration_row.mlog_part_table_name
        ,   pgpro_redefinition._default_schema()
        ,   part_desc.sub_part_table_name
        );
        call pgpro_redefinition._debug(
            errcode         => 'DFM81'
        ,   message         => format('Sql to detach sub partition: %1$s'
                                    ,   detach
                               )
        );
        call pgpro_redefinition._execute_sql (sql => detach);
        if drop_sub_partition then
            drop := format(
                drop_template
            ,   pgpro_redefinition._default_schema()
            ,   part_desc.sub_part_table_name
            );

            call pgpro_redefinition._debug(
                errcode         => 'DFM82'
            ,   message         => format('Sql to drop sub partition: %1$s'
                                        ,   drop
                                   )
            );
            call pgpro_redefinition._execute_sql (sql => drop);
        end if;
        update pgpro_redefinition.sub_part_mlog_desc d
          set is_deleted = true
            , deleted_ts = now()
        where d.id = part_desc.id;
    end loop;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._remove_all_old_mlog_partitions(
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
        call pgpro_redefinition._remove_old_mlog_partition(
            configuration_name  => conf_name
        );
    end loop;
end;
$body$ language plpgsql;

