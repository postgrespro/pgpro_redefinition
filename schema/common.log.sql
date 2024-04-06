create sequence pgpro_redefinition.log_id_seq;

create table pgpro_redefinition.log (
    id                          bigint default nextval('pgpro_redefinition.log_id_seq')
,   ts                          timestamp
,   loglevel                    smallint
,   message                     text
,   hint                        text
,   errcode                     varchar(5)
,   configuration_name          varchar(63)
,   sql                         text
,   is_error                    boolean
,   stacked_diagnostics_id      bigint
,   primary key (id)
);

create table pgpro_redefinition.stacked_diagnostics (
    log_id                              bigint
,   returned_sqlstate                   text
,   column_name                         text
,   constraint_name                     text
,   pg_datatype_name                    text
,   message_text                        text
,   table_name                          text
,   schema_name                         text
,   pg_exception_detail                 text
,   pg_exception_hint                   text
,   pg_exception_context                text
,   primary key (log_id)
);

create or replace function pgpro_redefinition._loglevel_notset(
) returns smallint as
$body$
    select 0;
$body$ language sql immutable;

create or replace function pgpro_redefinition._loglevel_notice(
) returns smallint as
$body$
    select 10;
$body$ language sql immutable;

create or replace function pgpro_redefinition._loglevel_info(
) returns smallint as
$body$
    select 20;
$body$ language sql immutable;

create or replace function pgpro_redefinition._loglevel_exception(
) returns smallint as
$body$
    select 40;
$body$ language sql immutable;

create or replace function pgpro_redefinition._current_loglevel_logging(
) returns smallint as
$body$
    select pgpro_redefinition._loglevel_notice();
$body$ language sql immutable ;

create or replace function pgpro_redefinition._current_loglevel_raise(
) returns smallint as
$body$
    select pgpro_redefinition._loglevel_notice()
$body$ language sql immutable;

create or replace function pgpro_redefinition._log_write_to_log(
) returns boolean as
$body$
    select true;
$body$ language sql immutable;

create or replace function pgpro_redefinition._log_show_message(
) returns boolean as
$body$
    select true;
$body$ language sql immutable;

/*create or replace procedure pgpro_redefinition._log(
    loglevel                                integer
,   errcode                                 varchar(5)
,   message                                 text
,   hint                                    text
) as
$body$
declare

begin autonomous
    insert into pgpro_redefinition.log (
        ts
    ,   loglevel
    ,   message
    ,   errcode
    ,   hint
    )
    values (
        clock_timestamp()
    ,   _log.loglevel
    ,   _log.message
    ,   _log.errcode
    ,   _log.hint
    );
end;
$body$ language plpgsql;
*/

create or replace function pgpro_redefinition._log(
    loglevel                                integer
,   errcode                                 varchar(5)
,   message                                 text
,   hint                                    text
,   configuration_name                      varchar(63) default null
,   sql                                     text default null
) returns bigint as
$body$
declare
    log_id          bigint;
begin autonomous
    if configuration_name is null then
        configuration_name := pgpro_redefinition._get_session_configuration_name();
    end if;
    insert into pgpro_redefinition.log (
        ts
    ,   loglevel
    ,   message
    ,   errcode
    ,   hint
    ,   configuration_name
    ,   sql
    )
    values (
        clock_timestamp()
    ,   _log.loglevel
    ,   _log.message
    ,   _log.errcode
    ,   _log.hint
    ,   _log.configuration_name
    ,   _log.sql
    ) returning id into log_id;
    return log_id;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._stacked_diagnostics(
    log_id                      bigint
,   returned_sqlstate           text
,   column_name                 text
,   constraint_name             text
,   pg_datatype_name            text
,   message_text                text
,   table_name                  text
,   schema_name                 text
,   pg_exception_detail         text
,   pg_exception_hint           text
,   pg_exception_context        text
) as
$body$
declare
begin autonomous
    insert into pgpro_redefinition.stacked_diagnostics (
        log_id
    ,   returned_sqlstate
    ,   column_name
    ,   constraint_name
    ,   pg_datatype_name
    ,   message_text
    ,   table_name
    ,   schema_name
    ,   pg_exception_detail
    ,   pg_exception_hint
    ,   pg_exception_context
    )
    values (
        _stacked_diagnostics.log_id
    ,   _stacked_diagnostics.returned_sqlstate
    ,   _stacked_diagnostics.column_name
    ,   _stacked_diagnostics.constraint_name
    ,   _stacked_diagnostics.pg_datatype_name
    ,   _stacked_diagnostics.message_text
    ,   _stacked_diagnostics.table_name
    ,   _stacked_diagnostics.schema_name
    ,   _stacked_diagnostics.pg_exception_detail
    ,   _stacked_diagnostics.pg_exception_hint
    ,   _stacked_diagnostics.pg_exception_context
    );
    update pgpro_redefinition.log
        set is_error = true
    where id = log_id;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._raise(
    loglevel                                integer
,   errcode                                 varchar(5)
,   message                                 text
,   hint                                    text default null
) as
$body$
declare
begin
    if loglevel = pgpro_redefinition._loglevel_notice()  then
        if pgpro_redefinition._log_show_message() then
            raise notice '%',        message  using  errcode = errcode, hint = _raise.hint;
        end if;
    elseif loglevel = pgpro_redefinition._loglevel_info()  then
        if pgpro_redefinition._log_show_message() then
            raise info '%',         message using  errcode = errcode, hint = _raise.hint;
        end if;
    elseif loglevel = pgpro_redefinition._loglevel_exception() then
        raise exception '%',    message using  errcode = errcode, hint = _raise.hint;
    end if;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._raise_log(
    loglevel                                integer
,   errcode                                 varchar(5)
,   message                                 text
,   hint                                    text default 'null'
) as
$body$
declare
begin
    if pgpro_redefinition._log_write_to_log() then
        perform pgpro_redefinition._log(
            loglevel                                => _raise_log.loglevel
        ,   errcode                                 => _raise_log.errcode
        ,   message                                 => _raise_log.message
        ,   hint                                    => _raise_log.hint
        );
    end if;

    call pgpro_redefinition._raise(
        loglevel    => loglevel
    ,   errcode     => errcode
    ,   message     => message
    ,   hint        => _raise_log.hint
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._error(
    errcode                                 varchar(5)
,   message                                 text
,   hint                                    text default 'null'
) as
$body$
declare
begin
    call pgpro_redefinition._raise_log(
        loglevel                                => pgpro_redefinition._loglevel_exception()
    ,   errcode                                 => _error.errcode
    ,   message                                 => _error.message
    ,   hint                                    => _error.hint
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._debug(
    errcode                                 varchar(5)
,   message                                 text
,   hint                                    text default 'null'
) as
$body$
declare
begin
    call pgpro_redefinition._raise_log(
        loglevel                                => pgpro_redefinition._loglevel_notice()
    ,   errcode                                 => _debug.errcode
    ,   message                                 => _debug.message
    ,   hint                                    => _debug.hint
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._info(
    errcode                                 varchar(5)
,   message                                 text
,   hint                                    text default 'null'
) as
$body$
declare
begin
    call pgpro_redefinition._raise_log(
        loglevel                                => pgpro_redefinition._loglevel_info()
    ,   errcode                                 => _info.errcode
    ,   message                                 => _info.message
    ,   hint                                    => _info.hint
    );
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._execute_sql(
    sql                                 text
,   raise_exception                     boolean default true
,   configuration_name                  varchar(63) default null
) as
$body$
declare
    configuration_row                           pgpro_redefinition.redef_table;
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
    is_err                                      boolean default false;
begin
    configuration_name := pgpro_redefinition._get_session_configuration_name();
    log_id := pgpro_redefinition._log(
        loglevel                => pgpro_redefinition._loglevel_info()
    ,   errcode                 => 'DFM39'::varchar(5)
    ,   message                 => 'sql'::text
    ,   hint                    => null::text
    ,   configuration_name      => _execute_sql.configuration_name
    ,   sql                     => _execute_sql.sql
    );

    begin
        execute sql;

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

    update pgpro_redefinition.log
        set is_error = is_err
    where id = log_id;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._set_session_configuration_name(
    value           varchar(63)
) as
$body$
declare
begin
    perform set_config('pgpro_redefinition.configuration_name', value, false);
end;
$body$ language plpgsql;

create or replace function pgpro_redefinition._get_session_configuration_name(
) returns varchar(63) as
$body$
declare
begin
    return coalesce(current_setting('pgpro_redefinition.configuration_name', true), 'null');
end;
$body$ language plpgsql;
