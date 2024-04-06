
create or replace function pgpro_redefinition._status_config_new(
) returns text as
$body$
    select 'config_new';
$body$ language sql;

create or replace function pgpro_redefinition._status_config_registered(
) returns text as
$body$
    select 'config_registered';
$body$ language sql;

create or replace function pgpro_redefinition._status_config_finished(
) returns text as
$body$
    select 'config_finished';
$body$ language sql;

create or replace function pgpro_redefinition._status_config_aborted(
) returns text as
$body$
    select 'config_aborted';
$body$ language sql;

create or replace function pgpro_redefinition._status_redef_started(
) returns text as
$body$
    select 'reder_started';
$body$ language sql;

create or replace function pgpro_redefinition._status_redef_pause(
) returns text as
$body$
    select 'reder_pause';
$body$ language sql;

create or replace function pgpro_redefinition._status_redef_finished(
) returns text as
$body$
    select 'reder_finished';
$body$ language sql;

create or replace function pgpro_redefinition._status_redef_aborted(
) returns text as
$body$
    select 'reder_aborted';
$body$ language sql;

create or replace function pgpro_redefinition._status_apply_started(
) returns text as
$body$
    select 'apply_started';
$body$ language sql;

create or replace function pgpro_redefinition._status_apply_pause(
) returns text as
$body$
    select 'apply_pause';
$body$ language sql;

create or replace function pgpro_redefinition._status_apply_finished(
) returns text as
$body$
    select 'apply_finished';
$body$ language sql;

create or replace function pgpro_redefinition._status_apply_aborted(
) returns text as
$body$
    select 'apply_aborted';
$body$ language sql;

create or replace procedure pgpro_redefinition._set_config_status(
    configuration_name       varchar(63)
,   status                   text
)  as
$body$
declare
    old_status          text;
begin
    select rt.status into old_status
      from pgpro_redefinition.redef_table rt
     where rt.configuration_name = _set_config_status.configuration_name;

    update  pgpro_redefinition.redef_table
      set status = _set_config_status.status
    where redef_table.configuration_name = _set_config_status.configuration_name;

    call pgpro_redefinition._info(
        errcode                 => 'DFM35'
    ,   message                 => format('Status of configuration %1$s : old status %2$s, new status %3$s'
                                        ,   _set_config_status.configuration_name
                                        ,   old_status
                                        ,   _set_config_status.status
                                    )
    );

    return ;
end
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._set_apply_status(
    configuration_name       varchar(63)
,   status                   text
)  as
$body$
declare
    old_status          text;
begin
    select rt.status_apply into old_status
      from pgpro_redefinition.redef_table rt
     where rt.configuration_name = _set_apply_status.configuration_name;

    update pgpro_redefinition.redef_table
       set status_apply = _set_apply_status.status
     where redef_table.configuration_name = _set_apply_status.configuration_name;

    call pgpro_redefinition._info(
        errcode                 => 'DFM36'
    ,   message                 => format('Apply status of configuration %1$s : old status %2$s, new status %3$s'
                                        ,   _set_apply_status.configuration_name
                                        ,   old_status
                                        ,   _set_apply_status.status
                                    )
    );
end
$body$ language plpgsql;

create or replace procedure pgpro_redefinition._set_redef_status(
    configuration_name       varchar(63)
,   status                   text
)  as
$body$
declare
    old_status          text;
begin
    select rt.status_redef into old_status
      from pgpro_redefinition.redef_table rt
     where rt.configuration_name = _set_redef_status.configuration_name;

    update pgpro_redefinition.redef_table
       set status_redef = _set_redef_status.status
     where redef_table.configuration_name = _set_redef_status.configuration_name;

    call pgpro_redefinition._info(
        errcode                 => 'DFM37'
    ,   message                 => format('Copy status of configuration %1$s : old status %2$s, new status %3$s'
                                        ,   _set_redef_status.configuration_name
                                        ,   old_status
                                        ,   _set_redef_status.status
                                    )
    );
end
$body$ language plpgsql;