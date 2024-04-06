create or replace procedure pgpro_redefinition._start_general_apply_data(
    job_type    pgpro_redefinition.job_type default pgpro_redefinition._job_type_apply_data()
,   count        integer default 100
) as
$body$
declare
    configuration_row           pgpro_redefinition.redef_table;
    conf_id                     bigint;
    template_proc               text default 'call %1$I.%2$I();';
    proc                        text;
    self_job_id                 bigint;
begin
    for i in 1 .. _start_general_apply_data.count loop
        begin autonomous
            proc := null;
            WITH sum_weight AS (
                SELECT random() * (SELECT SUM(weight) FROM pgpro_redefinition.redef_weight) as sum_weight
            )
            , all_weight_conf as (
                SELECT rw.config_id, SUM(weight) OVER (ORDER BY config_id) sum_config, sum_weight
                FROM pgpro_redefinition.redef_weight rw CROSS JOIN sum_weight
                where rw.job_type = _start_general_apply_data.job_type
            )
            select c.config_id into conf_id
            from all_weight_conf c
            join pgpro_redefinition.redef_weight rr on rr.config_id = c.config_id
            where c.sum_config >= (select sum_weight from sum_weight)
            order by c.config_id
            for update skip locked
            limit 1;

            if conf_id is null then
                continue ;
            end if;
            select * into configuration_row
              from pgpro_redefinition.redef_table rt
             where rt.id = conf_id;

            proc := format(
                    template_proc
                ,   configuration_row.apply_proc_schema_name
                ,   configuration_row.apply_proc_name
                );

            execute proc;
        end;
        begin autonomous
            self_job_id := schedule.get_self_id();
            if not exists(
                select *
                  from pgpro_redefinition.general_jobs gj
                 where gj.job_type = _start_general_apply_data.job_type
                   and job_id = self_job_id
            ) then
                perform schedule.cancel_job(job_id => self_job_id);
            end if;
        exception
            WHEN SQLSTATE 'XX000' THEN
                call pgpro_redefinition._info(
                    errcode         => 'DFM77'
                ,   message         => format('Function started not in scheduler')
                );
            continue ;
        end;
    end loop;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.start_general_jobs(
    job_type pgpro_redefinition.job_type
) as
$body$
declare
    schedule_query                      text default 'call %1$I.%2$I(); select schedule.resubmit()';
    schedule_name                       text default 'general_job__%1$s__%2$s';
    schedule_comments                   text default 'pgpro_redefinition - general job %1$s № %2$s';
    resubmit_limit                      bigint default 1000000000;
    schedule_job_id                     bigint;
    njob                                bigint  default 1;
begin
    call pgpro_redefinition._check_pgpro_scheduler();
    if start_general_jobs.job_type = pgpro_redefinition._job_type_apply_data() then
        njob := pgpro_redefinition._default_jobs_apply_count();
    elseif start_general_jobs.job_type = pgpro_redefinition._job_type_redef_data() then
        njob := pgpro_redefinition._default_jobs_redef_count();
    else
        call pgpro_redefinition._debug(
            errcode         => 'DFE53'
        ,   message         => format('job_type % not support'''
                                    ,    job_type::text
                               )
        );
    end if;

    for i in 1 ..njob loop
        schedule_query := format(
            schedule_query
        ,   pgpro_redefinition._default_schema()
        ,   '_start_general_apply_data'
        );
        schedule_name := format(
            schedule_name
        ,   start_general_jobs.job_type
        ,   njob::text
        );
        schedule_comments := format(
            schedule_comments
        ,   njob::text
        ,   njob::text
        );
         schedule_job_id := schedule.submit_job(
            query               => schedule_query
        ,   name                => schedule_name
        ,   comments            => schedule_comments
        ,   resubmit_limit      => resubmit_limit
        );
        insert into pgpro_redefinition.general_jobs(
            job_type
        ,   job_id
        )values(
            start_general_jobs.job_type
        ,   schedule_job_id
        );
    end loop;
end;
$body$ language plpgsql;

create or replace procedure pgpro_redefinition.stop_general_jobs(
    job_type pgpro_redefinition.job_type
) as
$body$
declare
begin
    delete from pgpro_redefinition.general_jobs gj
    where gj.job_type = stop_general_jobs.job_type;
end;
$body$ language plpgsql;
