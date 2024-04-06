--  PGPASSWORD=postgres psql -f install.sql -h 192.168.21.160 -U postgres
\set ON_ERROR_STOP on

drop schema if exists pgpro_redefinition cascade;
create schema if not exists pgpro_redefinition;

\ir common.const.sql
\ir common.tabl.sql
\ir common.func.sql
\ir common.log.sql
\ir common.view.sql
\ir common.status.sql

\ir deferred.func.sql
\ir weight.func.sql
