#!/bin/bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"

export PGDATABASE=postgres
export PGHOST=192.168.21.160
export PGPORT=5433
export PGUSER=postgres
export PGPASSWORD=postgres

echo "Prepare"
psql --set=ON_ERROR_STOP=on -f 00.prepare.sql
sleep 1

echo 'Register'
psql --set=ON_ERROR_STOP=on -f 01.reg.sql
sleep 1

echo "start_capture_data"
psql --set=ON_ERROR_STOP=on -f 02.start_capture_data.sql
sleep 10

echo "start_copy"
psql --set=ON_ERROR_STOP=on -f 14.start_redef.sql
sleep 1

echo "start_apply_mlog"
psql --set=ON_ERROR_STOP=on -f 24.start_apply_mlog.sql
sleep 1

sleep 30

echo "finish"
psql --set=ON_ERROR_STOP=on -f 60.finish.sql

echo "Finish test"

