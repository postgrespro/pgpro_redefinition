#!/bin/bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"

export PGDATABASE=postgres
export PGHOST=192.168.21.160
export PGPORT=5433
export PGUSER=postgres
export PGPASSWORD=postgres
export ON_ERROR_STOP=true

echo "Prepare"
psql -f 00.prepare.sql
sleep 1

echo 'Register'
psql --set=ON_ERROR_STOP=on -f 10.reg.sql
sleep 1

echo "Start"
psql --set=ON_ERROR_STOP=on -f 20.start_capture_data.sql
sleep 10

echo "Redef"
psql --set=ON_ERROR_STOP=on -f 30.start_redef_table.sql
sleep 10

echo "Finish"
psql --set=ON_ERROR_STOP=on -f 60.finish.sql

echo "Finish test"
