PGPASSWORD=postgres  pgbench -U postgres \
  -p 5433 -h 192.168.21.160 -f 00.1.prepare_ins_data.sql \
  -T 1000 -R 30 -j 4 -c 4  -P 1 postgres
