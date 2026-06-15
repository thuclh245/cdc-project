#!/bin/bash
set -e

echo "host replication replicator 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"
echo "host all all 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"

cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses = '*'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
EOF