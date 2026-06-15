#!/bin/bash
set -e

if [ -s "$PGDATA/PG_VERSION" ]; then
  echo "Replica data directory already initialized, skipping pg_basebackup."
  exit 0
fi

echo "Waiting for primary to be ready..."

until PGPASSWORD=repl_password pg_isready -h pg-primary -p 5432 -U replicator; do
  echo "Primary is not ready yet..."
  sleep 2
done

echo "Running pg_basebackup from pg-primary..."

rm -rf "$PGDATA"/*

PGPASSWORD=repl_password pg_basebackup \
  -h pg-primary \
  -p 5432 \
  -U replicator \
  -D "$PGDATA" \
  -Fp \
  -Xs \
  -P \
  -R

echo "pg_basebackup completed."