#!/usr/bin/env bash
set -euo pipefail

export PGPASSWORD="${PATRONI_SUPERUSER_PASSWORD:-postgres}"

until psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -Atc "SELECT 1" >/dev/null 2>&1; do
    sleep 1
done

if ! psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -Atc \
    "SELECT 1 FROM pg_database WHERE datname = 'ecommerce_ods'" | grep -q 1; then
    psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -v ON_ERROR_STOP=1 \
        -c "CREATE DATABASE ecommerce_ods"
fi

psql -h 127.0.0.1 -p 5432 -U postgres -d ecommerce_ods -v ON_ERROR_STOP=1 \
    -f /docker-entrypoint-initdb.d/01-init-primary.sql
