#!/usr/bin/env bash
set -euo pipefail

: "${PATRONI_NAME:?PATRONI_NAME is required}"
: "${PATRONI_ETCD_HOSTS:=etcd:2379}"
: "${PATRONI_DATA_DIR:=/var/lib/postgresql/data}"
: "${PATRONI_POSTGRES_CONNECT_ADDRESS:=${PATRONI_NAME}}"
: "${PATRONI_REST_CONNECT_ADDRESS:=${PATRONI_NAME}}"

mkdir -p "$PATRONI_DATA_DIR" /var/run/postgresql
chown -R postgres:postgres "$PATRONI_DATA_DIR" /var/run/postgresql
chmod 775 /var/run/postgresql

sed \
    -e "s|{{PATRONI_NAME}}|${PATRONI_NAME}|g" \
    -e "s|{{PATRONI_ETCD_HOSTS}}|${PATRONI_ETCD_HOSTS}|g" \
    -e "s|{{PATRONI_DATA_DIR}}|${PATRONI_DATA_DIR}|g" \
    -e "s|{{PATRONI_POSTGRES_CONNECT_ADDRESS}}|${PATRONI_POSTGRES_CONNECT_ADDRESS}|g" \
    -e "s|{{PATRONI_REST_CONNECT_ADDRESS}}|${PATRONI_REST_CONNECT_ADDRESS}|g" \
    /etc/patroni/patroni.yml.template > /tmp/patroni.yml

chown postgres:postgres /tmp/patroni.yml

exec gosu postgres /opt/patroni/bin/patroni /tmp/patroni.yml
