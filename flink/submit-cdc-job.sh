#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

log_file="$(mktemp)"
trap 'rm -f "$log_file"' EXIT

sql-client.sh -f /opt/flink/jobs/postgres_to_clickhouse.sql 2>&1 | tee "$log_file"

# Flink SQL Client can return zero even when an individual statement failed.
# Turn that misleading success into a failing one-shot Compose service.
if grep -q '\[ERROR\]' "$log_file"; then
  echo "Flink SQL job submission failed." >&2
  exit 1
fi
