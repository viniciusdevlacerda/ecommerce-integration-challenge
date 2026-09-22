#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

# Usuario fixo (admin/admin) em vez da senha aleatoria do 'airflow standalone':
# quem for avaliar precisa conseguir entrar sem caçar senha em log.
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.local 2>/dev/null || true

mkdir -p "${BULK_DIR:-/var/opt/mssql/bulk}"

airflow scheduler &
exec airflow webserver
