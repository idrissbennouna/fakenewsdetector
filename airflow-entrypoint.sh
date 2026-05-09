#!/bin/bash
set -e

echo "=== Waiting for PostgreSQL ==="
until airflow db check; do
  echo "PostgreSQL not ready, retrying in 5s..."
  sleep 5
done

echo "=== Migrating Airflow DB ==="
airflow db migrate

echo "=== Creating admin user ==="
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname EMSI \
  --role Admin \
  --email admin@gmail.com || true

echo "=== Starting Scheduler ==="
airflow scheduler &

echo "=== Starting Webserver ==="
exec airflow webserver --workers 2 --worker-timeout 120