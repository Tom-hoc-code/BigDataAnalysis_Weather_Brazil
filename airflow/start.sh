#!/bin/bash

echo "Waiting for Postgres..."

while ! nc -z postgres 5432; do
  sleep 1
done

echo "Postgres is ready"

airflow db migrate

airflow users create \
    --username admin \
    --password admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com || true

echo "Starting Airflow"

airflow scheduler &

exec airflow webserver --port 8080