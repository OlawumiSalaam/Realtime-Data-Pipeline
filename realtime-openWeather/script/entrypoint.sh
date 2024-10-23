#!/bin/bash
set -e

# Upgrade pip
if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install -r /opt/airflow/requirements.txt
fi

# Initialize Airflow database if it doesn't exist
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade the Airflow database
airflow db upgrade

# Start the Airflow webserver
exec airflow webserver
