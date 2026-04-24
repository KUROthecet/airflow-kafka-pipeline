#!/bin/bash
set -e

echo "Setting up Airflow Pipeline Project..."

# Setup directories with correct permissions
mkdir -p dags plugins logs config staging
mkdir -p staging/archive staging/reports
mkdir -p tests/unit tests/integration docs

# Export AIRFLOW_UID to current user id if not set
if [[ -z "${AIRFLOW_UID}" ]]; then
  export AIRFLOW_UID=$(id -u)
  echo "AIRFLOW_UID set to ${AIRFLOW_UID}"
fi

# Initialize Airflow DB and User
echo "Initializing Airflow (docker-compose up airflow-init)..."
docker-compose up airflow-init

# Import Connections
echo "Importing connections..."
docker-compose run --rm airflow-cli airflow connections import /opt/airflow/config/connections.json || echo "Warning: Connection import failed. Make sure airflow is initialized."

# Import Variables
echo "Importing variables..."
docker-compose run --rm airflow-cli airflow variables import /opt/airflow/config/variables.json || echo "Warning: Variables import failed."

echo "Setup complete! You can now start Airflow with: docker-compose up -d"
