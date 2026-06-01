#!/bin/bash

echo "Booting FinStream Medallion Architecture..."

echo "Creating data directories..."
mkdir -p data/bronze data/silver data/gold

echo "Setting host permissions for Docker containers..."
# This ensures PySpark and DuckDB can write files from inside Docker
sudo chmod -R 777 data/
sudo chmod -R 777 gold_analytics/
sudo chmod -R 777 gx/


echo "Starting the Message Broker (Kafka)..."
docker compose -f docker/docker-compose.yml up -d

echo "Starting the Orchestrator (Airflow)..."
docker compose -f docker/airflow-compose.yml up -d

echo "Starting the Telemetry Stack (Grafana/Prometheus)..."
docker compose -f docker/observability-compose.yml up -d

echo "Pipeline Infrastructure is Live!"
echo "Airflow UI: http://localhost:8080"
echo "Grafana UI: http://localhost:3000"

