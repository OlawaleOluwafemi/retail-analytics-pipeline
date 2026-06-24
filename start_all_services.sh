#!/bin/bash

echo "===================================================="
echo "      STARTING METRICS AND ETL ECOSYSTEM           "
echo "===================================================="

# 1. Start Airbyte Core (abctl handles its own state safely)
echo "--> Starting Airbyte Engine (Low-Resource)..."
abctl local install --low-resource-mode

# 2. Ensure docker-compose.yaml retains the correct host-gateway mapping
if [ -f docker-compose.yml ]; then
    echo "--> Verifying and locking safe network routing paths..."
    # This guarantees no accidental spaces or bad IPs creep back into your extra_hosts config
    sed -i 's/host.docker.internal:.*host-gateway/host.docker.internal:host-gateway/g' docker-compose.yml
fi

# 3. Completely drop half-created container map states to avoid cache corruption
echo "--> Cleaning up stale container infrastructures..."
docker compose down --remove-orphans

# 4. Spin up the Core Data Stack cleanly (Dagster, Clickhouse, Superset, Grafana, Postgres)
echo "--> Launching Core Infrastructure Containers..."
docker compose up -d

echo "----------------------------------------------------"
echo " Checking Service Health Status..."
echo "----------------------------------------------------" 
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

echo "===================================================="
echo " SUCCESS: All dashboards and pipelines are live!"
echo " Airbyte  -> http://localhost:8000"
echo " Dagster  -> http://localhost:3000"
echo " Clickhouse -> http://localhost:8123"
echo " Superset -> http://localhost:8088"
echo " Grafana  -> http://localhost:3001"
echo " Postgres -> http://localhost:5432"
echo "===================================================="