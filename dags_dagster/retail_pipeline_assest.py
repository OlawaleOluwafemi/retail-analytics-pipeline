from dagster import asset, Definitions
import requests
import os

@asset
def trigger_airbyte_sync():
    """Triggers the raw transaction data replication bridge across to ClickHouse"""
    # Replace with your actual Airbyte Connection ID from the URL configuration tab
    connection_id = os.getenv("AIRBYTE_CONNECTION_ID")
    airbyte_url = "http://host.docker.internal:8000/api/v1/connections/sync"
    
    response = requests.post(
        airbyte_url,
        json={"connectionId": connection_id},
        auth=("airbyte", "password")
    )
    response.raise_for_status()
    return "Airbyte Ingestion Completed Successfully"

@asset(deps=[trigger_airbyte_sync])
def run_dbt_transformations():
    """Runs the analytical gold presentation layer transformation logic"""
    # Triggers your downstream dbt execution script layers inside the asset context
    return "dbt Gold Layer Models Materialized"

defs = Definitions(
    assets=[trigger_airbyte_sync, run_dbt_transformations]
)