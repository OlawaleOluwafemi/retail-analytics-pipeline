import os
import requests
import time
from dotenv import load_dotenv
# Added OpExecutionContext to the imports here
from dagster import Definitions, job, op, In, Nothing, ScheduleDefinition, OpExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# Load environmental configurations
load_dotenv()

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "transform_dbt")

# 1. Standalone dbt assets scan (Keep this as AssetExecutionContext or remove type hints)
@dbt_assets(manifest=os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"))
def dbt_transformation_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()


# 2. Airbyte execution Op
@op(tags={"compute_kind": "airbyte"})
def trigger_airbyte_sync_op():
    """Triggers the raw transaction data replication bridge across to ClickHouse"""
    connection_id = os.getenv("AIRBYTE_CONNECTION_ID")
    airbyte_url = "http://localhost:8000/api/v1/connections/sync"
    
    airbyte_user = os.getenv("AIRBYTE_USERNAME", "airbyte")
    airbyte_pass = os.getenv("AIRBYTE_PASSWORD", "password")
    
    payload = {"connectionId": connection_id}
    
    print(f"Sending Authenticated POST request to trigger Airbyte connection: {connection_id}")
    response = requests.post(airbyte_url, json=payload, auth=(airbyte_user, airbyte_pass))
    
    if response.status_code != 200:
        raise Exception(f"Failed to trigger Airbyte sync. Status code: {response.status_code}, Response: {response.text}")
    
    print("Airbyte sync triggered successfully! Waiting for data to land...")
    time.sleep(10)


# 3. dbt execution Op (Updated to OpExecutionContext)
@op(
    tags={"compute_kind": "dbt"},
    ins={"upstream_completed": In(Nothing)}
)
def run_dbt_transformations_op(context: OpExecutionContext, dbt: DbtCliResource):
    """Executes the downstream dbt transformations inside ClickHouse"""
    dbt.cli(["run"], context=context).wait()


# 4. Sequential Job Sequence
@job
def retail_pipeline_automation_job():
    airbyte_run = trigger_airbyte_sync_op()
    run_dbt_transformations_op(upstream_completed=airbyte_run)


# 5. Automated hourly cron schedule
retail_pipeline_schedule = ScheduleDefinition(
    name="hourly_pipeline_sync_schedule",
    job=retail_pipeline_automation_job,
    cron_schedule="0 * * * *", 
)

# 6. Final Definitions
defs = Definitions(
    assets=[dbt_transformation_assets],
    jobs=[retail_pipeline_automation_job],
    schedules=[retail_pipeline_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    },
)