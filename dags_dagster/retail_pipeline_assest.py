import os
import time
import requests
import urllib3
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from dagster import (
    Definitions,
    job,
    op,
    In,
    Out,
    Nothing,
    Output,
    ScheduleDefinition,
    OpExecutionContext
)
from dagster_dbt import DbtCliResource, dbt_assets

# Suppress self-signed cert warnings from local abctl instances
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

DBT_PROJECT_DIR = "/opt/dagster/transform_dbt"

# Validate required env vars at container startup
_REQUIRED_ENV = [
    "AIRBYTE_BASE_URL",
    "AIRBYTE_USERNAME",
    "AIRBYTE_PASSWORD",
    "AIRBYTE_CONNECTION_ID"
]
for _var in _REQUIRED_ENV:
    if not os.getenv(_var):
        raise EnvironmentError(f"Missing required environment variable: {_var}")


# ── 1. dbt Assets Definition ───────────────────────────────────────────────────
@dbt_assets(manifest=os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"))
def dbt_transformation_assets(context, dbt: DbtCliResource):
    """Bronze → Silver → Gold transformations inside ClickHouse"""
    yield from dbt.cli(["run"], context=context).stream()


# ── 2. Airbyte Sync Op ─────────────────────────────────────────────────────────
@op(
    tags={"compute_kind": "dbt"},
    ins={"upstream_completed": In(Nothing)}
)
def trigger_airbyte_sync_op(context: OpExecutionContext):
    """Triggers MySQL/Postgres → ClickHouse Bronze replication via Airbyte OSS"""

    connection_id = os.getenv("AIRBYTE_CONNECTION_ID")
    base_url      = os.getenv("AIRBYTE_BASE_URL")
    username      = os.getenv("AIRBYTE_USERNAME")
    password      = os.getenv("AIRBYTE_PASSWORD")

    auth    = HTTPBasicAuth(username, password)
    headers = {"Content-Type": "application/json"}

    context.log.info(f"Triggering Airbyte sync for connection: {connection_id}")

    response = requests.post(
        f"{base_url}/connections/sync",
        json={"connectionId": connection_id},
        headers=headers,
        auth=auth,
        verify=False
    )

    # Handle concurrent run conflicts elegantly
    if response.status_code == 409:
        context.log.warning(
            f"Airbyte connection {connection_id} is already running an active sync task. "
            "Skipping trigger invocation and proceeding straight to sleep buffer."
        )
    elif response.status_code not in [200, 201]:
        raise Exception(
            f"Airbyte sync failed — "
            f"Status: {response.status_code} | Body: {response.text}"
        )
    else:
        job_id = response.json().get("job", {}).get("id")
        context.log.info(f"Sync triggered successfully. Airbyte Job ID: {job_id}")

    # Safe time-buffer sleep tracking to prevent API read permission blocks
    sleep_duration = 60
    context.log.info(f"Waiting for {sleep_duration} seconds to allow extraction pipelines to land...")
    time.sleep(sleep_duration)

    yield Output(None)


# ── 3. dbt Execution Op ────────────────────────────────────────────────────────
@op(
    tags={"compute_kind": "dbt"},
    ins={"upstream_completed": In(Nothing)}
)

def run_dbt_transformations_op(context: OpExecutionContext, dbt: DbtCliResource):
    """Runs dbt models after Airbyte sync completes safely"""
    context.log.info("Starting dbt transformations...")
    
    invocation = dbt.cli(["run"])
    invocation.wait()
    
    if invocation.process.returncode != 0:
        raise Exception("dbt run failed. Check dbt logs for model errors.")
    
    context.log.info("dbt complete. Gold analytics layer ready for consumption.")
    yield Output(None)


# ── 4. End-to-End Orchestration Job ──────────────────────────────────────────
@job(resource_defs={"dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR)})
def retail_pipeline_automation_job():
    airbyte_done = trigger_airbyte_sync_op()
    run_dbt_transformations_op(upstream_completed=airbyte_done)


# ── 5. Automation Schedule ────────────────────────────────────────────────────
retail_pipeline_schedule = ScheduleDefinition(
    name="hourly_pipeline_sync_schedule",
    job=retail_pipeline_automation_job,
    cron_schedule="0 * * * *",
)


# ── 6. Dagster Deployment Definitions ──────────────────────────────────────────
defs = Definitions(
    assets=[dbt_transformation_assets],
    jobs=[retail_pipeline_automation_job],
    schedules=[retail_pipeline_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    },
)