import os
import requests
import time
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from dagster import (
    Definitions, job, op, In, Out, Nothing, Output,
    ScheduleDefinition, OpExecutionContext
)
from dagster_dbt import DbtCliResource, dbt_assets

# Load environmental configurations
load_dotenv()

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "transform_dbt")


# 1. Standalone dbt assets scan
@dbt_assets(manifest=os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"))
def dbt_transformation_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()


# 2. Airbyte execution Op — fixed to use Basic Auth (OSS only)
@op(
    tags={"compute_kind": "airbyte"},
    out=Out(Nothing)  # explicit Nothing output for downstream chaining
)
def trigger_airbyte_sync_op(context: OpExecutionContext):
    """Triggers the raw transaction data replication from PostgresDB to ClickHouse via Airbyte OSS"""

    connection_id = os.getenv("AIRBYTE_CONNECTION_ID")

    # OSS self-hosted Airbyte uses Basic Auth — NOT OAuth client credentials
    # Default credentials unless you changed them in your .env / docker-compose
    airbyte_username = os.getenv("AIRBYTE_USERNAME", "airbyte")
    airbyte_password = os.getenv("AIRBYTE_PASSWORD", "password")

    # Use container name instead of hardcoded IP to avoid Docker bridge mismatch
    # Port 8006 is the Airbyte API server (not the webapp on 8000)
    base_url = os.getenv("AIRBYTE_BASE_URL")

    auth = HTTPBasicAuth(airbyte_username, airbyte_password)
    headers = {"Content-Type": "application/json"}

    # Trigger the sync job
    jobs_url = f"{base_url}/connections/sync"
    payload = {"connectionId": connection_id}

    context.log.info(f"Triggering Airbyte sync for connection: {connection_id}")

    response = requests.post(
        jobs_url,
        json=payload,
        headers=headers,
        auth=auth,
        verify=False
    )

    if response.status_code not in [200, 201]:
        raise Exception(
            f"Airbyte sync trigger failed. "
            f"Status: {response.status_code} | Response: {response.text}"
        )

    job_id = response.json().get("job", {}).get("id")
    context.log.info(f"Sync triggered successfully. Job ID: {job_id}")

    # Poll for job completion instead of a blind sleep
    _wait_for_airbyte_job(base_url, job_id, auth, context)

    yield Output(None)


def _wait_for_airbyte_job(
    base_url: str,
    job_id: str,
    auth: HTTPBasicAuth,
    context: OpExecutionContext,
    poll_interval: int = 10,
    timeout: int = 3600
):
    """Polls Airbyte job status until completion or timeout."""
    elapsed = 0
    status_url = f"{base_url}/jobs/{job_id}"

    while elapsed < timeout:
        resp = requests.get(status_url, auth=auth)
        resp.raise_for_status()

        status = resp.json().get("job", {}).get("status")
        context.log.info(f"Airbyte job {job_id} status: {status} ({elapsed}s elapsed)")

        if status == "succeeded":
            context.log.info("Airbyte sync completed. Data landed in ClickHouse Bronze.")
            return
        elif status in ["failed", "cancelled"]:
            raise Exception(f"Airbyte job {job_id} ended with status: {status}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise Exception(f"Airbyte job {job_id} timed out after {timeout}s")


# 3. dbt execution Op
@op(
    tags={"compute_kind": "dbt"},
    ins={"upstream_completed": In(Nothing)}
)
def run_dbt_transformations_op(context: OpExecutionContext, dbt: DbtCliResource):
    """Executes Bronze → Silver → Gold transformations inside ClickHouse via dbt"""
    context.log.info("Starting dbt transformations...")
    yield from dbt.cli(["run"], context=context).stream()
    context.log.info("dbt transformations complete. Gold layer ready.")


# 4. Sequential Job
@job(
    resource_defs={"dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR)}
)
def retail_pipeline_automation_job():
    airbyte_done = trigger_airbyte_sync_op()
    run_dbt_transformations_op(upstream_completed=airbyte_done)


# 5. Hourly schedule
retail_pipeline_schedule = ScheduleDefinition(
    name="hourly_pipeline_sync_schedule",
    job=retail_pipeline_automation_job,
    cron_schedule="0 * * * *",
)

# 6. Definitions
defs = Definitions(
    assets=[dbt_transformation_assets],
    jobs=[retail_pipeline_automation_job],
    schedules=[retail_pipeline_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    },
)