import dagster as dg

from .assets import my_dbt_project_dbt_assets
from .resources import dbt_resource, slack_resource, snowflake_resource
from .schedules import schedules
from .schema_drift import dbt_schema_drift_job
from .sensors import sensors
from .source_freshness import dbt_source_freshness_job

defs = dg.Definitions(
    assets=[my_dbt_project_dbt_assets],
    jobs=[dbt_source_freshness_job, dbt_schema_drift_job],
    resources={
        "dbt": dbt_resource,
        "snowflake": snowflake_resource,
        "slack": slack_resource,
    },
    schedules=schedules,
    sensors=sensors,
)
