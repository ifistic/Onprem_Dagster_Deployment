import dagster as dg
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import my_dbt_project_dbt_assets
from .schema_drift import dbt_schema_drift_job
from .source_freshness import dbt_source_freshness_job

daily_dbt_build_schedule = build_schedule_from_dbt_selection(
    [my_dbt_project_dbt_assets],
    job_name="daily_dbt_build",
    cron_schedule="0 6 * * *",
    dbt_select="fqn:*",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

source_freshness_schedule = dg.ScheduleDefinition(
    name="source_freshness_check_schedule",
    job=dbt_source_freshness_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

schema_drift_schedule = dg.ScheduleDefinition(
    name="schema_drift_check_schedule",
    job=dbt_schema_drift_job,
    cron_schedule="0 5 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

schedules = [daily_dbt_build_schedule, source_freshness_schedule, schema_drift_schedule]
