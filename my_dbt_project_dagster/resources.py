import os
from dagster_dbt import DbtCliResource
from dagster_snowflake import SnowflakeResource
from dagster_slack import SlackResource
import dagster as dg
from .project import my_dbt_project

_profiles_dir = os.getenv("DBT_PROFILES_DIR")

dbt_resource = DbtCliResource(
    project_dir=my_dbt_project,
    **( {"profiles_dir": _profiles_dir} if _profiles_dir else {} ),
)

snowflake_resource = SnowflakeResource(
    account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
    user=dg.EnvVar("SNOWFLAKE_USER"),
    password=dg.EnvVar("SNOWFLAKE_PASSWORD"),
    role=dg.EnvVar("SNOWFLAKE_ROLE"),
    database=dg.EnvVar("SNOWFLAKE_DATABASE"),
    warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE"),
    schema_=dg.EnvVar("SNOWFLAKE_SCHEMA"),
)

slack_resource = SlackResource(token=dg.EnvVar("SLACK_BOT_TOKEN"))
