from pathlib import Path

# This dagster project is expected to live as a SIBLING directory to the
# dbt project, i.e.:
#
#   my_dbt_project/                <- the dbt project (this repo)
#   dagster_orchestration/         <- this package's parent
#     my_dbt_project_dagster/
#       constants.py               <- you are here
#
# If you place this folder somewhere else, just update DBT_PROJECT_DIR.
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "my_dbt_project").resolve()

# Where dagster-dbt stores the packaged manifest.json / compiled artifacts
# when this code location is deployed (e.g. built into a Docker image).
DBT_PACKAGED_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt-project").resolve()
