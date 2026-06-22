from pathlib import Path

# dbt project is bundled alongside the Dagster package in the repo
DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "my_dbt_project"
DBT_PACKAGED_PROJECT_DIR = Path(__file__).resolve().parent / "dbt-project"
