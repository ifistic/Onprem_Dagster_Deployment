from pathlib import Path

# dbt project lives inside this package folder
DBT_PROJECT_DIR = Path(__file__).resolve().parent

# in Dagster Cloud / packaged builds, same location is used
DBT_PACKAGED_PROJECT_DIR = Path(__file__).resolve().parent
