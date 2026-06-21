from dagster_dbt import DbtProject

from .constants import DBT_PACKAGED_PROJECT_DIR, DBT_PROJECT_DIR

my_dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    packaged_project_dir=DBT_PACKAGED_PROJECT_DIR,
)

# In dev (`dagster dev`), this re-parses the dbt project and regenerates
# manifest.json on every code reload. In a deployed/packaged context it
# is a no-op and dagster reads the manifest baked in at build time.
my_dbt_project.prepare_if_dev()
