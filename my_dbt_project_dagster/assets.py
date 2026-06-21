import re
from typing import Any, Mapping, Optional

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from .project import my_dbt_project


def _slugify_group(name: str) -> str:
    """Dagster group names must be alphanumeric/underscore only."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """Maps the dbt DAG (models/staging, models/dim, models/FCT, models/Marts,
    seeds, snapshots, sources) onto sensible Dagster asset groups so the
    Dagster UI mirrors the dbt project layout instead of dumping everything
    into one 'default' group."""

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        resource_type = dbt_resource_props.get("resource_type")
        fqn = dbt_resource_props.get("fqn", [])

        if resource_type == "source":
            # Note: dbt sources show up as upstream "external" stub nodes in
            # the asset graph (they aren't materializable via dbt build), so
            # this group is mostly cosmetic for lineage browsing.
            return "raw_airbnb_sources"
        if resource_type == "seed":
            return "seeds"
        if resource_type == "snapshot":
            return "snapshots"

        # fqn[0] is the dbt project name; fqn[1] is the subfolder under
        # models/ (staging, dim, FCT, Marts) when one exists.
        if len(fqn) > 2:
            return _slugify_group(fqn[1])

        return "models_root"

    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
        tags = dict(super().get_tags(dbt_resource_props))
        materialization = dbt_resource_props.get("config", {}).get("materialized")
        if materialization:
            tags["materialization"] = materialization
        return tags


@dbt_assets(
    manifest=my_dbt_project.manifest_path,
    project=my_dbt_project,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(
            enable_asset_checks=True,  # dbt tests -> Dagster asset checks
            enable_code_references=True,
        )
    ),
)
def my_dbt_project_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    # `dbt build` runs seeds -> snapshots -> models -> tests in correct
    # dependency order in one pass, and streams each step back as a
    # Dagster asset materialization / asset check result.
    yield from dbt.cli(["build"], context=context).stream()
