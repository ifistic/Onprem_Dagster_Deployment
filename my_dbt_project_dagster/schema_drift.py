"""Detects schema drift on the raw Airbnb source tables (RAW_LISTINGS,
RAW_HOSTS, RAW_REVIEWS) by comparing the live Snowflake schema against a
stored baseline, alerts on Slack when drift is found, and opens a GitHub
PR with dbt-codegen's best-effort regeneration of source.yml for human
review.

Deliberately does NOT auto-commit schema changes to main: a renamed or
type-changed column can silently break downstream joins/tests in ways an
automated diff can't always catch, so the PR step exists to put a human
in the loop before anything ships.
"""

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from dagster_snowflake import SnowflakeResource

from .project import my_dbt_project

SOURCE_DATABASE = "AIRBNB"
SOURCE_SCHEMA = "RAW"
SOURCE_TABLES = ("RAW_LISTINGS", "RAW_HOSTS", "RAW_REVIEWS")
BASELINE_TABLE = f"{SOURCE_DATABASE}.{SOURCE_SCHEMA}._DAGSTER_SCHEMA_BASELINE"

GITHUB_REPO = os.getenv("GITHUB_REPO", "ifistic/my_dbt_project")
SchemaMap = dict[tuple[str, str], tuple[str, int]]


def _fetch_current_schema(conn) -> SchemaMap:
    cur = conn.cursor()
    table_list = ", ".join(f"'{t}'" for t in SOURCE_TABLES)
    cur.execute(f"""
        select table_name, column_name, data_type, ordinal_position
        from {SOURCE_DATABASE}.information_schema.columns
        where table_schema = '{SOURCE_SCHEMA}'
          and table_name in ({table_list})
    """)
    return {(row[0], row[1]): (row[2], row[3]) for row in cur.fetchall()}


def _fetch_baseline_schema(conn) -> SchemaMap:
    cur = conn.cursor()
    cur.execute(f"""
        create table if not exists {BASELINE_TABLE} (
            table_name string,
            column_name string,
            data_type string,
            ordinal_position integer,
            captured_at timestamp_ntz
        )
    """)
    cur.execute(
        f"select table_name, column_name, data_type, ordinal_position from {BASELINE_TABLE}"
    )
    return {(row[0], row[1]): (row[2], row[3]) for row in cur.fetchall()}


def _replace_baseline_schema(conn, schema: SchemaMap) -> None:
    cur = conn.cursor()
    cur.execute(f"truncate table {BASELINE_TABLE}")
    now = datetime.now(timezone.utc)
    rows = [
        (table, column, data_type, ordinal, now)
        for (table, column), (data_type, ordinal) in schema.items()
    ]
    if rows:
        cur.executemany(
            f"insert into {BASELINE_TABLE} "
            f"(table_name, column_name, data_type, ordinal_position, captured_at) "
            f"values (%s, %s, %s, %s, %s)",
            rows,
        )


def _diff_schemas(baseline: SchemaMap, current: SchemaMap):
    added = sorted(k for k in current if k not in baseline)
    removed = sorted(k for k in baseline if k not in current)
    type_changed = sorted(
        k for k in current if k in baseline and current[k][0] != baseline[k][0]
    )
    return added, removed, type_changed


@dg.op(required_resource_keys={"snowflake"})
def detect_schema_drift_op(context: dg.OpExecutionContext) -> dict[str, Any]:
    """Compares the live schema of the raw Airbnb source tables against the
    last-known baseline (stored in Snowflake), logs any drift, and resets
    the baseline to the current schema -- so the next run only reports NEW
    drift rather than re-flagging the same change forever."""
    snowflake: SnowflakeResource = context.resources.snowflake

    with snowflake.get_connection() as conn:
        baseline = _fetch_baseline_schema(conn)
        current = _fetch_current_schema(conn)
        added, removed, type_changed = _diff_schemas(baseline, current)

        if not baseline:
            context.log.info("No baseline schema found yet -- recording current schema as the baseline.")
        elif added or removed or type_changed:
            for table, column in added:
                context.log.warning(f"Schema drift: column ADDED -> {table}.{column} ({current[(table, column)][0]})")
            for table, column in removed:
                context.log.warning(f"Schema drift: column REMOVED -> {table}.{column}")
            for table, column in type_changed:
                old_type, new_type = baseline[(table, column)][0], current[(table, column)][0]
                context.log.warning(f"Schema drift: column TYPE CHANGED -> {table}.{column} ({old_type} -> {new_type})")
        else:
            context.log.info("No schema drift detected.")

        _replace_baseline_schema(conn, current)

    return {
        "added": [f"{t}.{c}" for t, c in added],
        "removed": [f"{t}.{c}" for t, c in removed],
        "type_changed": [f"{t}.{c}" for t, c in type_changed],
    }


@dg.op(required_resource_keys={"slack"})
def notify_schema_drift_op(context: dg.OpExecutionContext, drift: dict[str, Any]) -> dict[str, Any]:
    has_drift = drift["added"] or drift["removed"] or drift["type_changed"]
    if not has_drift:
        context.log.info("No drift to report -- skipping Slack notification.")
        return drift

    lines = ["*Schema drift detected on Airbnb raw source tables:*"]
    if drift["added"]:
        lines.append(f"- Added: {', '.join(drift['added'])}")
    if drift["removed"]:
        lines.append(f"- Removed: {', '.join(drift['removed'])}")
    if drift["type_changed"]:
        lines.append(f"- Type changed: {', '.join(drift['type_changed'])}")

    client = context.resources.slack.get_client()
    client.chat_postMessage(channel="#data-pipeline-alerts", text="\n".join(lines))
    return drift


@dg.op
def open_schema_drift_pr_op(context: dg.OpExecutionContext, drift: dict[str, Any]) -> None:
    """Best-effort: regenerates source.yml via dbt-codegen and opens a PR
    for human review. Requires the `dbt-codegen` package in packages.yml
    and a GITHUB_TOKEN with repo write access. Skips quietly (with a log
    line) if either prerequisite is missing, since drift was already
    reported to Slack regardless."""
    has_drift = drift["added"] or drift["removed"] or drift["type_changed"]
    if not has_drift:
        context.log.info("No drift -- skipping codegen/PR step.")
        return

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        context.log.warning("GITHUB_TOKEN not set -- skipping automatic PR. Drift was still reported to Slack.")
        return

    repo_dir = my_dbt_project.project_dir
    result = subprocess.run(
        [
            "dbt", "run-operation", "generate_source",
            "--args", f"{{schema_name: {SOURCE_SCHEMA}, database_name: {SOURCE_DATABASE}, "
                      f"generate_columns: True, include_descriptions: True}}",
        ],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    if result.returncode != 0:
        context.log.error(f"dbt-codegen failed, skipping PR:\n{result.stderr}")
        return

    generated_yml = "\n".join(
        line for line in result.stdout.splitlines()
        if not (len(line) > 8 and line[2] == ":" and line[5] == ":")
    ).strip()

    branch = f"schema-drift/{context.run_id[:8]}"
    generated_path = Path(repo_dir, "models", "staging", "_generated_source.yml")

    git = lambda *args: subprocess.run(["git", "-C", str(repo_dir), *args], check=True)  # noqa: E731
    git("checkout", "-b", branch)
    generated_path.write_text(generated_yml)
    git("add", str(generated_path))
    git("commit", "-m", f"Auto-generated source.yml after schema drift ({context.run_id[:8]})")
    git("push", "origin", branch)
    git("checkout", "main")

    body = ["Automated PR: schema drift detected on the Airbnb raw source tables.\n"]
    if drift["added"]:
        body.append(f"**Added:** {', '.join(drift['added'])}")
    if drift["removed"]:
        body.append(f"**Removed:** {', '.join(drift['removed'])}")
    if drift["type_changed"]:
        body.append(f"**Type changed:** {', '.join(drift['type_changed'])}")
    body.append(
        "\nReview `models/staging/_generated_source.yml` (best-effort dbt-codegen output) "
        "and manually fold the relevant changes into `source.yml` and any affected staging "
        "models before merging. Do not merge this PR as-is."
    )

    response = requests.post(
        f"https://api.github.com/repos/{GITHUB_REPO}/pulls",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        json={"title": f"Schema drift detected: {context.run_id[:8]}", "head": branch, "base": "main", "body": "\n".join(body)},
        timeout=30,
    )
    response.raise_for_status()
    context.log.info(f"Opened PR for review: {response.json()['html_url']}")


@dg.job(
    name="dbt_schema_drift_job",
    description="Detects schema drift on the raw Airbnb source tables, alerts on Slack, and opens a PR for review.",
)
def dbt_schema_drift_job():
    open_schema_drift_pr_op(notify_schema_drift_op(detect_schema_drift_op()))
