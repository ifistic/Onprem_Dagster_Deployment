# Dagster orchestration for `my_dbt_project`

Orchestrates the `ifiok_dbt_proj` dbt project (Snowflake, Airbnb-style
listings/hosts/reviews data) with Dagster: full asset lineage, scheduled
`dbt build` runs, independent source-freshness checks, and Slack alerting
on failure.

## ⚠️ Do this first: rotate your Snowflake password

`my_dbt_project/profiles.yml` currently has a real Snowflake password
committed in plaintext to a public GitHub repo. That credential should be
considered compromised. Rotate it in Snowflake now, then use the
`profiles.yml` included in this folder (env-var based) to replace the
committed one, and add `.env` to `.gitignore`. None of the code here ever
reads or writes that password — it all goes through `SNOWFLAKE_PASSWORD`.

## Layout

This is designed to sit as a **sibling directory** to the dbt project:

```
my_dbt_project/              <- existing dbt project (unchanged, except profiles.yml)
dagster_orchestration/       <- this folder
  my_dbt_project_dagster/
    project.py                 DbtProject pointing at ../../my_dbt_project
    assets.py                  @dbt_assets: one multi_asset running `dbt build`,
                                grouped to mirror models/staging, models/dim,
                                models/FCT, models/Marts, seeds, snapshots
    source_freshness.py        op/job running `dbt source freshness`
                                (honors warn_after/error_after in source.yml)
    resources.py                DbtCliResource, profiles dir from env
    schedules.py                daily full build (06:00 UTC) + hourly freshness check
    sensors.py                  Slack alert on any run failure
    definitions.py               wires it all together
  pyproject.toml / requirements.txt
  dagster.yaml.postgres-example  optional Postgres-backed instance (rename to
                                  dagster.yaml for an always-on deployment)
  deploy/                        systemd units for webserver + daemon
  .env.example
  profiles.yml                   secure replacement for the committed one
```

## Local setup

```bash
cd dagster_orchestration
python3 -m venv venv && source venv/bin/activate
pip install -e .                      # or: pip install -r requirements.txt

cp .env.example .env                  # fill in SNOWFLAKE_PASSWORD, SLACK_BOT_TOKEN, etc.
set -a; source .env; set +a

# replace the committed profiles.yml with the env-var based one:
cp profiles.yml ../my_dbt_project/profiles.yml

cd ../my_dbt_project && dbt deps      # installs dbt_utils, dbt_external_tables, etc.
cd ../dagster_orchestration

dagster dev -m my_dbt_project_dagster.definitions
```

Open http://localhost:3000 — you'll see the asset graph grouped as
`staging → dim → fct/marts`, plus `seeds` and `snapshots`, with the dbt
tests on `dim_listings` (the `relationships` test in `my_test.yml`) showing
as asset checks.

## What's running on a schedule

- **`daily_dbt_build_schedule`** (06:00 UTC) — `dbt build`: seeds →
  `scd_raw_listings` snapshot → staging (`AnB_raw_listing`, `raw_host`,
  `src_reviews_model`) → `dim_*` → `FCT_reviews` → `full_moon_Joint_review`
  → tests, in correct dependency order, in one run.
- **`source_freshness_check_schedule`** (hourly) — `dbt source freshness`
  against `airbnb.listings` / `airbnb.hosts` / `airbnb.reviews`, using the
  exact `warn_after`/`error_after` thresholds already defined in
  `models/source.yml`. Fails the run if any source is past its error
  threshold, independent of whether a build happens to be running.

Both default to `RUNNING` on deploy; flip `default_status` in
`schedules.py` to `STOPPED` if you'd rather turn them on manually in the UI.

## Alerting

`sensors.py` registers `slack_on_run_failure`, which posts to
`#data-pipeline-alerts` on any failed run across the code location.
Requires `SLACK_BOT_TOKEN` (a Slack bot token with `chat:write` scope,
invited to that channel).

## Customizing the build selection

`daily_dbt_build_schedule` uses `dbt_select="fqn:*"` (everything). To
target a subset, e.g. only the reviews lineage:

```python
dbt_select="fqn:src_reviews_model+"
```

or add a second schedule with a narrower selection for `dim_reviwe` /
`FCT_reviews` on a tighter cadence than the rest of the marts.

## Production deployment (matches the systemd pattern used elsewhere)

1. Rename `dagster.yaml.postgres-example` → `dagster.yaml`, point
   `DAGSTER_HOME` at this directory, and set `DAGSTER_PG_*` env vars for a
   real Postgres instance (run/event-log storage; required for the daemon
   and webserver to share state reliably on a long-running box).
2. Install the two unit files in `deploy/` to `/etc/systemd/system/`,
   adjusting `User=`/paths to match your install location, then:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now dagster-daemon dagster-webserver
   ```
3. The daemon is what actually fires `schedules.py` and `sensors.py` —
   the webserver alone won't trigger scheduled runs.
# Onprem_Dagster_Deployment
