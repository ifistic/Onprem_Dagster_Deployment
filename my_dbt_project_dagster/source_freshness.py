import dagster as dg
from dagster_dbt import DbtCliResource

# dbt sources aren't materializable nodes, so `@dbt_assets` (which only
# builds specs for models/seeds/snapshots/tests) can't turn the
# warn_after/error_after blocks in models/source.yml into asset checks
# directly. Instead this runs `dbt source freshness` as its own op/job,
# which honors those exact thresholds (RAW_LISTINGS, raw_hosts, raw_reviews)
# and fails the run on hard staleness.


@dg.op(required_resource_keys={"dbt"})
def dbt_source_freshness_op(context: dg.OpExecutionContext):
    dbt: DbtCliResource = context.resources.dbt
    invocation = dbt.cli(["source", "freshness"], context=context, raise_on_error=False)
    invocation.wait()

    results = invocation.get_artifact("sources.json").get("results", [])
    has_error = False
    for result in results:
        status = result.get("status")
        context.log.info(
            f"{result.get('unique_id')}: status={status} "
            f"max_loaded_at={result.get('max_loaded_at')}"
        )
        if status == "error":
            has_error = True

    if has_error:
        raise dg.Failure(
            description=(
                "dbt source freshness reported one or more sources past their "
                "error_after threshold -- see op logs for which source(s)."
            )
        )


@dg.job(name="dbt_source_freshness_job", description="Runs `dbt source freshness` against the airbnb sources.")
def dbt_source_freshness_job():
    dbt_source_freshness_op()
