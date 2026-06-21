import dagster as dg
from dagster_slack import make_slack_on_run_failure_sensor

# Set SLACK_BOT_TOKEN in the environment (see .env.example). If it's unset
# this sensor simply won't fire/authenticate -- it won't crash the code
# location, but you should set it before relying on alerting in prod.
slack_on_run_failure_sensor = make_slack_on_run_failure_sensor(
    channel="#data-pipeline-alerts",
    slack_token=dg.EnvVar("SLACK_BOT_TOKEN"),
    monitor_all_code_locations=True,
    default_status=dg.DefaultSensorStatus.RUNNING,
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitor_all_code_locations=True,
    default_status=dg.DefaultSensorStatus.STOPPED,  # flip to RUNNING if you want success pings too
)
def slack_on_run_success_sensor(context: dg.RunStatusSensorContext):
    """Optional: posts a quieter confirmation message on successful builds.
    Disabled by default so Slack isn't noisy -- flip default_status to
    RUNNING above if you want it."""
    return dg.SensorResult()


sensors = [slack_on_run_failure_sensor, slack_on_run_success_sensor]
