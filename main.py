import traceback

from jobs import lifts, weather, history, conditions


def _run(job, *args):
    """Runs one job; a failure is printed and the other jobs still run."""
    try:
        return job(*args)
    except Exception:
        traceback.print_exc()
        return None


def run_all_tasks():
    """Runs every job. Returns minutes until the next forecast update, or None if unknown."""
    print("🚀 Starting All Tasks...", flush=True)

    # 1. Static Data
    _run(lifts.sync_lift_info)
    _run(history.update_snow_history)

    # 2. Conditions (Webcams/AI)
    _run(conditions.sync_conditions)

    # 3. Forecasts (the 1480m page also tells us when the next update is)
    wait_minutes = _run(weather.update_forecast, "1480m")
    _run(weather.update_forecast, "1800m")
    _run(weather.update_forecast, "2248m")

    return wait_minutes


if __name__ == "__main__":
    run_all_tasks()
