from jobs import lifts, weather, history, conditions


def run_all_tasks():
    print("🚀 Starting All Tasks...", flush=True)

    # 1. Static Data
    lifts.sync_lift_info()
    history.update_snow_history()

    # 2. Conditions (Webcams/AI)
    conditions.sync_conditions()

    # 3. Forecasts
    weather.update_forecast("1480m")
    weather.update_forecast("1800m")
    weather.update_forecast("2248m")

    # 4. Schedule
    return weather.get_time_until_update()


if __name__ == "__main__":
    run_all_tasks()