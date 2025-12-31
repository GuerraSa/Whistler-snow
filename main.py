import utils.forecast as forecast
import time
import sys


def run_task():
    """
    This is your main logic (scraping, data processing, etc).
    It MUST return the delay for the next run (in minutes).
    """
    # flush=True forces the print to appear immediately in logs
    print("Running the main task...", flush=True)

    # Run your scrapes
    forecast.get_forecast("1480m")
    forecast.get_forecast("1800m")
    forecast.get_forecast("2248m")

    # Update history
    forecast.update_snow_history()

    # Calculate next run time
    wait_time = forecast.get_min_time_until_update()

    print(f"Task complete. Requesting next run in {wait_time} minutes.", flush=True)

    # Return the value so adaptive_service.py can catch it
    return wait_time


# This block allows you to still run 'python main.py' manually for testing
if __name__ == "__main__":
    run_task()