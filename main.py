import utils.forecast as forecast
import utils.func as func
import time
import logging

# Configure logging so you can see what's happening
logging.basicConfig(
    filename='service.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)


def run_task():
    """
    This is your main logic (scraping, data processing, etc).
    It MUST return the delay for the next run.
    """
    print("Running the main task...")

    forecast.get_forecast("1480m")
    forecast.get_forecast("1800m")
    forecast.get_forecast("2248m")
    forecast.update_snow_history()
    wait_time = forecast.get_min_time_until_update()

    print(f"Task complete. Requesting next run in {wait_time} minutes.")

    # Return the value so launcher.py can catch it
    return wait_time


# This block allows you to still run 'python main.py' manually for testing
if __name__ == "__main__":
    run_task()