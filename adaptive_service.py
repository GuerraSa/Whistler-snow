import time
import logging
import utils.forecast as forecast
import main

# Configure logging so you can see what's happening
logging.basicConfig(
    filename='service.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)


# This block allows you to still run 'python main.py' manually for testing
if __name__ == "__main__":
    logging.info("Service started.")

    while True:
        # 1. Run task and get dynamic delay
        wait_time = main.run_task()

        # 2. Sleep for that duration
        time.sleep(wait_time)