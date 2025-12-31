import time
import logging
import sys
import main

# Configure logging to show in Systemd/Journalctl
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]  # Print to console/systemd
)

if __name__ == "__main__":
    logging.info("Service started - Adaptive Scheduler Running")

    while True:
        try:
            # 1. Run task and get dynamic delay (in MINUTES)
            wait_minutes = main.run_task()

            # Safety check: If calculation failed or returned 0/None
            if not wait_minutes or wait_minutes < 1:
                logging.warning("⚠️ Received 0 or None for wait time. Defaulting to 60 minutes.")
                wait_minutes = 60

            # 2. Log the sleep time clearly
            logging.info(f"--- 💤 Sleeping for {wait_minutes:.1f} minutes ---")

            # 3. Sleep (Convert Minutes -> Seconds)
            time.sleep(wait_minutes * 60)

        except KeyboardInterrupt:
            # Allow you to Ctrl+C if running manually
            logging.info("Stopping service manually.")
            break

        except Exception as e:
            # 4. Crash Handling: If main.py crashes, don't kill the service.
            # Log the error, wait 1 minute, and try again.
            logging.error(f"🔥 CRASH OCCURRED: {e}")
            logging.info("Retrying in 60 seconds...")
            time.sleep(60)