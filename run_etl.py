
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

#TODO: Check if work without it, if yes delete.
# import os
# import sys
# Add the project root directory to Python path
# project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(project_root)

from src import AirQualityETL


def etl_job():
    try:
        print(f"Starting ETL job at {datetime.now()}")
        etl = AirQualityETL()
        etl.run()
        print(f"ETL job completed successfully at {datetime.now()}")
    except Exception as e:
        print(f"Error in ETL job: {str(e)}")


def main():
    scheduler = BlockingScheduler()

    scheduler.add_job(
        etl_job,
        'interval',
        minutes=10,
        next_run_time=datetime.now()
    )
    print("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except(KeyboardInterrupt, SystemExit):
        print("\nScheduler stopped.")


if __name__ == "__main__":
    main()
