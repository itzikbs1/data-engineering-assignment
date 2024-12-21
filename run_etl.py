from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from src import AirQualityETL


def etl_job():
    try:
        etl = AirQualityETL()
        etl.run()
    except Exception as e:
        print(f"Error in ETL job: {str(e)}")


def main():
    scheduler = BackgroundScheduler()

    scheduler.add_job(
        etl_job,
        'interval',
        hours=1,
        next_run_time=datetime.now()
    )

    try:
        scheduler.start()
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("\nScheduler stopped.")


if __name__ == "__main__":
    main()