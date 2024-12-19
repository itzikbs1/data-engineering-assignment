#scripts/run_scheduled_etl.py
from src.etl.scheduler import ETLScheduler
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    try:
        scheduler = ETLScheduler()
        scheduler.start(interval_minutes=60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        raise

if __name__ == "__main__":
    main()