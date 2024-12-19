from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
from .processor import ETLProcessor
from config.logging_config import setup_logging

logger = setup_logging(__name__)


class ETLScheduler:
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.processor = ETLProcessor()

    def _run_etl(self, max_locations: int = None):
        """Wrapper for ETL process to handle exceptions"""
        try:
            logger.info("Starting scheduled ETL run")
            self.processor.process(max_locations)
            logger.info("Completed scheduled ETL run")
        except Exception as e:
            logger.error(f"Scheduled ETL run failed: {str(e)}")

    def start(self, interval_minutes: int = 60, max_locations: int = None):
        """Start the scheduler"""
        self.scheduler.add_job(
            self._run_etl,
            trigger=IntervalTrigger(minutes=interval_minutes),
            next_run_time=datetime.now(),
            kwargs={'max_locations': max_locations}
        )

        logger.info(f"Starting scheduler with {interval_minutes} minute interval")
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped")
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
            raise