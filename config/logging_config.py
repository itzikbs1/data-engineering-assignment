import logging
from pathlib import Path
from typing import Optional


def setup_logging(name: Optional[str] = None) -> logging.Logger:
    """
    Configure and return a logger instance
    Args:
        name: Optional name for the logger. If None, returns root logger
    """
    # Create logs directory if it doesn't exist
    Path("logs").mkdir(exist_ok=True)

    # Configure root logger if no handlers exist
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        # Set basic configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/openaq_etl.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    # Return named logger if name provided, otherwise return root logger
    return logging.getLogger(name if name else 'openaq_etl')