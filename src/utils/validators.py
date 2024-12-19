# src/utils/validators.py
from datetime import datetime, timedelta


def is_recent_data(timestamp: datetime, hours: int = 24) -> bool:
    """Check if data is within specified hours"""
    return datetime.utcnow() - timestamp <= timedelta(hours=hours)