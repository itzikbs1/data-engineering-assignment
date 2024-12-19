from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel
from .coordinates import Coordinates


class Location(BaseModel):
    id: int
    name: str
    country_code: str
    country_name: str
    city: Optional[str] = None
    coordinates: Coordinates
    timezone: Optional[str] = None
    owner_name: Optional[str] = None
    provider_name: Optional[str] = None
    is_mobile: bool = False
    is_monitor: bool = False
    created_at: Optional[datetime] = None
    sensor_ids: List[int] = []  # Added field for sensor IDs
    last_update: Optional[datetime] = None  # Added field for checking active status