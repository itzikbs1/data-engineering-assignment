from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from .coordinates import Coordinates


class Measurement(BaseModel):
    id: Optional[int] = None
    location_id: int
    sensor_id: int
    value: float
    datetime: datetime
    coordinates: Optional[Coordinates] = None
    created_at: Optional[datetime] = None
