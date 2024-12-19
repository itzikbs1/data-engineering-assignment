from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class Sensor(BaseModel):
    id: int
    location_id: int
    name: str
    parameter_name: str
    parameter_display_name: str
    units: str
    created_at: Optional[datetime] = None