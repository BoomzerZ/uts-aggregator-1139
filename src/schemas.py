from pydantic import BaseModel, Field, validator
from typing import Any, Dict, Optional
from datetime import datetime


class EventPayload(BaseModel):
    """Payload fleksibel"""
    data: Dict[str, Any] = {}


class Event(BaseModel):
    topic: str = Field(..., min_length=1)
    event_id: str = Field(..., min_length=1)
    timestamp: str
    source: str = Field(..., min_length=1)
    payload: Optional[Dict[str, Any]] = {}

    @validator("timestamp")
    def validate_timestamp(cls, v):
        # pastikan ISO8601 parseable
        try:
            datetime.fromisoformat(v)
        except Exception as e:
            raise ValueError("timestamp harus ISO8601 parseable") from e
        return v
