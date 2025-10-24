from pydantic import BaseModel, Field, field_validator
from typing import Dict, Any, List
from datetime import datetime

class EventSchema(BaseModel):
    topic: str = Field(..., min_length=1, max_length=255)
    event_id: str = Field(..., min_length=1, max_length=255)
    timestamp: str
    source: str = Field(..., min_length=1, max_length=255)
    payload: Dict[str, Any]
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('Invalid ISO8601 timestamp format')

class EventBatchSchema(BaseModel):
    events: List[EventSchema]

class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime: float