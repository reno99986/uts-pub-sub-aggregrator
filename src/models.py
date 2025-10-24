from sqlalchemy import Column, String, DateTime, Text, Integer, Index
from src.database import Base
from datetime import datetime
import json

class Event(Base):
    __tablename__ = "events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String(255), nullable=False, index=True)
    event_id = Column(String(255), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    source = Column(String(255), nullable=False)
    payload = Column(Text, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_topic_event_id', 'topic', 'event_id', unique=True),
    )
    
    def to_dict(self):
        return {
            "topic": self.topic,
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "payload": json.loads(self.payload),
            "received_at": self.received_at.isoformat()
        }

class Statistics(Base):
    __tablename__ = "statistics"
    
    id = Column(Integer, primary_key=True)
    received_count = Column(Integer, default=0)
    unique_processed = Column(Integer, default=0)
    duplicate_dropped = Column(Integer, default=0)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)