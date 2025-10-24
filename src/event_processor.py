import asyncio
import logging
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from src.database import SessionLocal
from src.models import Event, Statistics
import json
from typing import List, Dict
import os

# Setup logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger(__name__)

class EventProcessor:
    def __init__(self, batch_size=None):
        # Use environment variable or default
        default_batch_size = int(os.getenv("BATCH_SIZE", "100"))
        self.batch_size = batch_size if batch_size is not None else default_batch_size
        
        self.queue = asyncio.Queue()
        self.running = False
        self.start_time = datetime.utcnow()
        self.processing_count = 0
        
    async def start(self):
        """Start the event processor with batch processing"""
        self.running = True
        logger.info(f"Event processor started with batch size: {self.batch_size}")
        
        while self.running:
            try:
                # Collect batch of events
                batch = []
                timeout = 0.1  # Shorter timeout for batch collection
                
                try:
                    # Get first event with longer timeout
                    first_event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    batch.append(first_event)
                    self.queue.task_done()
                    
                    # Collect more events quickly
                    while len(batch) < self.batch_size:
                        try:
                            event = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                            batch.append(event)
                            self.queue.task_done()
                        except asyncio.TimeoutError:
                            # Process whatever we have after timeout
                            break
                    
                    # Process batch (even if not full)
                    if batch:
                        await self.process_batch(batch)
                        
                except asyncio.TimeoutError:
                    continue
                    
            except asyncio.CancelledError:
                logger.info("Event processor cancelled")
                # Process remaining events before stopping
                await self._flush_remaining()
                break
            except Exception as e:
                logger.error(f"Error in event processor: {e}")
    
    async def _flush_remaining(self):
        """Flush remaining events in queue"""
        remaining = []
        while not self.queue.empty():
            try:
                event = self.queue.get_nowait()
                remaining.append(event)
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break
        
        if remaining:
            logger.info(f"Flushing {len(remaining)} remaining events")
            await self.process_batch(remaining)
    
    async def process_batch(self, batch: List[Dict]):
        """Process a batch of events with proper deduplication"""
        db = SessionLocal()
        try:
            unique_count = 0
            duplicate_count = 0
            
            # Process each event individually to handle duplicates properly
            for event_data in batch:
                # Create a new session for each event to isolate transactions
                event_db = SessionLocal()
                try:
                    # Parse timestamp
                    timestamp = datetime.fromisoformat(event_data['timestamp'].replace('Z', '+00:00'))
                    
                    # Create event object
                    event = Event(
                        topic=event_data['topic'],
                        event_id=event_data['event_id'],
                        timestamp=timestamp,
                        source=event_data['source'],
                        payload=json.dumps(event_data['payload'])
                    )
                    
                    # Try to insert
                    event_db.add(event)
                    event_db.commit()
                    unique_count += 1
                    logger.debug(f"✓ Unique: {event.topic}/{event.event_id}")
                    
                except IntegrityError:
                    # Duplicate detected
                    event_db.rollback()
                    duplicate_count += 1
                    logger.debug(f"✗ Duplicate: {event_data['topic']}/{event_data['event_id']}")
                    
                except Exception as e:
                    event_db.rollback()
                    logger.error(f"Error processing event: {e}")
                    
                finally:
                    event_db.close()
            
            # Update statistics in main session
            self._update_stats_batch(db, len(batch), unique_count, duplicate_count)
            
            logger.info(f"Processed batch: {len(batch)} events ({unique_count} unique, {duplicate_count} duplicates)")
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
        finally:
            db.close()
    
    def _update_stats_batch(self, db, received, unique, duplicates):
        """Update statistics for batch processing"""
        try:
            stats = db.query(Statistics).first()
            if not stats:
                stats = Statistics(received_count=0, unique_processed=0, duplicate_dropped=0)
                db.add(stats)
            
            stats.received_count += received
            stats.unique_processed += unique
            stats.duplicate_dropped += duplicates
            
            db.commit()
            logger.debug(f"Stats updated: R={stats.received_count}, U={stats.unique_processed}, D={stats.duplicate_dropped}")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating stats: {e}")
    
    async def add_event(self, event_data: dict):
        """Add event to processing queue"""
        await self.queue.put(event_data)
    
    async def wait_until_complete(self, timeout=30):
        """Wait until all events in queue are processed"""
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout)
            
            # Small delay to ensure DB commit completes
            await asyncio.sleep(0.2)
            
            return True
        except asyncio.TimeoutError:
            logger.warning(f"Queue processing timeout after {timeout}s, remaining: {self.queue.qsize()}")
            return False
    
    def queue_size(self):
        """Get current queue size"""
        return self.queue.qsize()
    
    def get_uptime(self) -> float:
        """Get uptime in seconds"""
        return (datetime.utcnow() - self.start_time).total_seconds()
    
    async def stop(self):
        """Stop the event processor gracefully"""
        logger.info("Stopping event processor...")
        
        # Wait for queue to be empty
        await self.wait_until_complete(timeout=30)
        
        # Flush any remaining
        await self._flush_remaining()
        
        self.running = False
        logger.info("Event processor stopped")

# Global processor instance - use default batch size from env or 100
event_processor = EventProcessor()