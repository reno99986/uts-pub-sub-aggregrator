import pytest
import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set testing mode
os.environ["TESTING"] = "True"

from src.database import SessionLocal
from src.models import Event, Statistics
from src.event_processor import EventProcessor
import asyncio

@pytest.mark.asyncio
async def test_deduplication(clean_db):
    """Test event deduplication"""
    # Use smaller batch size but ensure it processes
    processor = EventProcessor(batch_size=5)
    
    # Start processor in background
    processor_task = asyncio.create_task(processor.start())
    
    # Wait for processor to start
    await asyncio.sleep(0.2)
    
    try:
        # Create duplicate events
        event_data = {
            "topic": "test.dedup",
            "event_id": "duplicate-001",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "test",
            "payload": {"data": "test"}
        }
        
        print("\nSending 3 duplicate events...")
        
        # Send same event 3 times
        for i in range(3):
            await processor.add_event(event_data.copy())
            print(f"Event {i+1} queued")
        
        print(f"Queue size: {processor.queue_size()}")
        
        # Wait for all events to be processed
        print("Waiting for processing...")
        completed = await processor.wait_until_complete(timeout=5)
        
        if not completed:
            print(f"Warning: timeout, queue size: {processor.queue_size()}")
        else:
            print("Processing completed!")
        
        # Extra wait for DB commit
        await asyncio.sleep(1)
        
        # Check database
        db = SessionLocal()
        try:
            events = db.query(Event).filter_by(event_id="duplicate-001").all()
            stats = db.query(Statistics).first()
            
            print(f"\nResults:")
            print(f"Events in DB: {len(events)}")
            if stats:
                print(f"Stats - Received: {stats.received_count}, Unique: {stats.unique_processed}, Duplicates: {stats.duplicate_dropped}")
            else:
                print("No statistics found")
            
            assert len(events) == 1, f"Should only have 1 unique event, got {len(events)}"
            assert stats is not None, "Statistics should exist"
            assert stats.received_count == 3, f"Should have received 3 events, got {stats.received_count}"
            assert stats.unique_processed == 1, f"Should have processed 1 unique event, got {stats.unique_processed}"
            assert stats.duplicate_dropped == 2, f"Should have dropped 2 duplicates, got {stats.duplicate_dropped}"
            
            print("✓ Test passed!")
            
        finally:
            db.close()
    
    finally:
        # Cleanup
        print("\nStopping processor...")
        await processor.stop()
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass

@pytest.mark.asyncio
async def test_high_volume_with_duplicates(clean_db):
    """Test processing 5000+ events with 20% duplication"""
    # Use larger batch size for better performance
    processor = EventProcessor(batch_size=200)
    processor_task = asyncio.create_task(processor.start())
    
    # Wait for processor to start
    await asyncio.sleep(0.2)
    
    try:
        total_events = 6000
        duplicate_ratio = 0.2
        unique_count = int(total_events * (1 - duplicate_ratio))
        
        events_to_send = []
        
        # Create unique events
        base_timestamp = datetime.utcnow().isoformat()
        for i in range(unique_count):
            events_to_send.append({
                "topic": f"topic-{i % 10}",
                "event_id": f"evt-{i}",
                "timestamp": base_timestamp,
                "source": "load-test",
                "payload": {"index": i}
            })
        
        # Add duplicates
        duplicate_count = total_events - unique_count
        for i in range(duplicate_count):
            events_to_send.append(events_to_send[i % unique_count].copy())
        
        # Send all events
        start_time = time.time()
        
        print(f"\nSending {total_events} events...")
        
        # Send all events as fast as possible
        for i, event in enumerate(events_to_send):
            await processor.add_event(event)
            if (i + 1) % 1000 == 0:
                print(f"Queued {i + 1}/{total_events} events...")
        
        queue_time = time.time() - start_time
        print(f"All {total_events} events queued in {queue_time:.2f}s")
        print(f"Queue size: {processor.queue_size()}")
        print("Waiting for processing to complete...")
        
        # Wait for all events to be processed with extended timeout
        completed = await processor.wait_until_complete(timeout=120)
        
        if not completed:
            print(f"Warning: Processing timeout, remaining queue size: {processor.queue_size()}")
        else:
            print("Processing completed successfully!")
        
        # Additional wait to ensure all DB commits are done
        await asyncio.sleep(2)
        
        processing_time = time.time() - start_time
        
        # Verify results
        db = SessionLocal()
        try:
            stats = db.query(Statistics).first()
            
            assert stats is not None, "Statistics should exist"
            
            # Verify event counts
            total_db_events = db.query(Event).count()
            
            print(f"\nPerformance Results:")
            print(f"Total events sent: {total_events}")
            print(f"Expected unique: {unique_count}")
            print(f"Expected duplicates: {duplicate_count}")
            print(f"Actual received: {stats.received_count}")
            print(f"Actual unique processed: {stats.unique_processed}")
            print(f"Actual duplicates dropped: {stats.duplicate_dropped}")
            print(f"Total unique events in DB: {total_db_events}")
            print(f"Processing time: {processing_time:.2f}s")
            print(f"Throughput: {stats.received_count/processing_time:.2f} events/sec")
            print(f"Final queue size: {processor.queue_size()}")
            
            assert stats.received_count == total_events, \
                f"Should have received {total_events} events, got {stats.received_count}"
            assert stats.unique_processed == unique_count, \
                f"Should have processed {unique_count} unique events, got {stats.unique_processed}"
            assert stats.duplicate_dropped == duplicate_count, \
                f"Should have dropped {duplicate_count} duplicates, got {stats.duplicate_dropped}"
            assert total_db_events == unique_count, \
                f"Should have {unique_count} events in DB, got {total_db_events}"
            
            print("✓ All assertions passed!")
                
        finally:
            db.close()
    
    finally:
        # Cleanup
        print("\nStopping processor...")
        await processor.stop()
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass

@pytest.mark.asyncio
async def test_batch_processing_performance(clean_db):
    """Test batch processing with smaller dataset"""
    processor = EventProcessor(batch_size=50)
    processor_task = asyncio.create_task(processor.start())
    
    await asyncio.sleep(0.2)
    
    try:
        total_events = 1000
        
        print(f"\nSending {total_events} events...")
        
        # Send events
        start_time = time.time()
        for i in range(total_events):
            await processor.add_event({
                "topic": f"topic-{i % 5}",
                "event_id": f"evt-{i}",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "batch-test",
                "payload": {"index": i}
            })
        
        # Wait for processing
        await processor.wait_until_complete(timeout=30)
        await asyncio.sleep(1)
        
        processing_time = time.time() - start_time
        
        db = SessionLocal()
        try:
            stats = db.query(Statistics).first()
            
            print(f"\nBatch Processing Test:")
            print(f"Events: {total_events}")
            print(f"Time: {processing_time:.2f}s")
            print(f"Throughput: {stats.received_count/processing_time:.2f} events/sec")
            
            assert stats.received_count == total_events
            assert stats.unique_processed == total_events
            
            print("✓ Test passed!")
        finally:
            db.close()
    
    finally:
        await processor.stop()
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass