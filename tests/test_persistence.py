import pytest
import sys
import os
import time
from datetime import datetime
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set testing mode
os.environ["TESTING"] = "True"

from src.database import SessionLocal, engine, Base
from src.models import Event, Statistics
from src.event_processor import EventProcessor

@pytest.mark.asyncio
async def test_dedup_persistence_after_restart(clean_db):
    """Test deduplication persists after simulated restart
    
    Scenario:
    1. Send event A
    2. Stop processor (simulate crash)
    3. Start new processor (simulate restart)
    4. Send same event A again
    5. Verify it's detected as duplicate
    """
    
    print("\n" + "="*70)
    print("TEST: DEDUPLICATION PERSISTENCE AFTER RESTART")
    print("="*70)
    
    # Phase 1: Initial processing
    print("\n[Phase 1] Initial event processing...")
    processor1 = EventProcessor(batch_size=5)
    processor_task1 = asyncio.create_task(processor1.start())
    
    await asyncio.sleep(0.2)
    
    event_data = {
        "topic": "payment.processed",
        "event_id": "persist-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "payment-service",
        "payload": {"amount": 1000, "user_id": 123}
    }
    
    try:
        # Send first event
        await processor1.add_event(event_data.copy())
        print("✓ Event sent")
        
        # Wait for processing
        await processor1.wait_until_complete(timeout=5)
        await asyncio.sleep(1)
        
        # Verify first event processed
        db = SessionLocal()
        try:
            events = db.query(Event).filter_by(event_id="persist-001").all()
            stats = db.query(Statistics).first()
            
            print(f"✓ Events in DB: {len(events)}")
            print(f"✓ Stats - Received: {stats.received_count}, Unique: {stats.unique_processed}")
            
            assert len(events) == 1, "Should have 1 event"
            assert stats.unique_processed == 1, "Should have processed 1 unique"
        finally:
            db.close()
    
    finally:
        # Stop processor 1 (simulate crash/restart)
        print("\n[Phase 2] Simulating service restart...")
        await processor1.stop()
        processor_task1.cancel()
        try:
            await processor_task1
        except asyncio.CancelledError:
            pass
    
    # Phase 3: After restart
    print("\n[Phase 3] Service restarted, sending duplicate event...")
    processor2 = EventProcessor(batch_size=5)
    processor_task2 = asyncio.create_task(processor2.start())
    
    await asyncio.sleep(0.2)
    
    try:
        # Send SAME event again (simulate at-least-once delivery)
        await processor2.add_event(event_data.copy())
        print("✓ Duplicate event sent")
        
        # Wait for processing
        await processor2.wait_until_complete(timeout=5)
        await asyncio.sleep(1)
        
        # Verify deduplication still works
        db = SessionLocal()
        try:
            events = db.query(Event).filter_by(event_id="persist-001").all()
            stats = db.query(Statistics).first()
            
            print(f"\n[Results After Restart]")
            print(f"Events in DB: {len(events)}")
            print(f"Stats - Received: {stats.received_count}, Unique: {stats.unique_processed}, Duplicates: {stats.duplicate_dropped}")
            
            # Assertions
            assert len(events) == 1, f"Should still have only 1 event, got {len(events)}"
            assert stats.received_count == 2, f"Should have received 2 total, got {stats.received_count}"
            assert stats.unique_processed == 1, f"Should have 1 unique, got {stats.unique_processed}"
            assert stats.duplicate_dropped == 1, f"Should have dropped 1 duplicate, got {stats.duplicate_dropped}"
            
            print("✓ Deduplication persisted after restart!")
            print("="*70)
            
        finally:
            db.close()
    
    finally:
        await processor2.stop()
        processor_task2.cancel()
        try:
            await processor_task2
        except asyncio.CancelledError:
            pass

@pytest.mark.asyncio
async def test_multiple_restarts_with_duplicates(clean_db):
    """Test deduplication across multiple restart cycles"""
    
    print("\n" + "="*70)
    print("TEST: MULTIPLE RESTARTS WITH DUPLICATES")
    print("="*70)
    
    event_data = {
        "topic": "order.created",
        "event_id": "multi-restart-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "order-service",
        "payload": {"order_id": 999}
    }
    
    # Simulate 3 restart cycles
    for cycle in range(1, 4):
        print(f"\n[Cycle {cycle}] Starting processor...")
        
        processor = EventProcessor(batch_size=5)
        processor_task = asyncio.create_task(processor.start())
        await asyncio.sleep(0.2)
        
        try:
            # Send same event in each cycle
            await processor.add_event(event_data.copy())
            print(f"✓ Event sent in cycle {cycle}")
            
            await processor.wait_until_complete(timeout=5)
            await asyncio.sleep(0.5)
            
        finally:
            await processor.stop()
            processor_task.cancel()
            try:
                await processor_task
            except asyncio.CancelledError:
                pass
            
            print(f"✓ Processor stopped (cycle {cycle})")
    
    # Final verification
    db = SessionLocal()
    try:
        events = db.query(Event).filter_by(event_id="multi-restart-001").all()
        stats = db.query(Statistics).first()
        
        print(f"\n[Final Results After 3 Restart Cycles]")
        print(f"Events in DB: {len(events)}")
        print(f"Stats - Received: {stats.received_count}, Unique: {stats.unique_processed}, Duplicates: {stats.duplicate_dropped}")
        
        assert len(events) == 1, f"Should have only 1 event, got {len(events)}"
        assert stats.received_count == 3, f"Should have received 3, got {stats.received_count}"
        assert stats.unique_processed == 1, f"Should have 1 unique, got {stats.unique_processed}"
        assert stats.duplicate_dropped == 2, f"Should have dropped 2, got {stats.duplicate_dropped}"
        
        print("✓ Deduplication works across multiple restarts!")
        print("="*70)
        
    finally:
        db.close()