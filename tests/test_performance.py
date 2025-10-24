import pytest
import sys
import os
import time
from datetime import datetime
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set testing mode
os.environ["TESTING"] = "True"

from src.database import SessionLocal
from src.models import Event, Statistics
from src.event_processor import EventProcessor

@pytest.mark.asyncio
async def test_stress_batch_processing_with_timing(clean_db):
    """Stress test: batch 1000 events with timing assertions"""
    
    print("\n" + "="*70)
    print("STRESS TEST: BATCH PROCESSING WITH TIMING")
    print("="*70)
    
    processor = EventProcessor(batch_size=100)
    processor_task = asyncio.create_task(processor.start())
    
    await asyncio.sleep(0.2)
    
    try:
        total_events = 1000
        base_timestamp = datetime.utcnow().isoformat()
        
        print(f"\n[Phase 1] Queuing {total_events} events...")
        queue_start = time.time()
        
        # Queue all events
        for i in range(total_events):
            await processor.add_event({
                "topic": f"stress.test.{i % 10}",
                "event_id": f"stress-{i}",
                "timestamp": base_timestamp,
                "source": "stress-test",
                "payload": {"index": i, "data": f"test-data-{i}"}
            })
        
        queue_time = time.time() - queue_start
        print(f"✓ Queuing completed in {queue_time:.3f}s")
        print(f"✓ Queuing rate: {total_events/queue_time:.2f} events/sec")
        
        # Assert queuing is fast (should be < 1 second for 1000 events)
        assert queue_time < 2.0, f"Queuing too slow: {queue_time:.3f}s (max: 2.0s)"
        
        print(f"\n[Phase 2] Processing {total_events} events...")
        process_start = time.time()
        
        # Wait for processing
        completed = await processor.wait_until_complete(timeout=30)
        
        process_time = time.time() - process_start
        
        if completed:
            print(f"✓ Processing completed in {process_time:.3f}s")
        else:
            print(f"⚠ Processing timeout after {process_time:.3f}s")
            print(f"  Remaining in queue: {processor.queue_size()}")
        
        # Additional wait for DB commits
        await asyncio.sleep(1)
        
        total_time = time.time() - queue_start
        
        # Verify results
        db = SessionLocal()
        try:
            stats = db.query(Statistics).first()
            total_db_events = db.query(Event).count()
            
            print(f"\n[Performance Metrics]")
            print(f"Total execution time: {total_time:.3f}s")
            print(f"Queuing time: {queue_time:.3f}s")
            print(f"Processing time: {process_time:.3f}s")
            print(f"Overall throughput: {total_events/total_time:.2f} events/sec")
            print(f"Processing throughput: {total_events/process_time:.2f} events/sec")
            
            print(f"\n[Database Stats]")
            print(f"Events received: {stats.received_count}")
            print(f"Events in DB: {total_db_events}")
            print(f"Unique processed: {stats.unique_processed}")
            
            # Assertions
            assert stats.received_count == total_events, \
                f"Should receive {total_events}, got {stats.received_count}"
            assert total_db_events == total_events, \
                f"Should have {total_events} in DB, got {total_db_events}"
            
            # Performance assertions (adjust based on your requirements)
            assert total_time < 30.0, \
                f"Total time too slow: {total_time:.3f}s (max: 30s)"
            assert total_events/total_time >= 30, \
                f"Throughput too low: {total_events/total_time:.2f} events/sec (min: 30)"
            
            print(f"\n✓ Performance requirements met!")
            print("="*70)
            
        finally:
            db.close()
    
    finally:
        await processor.stop()
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass

@pytest.mark.asyncio
async def test_stress_concurrent_topics(clean_db):
    """Stress test: multiple topics concurrently"""
    
    print("\n" + "="*70)
    print("STRESS TEST: CONCURRENT TOPICS")
    print("="*70)
    
    processor = EventProcessor(batch_size=50)
    processor_task = asyncio.create_task(processor.start())
    
    await asyncio.sleep(0.2)
    
    try:
        topics = ["user.created", "order.placed", "payment.processed", 
                  "notification.sent", "audit.logged"]
        events_per_topic = 200
        total_events = len(topics) * events_per_topic
        
        print(f"\nSending {events_per_topic} events per topic ({len(topics)} topics)")
        start_time = time.time()
        
        # Send events for all topics concurrently
        for topic in topics:
            for i in range(events_per_topic):
                await processor.add_event({
                    "topic": topic,
                    "event_id": f"{topic}-{i}",
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": f"{topic.split('.')[0]}-service",
                    "payload": {"index": i}
                })
        
        await processor.wait_until_complete(timeout=30)
        await asyncio.sleep(1)
        
        total_time = time.time() - start_time
        
        # Verify
        db = SessionLocal()
        try:
            stats = db.query(Statistics).first()
            
            # Count events per topic
            topic_counts = {}
            for topic in topics:
                count = db.query(Event).filter_by(topic=topic).count()
                topic_counts[topic] = count
            
            print(f"\n[Results]")
            print(f"Total time: {total_time:.3f}s")
            print(f"Throughput: {total_events/total_time:.2f} events/sec")
            print(f"\nEvents per topic:")
            for topic, count in topic_counts.items():
                print(f"  {topic}: {count}")
            
            # Assertions
            assert stats.received_count == total_events
            for topic in topics:
                assert topic_counts[topic] == events_per_topic, \
                    f"Topic {topic} should have {events_per_topic}, got {topic_counts[topic]}"
            
            print(f"\n✓ All topics processed correctly!")
            print("="*70)
            
        finally:
            db.close()
    
    finally:
        await processor.stop()
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass

@pytest.mark.asyncio
async def test_stats_consistency_during_load(clean_db):
    """Test /stats endpoint consistency during high load"""
    
    print("\n" + "="*70)
    print("TEST: STATS CONSISTENCY DURING LOAD")
    print("="*70)
    
    processor = EventProcessor(batch_size=50)
    processor_task = asyncio.create_task(processor.start())
    
    await asyncio.sleep(0.2)
    
    try:
        total_events = 500
        
        print(f"\nSending {total_events} events while checking stats...")
        
        # Send events
        for i in range(total_events):
            await processor.add_event({
                "topic": f"consistency.test.{i % 5}",
                "event_id": f"evt-{i}",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test",
                "payload": {"index": i}
            })
            
            # Check stats periodically
            if (i + 1) % 100 == 0:
                db = SessionLocal()
                try:
                    stats = db.query(Statistics).first()
                    if stats:
                        print(f"  After {i+1} events: stats.received={stats.received_count}")
                finally:
                    db.close()
        
        await processor.wait_until_complete(timeout=20)
        await asyncio.sleep(1)
        
        # Final verification
        db = SessionLocal()
        try:
            stats = db.query(Statistics).first()
            db_count = db.query(Event).count()
            
            print(f"\n[Final Stats]")
            print(f"Stats received: {stats.received_count}")
            print(f"Stats unique: {stats.unique_processed}")
            print(f"DB count: {db_count}")
            
            # Stats should be consistent with DB
            assert stats.received_count == total_events
            assert stats.unique_processed == db_count
            assert db_count == total_events
            
            print(f"✓ Stats consistent with database!")
            print("="*70)
            
        finally:
            db.close()
    
    finally:
        await processor.stop()
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass