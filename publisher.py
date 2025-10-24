import requests
import json
import time
import random
from datetime import datetime
from typing import List, Dict, Any
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

class EventPublisher:
    """Event publisher with at-least-once delivery simulation"""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        self.base_url = base_url
        self.stats = {
            "total_sent": 0,
            "successful": 0,
            "failed": 0,
            "unique_events": 0,
            "duplicate_events": 0
        }
    
    def publish_single(self, event: Dict[str, Any]) -> bool:
        """Publish single event to aggregator"""
        try:
            response = requests.post(
                f"{self.base_url}/publish",
                json=event,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            
            self.stats["total_sent"] += 1
            
            if response.status_code == 202:
                self.stats["successful"] += 1
                return True
            else:
                self.stats["failed"] += 1
                print(f"Failed to publish: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            self.stats["failed"] += 1
            print(f"Request error: {e}")
            return False
    
    def publish_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Publish batch of events (as array)"""
        try:
            response = requests.post(
                f"{self.base_url}/publish",
                json=events,  # Direct array
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            self.stats["total_sent"] += len(events)
            
            if response.status_code == 202:
                self.stats["successful"] += len(events)
                return True
            else:
                self.stats["failed"] += len(events)
                print(f"Failed to publish batch: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            self.stats["failed"] += len(events)
            print(f"Batch request error: {e}")
            return False
    
    def generate_event(self, index: int, topic: str = None) -> Dict[str, Any]:
        """Generate a single event"""
        topics = [
            "user.created",
            "order.placed", 
            "payment.processed",
            "notification.sent",
            "audit.logged",
            "inventory.updated",
            "shipping.dispatched",
            "review.submitted",
            "refund.processed",
            "subscription.activated"
        ]
        
        selected_topic = topic if topic else random.choice(topics)
        
        return {
            "topic": selected_topic,
            "event_id": f"evt-{index}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": f"{selected_topic.split('.')[0]}-service",
            "payload": {
                "index": index,
                "data": f"test-data-{index}",
                "timestamp": time.time()
            }
        }
    
    def simulate_at_least_once(
        self, 
        total_events: int,
        duplicate_ratio: float = 0.2,
        batch_size: int = 1,
        use_threads: bool = False,
        max_workers: int = 5
    ):
        """
        Simulate at-least-once delivery with duplicates
        
        Args:
            total_events: Total number of events to send (including duplicates)
            duplicate_ratio: Ratio of duplicate events (0.2 = 20%)
            batch_size: Number of events per batch (1 = single event mode)
            use_threads: Use multiple threads for sending
            max_workers: Number of concurrent threads
        """
        print("\n" + "="*70)
        print("EVENT PUBLISHER - AT-LEAST-ONCE DELIVERY SIMULATION")
        print("="*70)
        print(f"Configuration:")
        print(f"  Total events to send: {total_events}")
        print(f"  Duplicate ratio: {duplicate_ratio*100}%")
        print(f"  Batch size: {batch_size}")
        print(f"  Multi-threading: {use_threads}")
        if use_threads:
            print(f"  Max workers: {max_workers}")
        print("="*70)
        
        # Calculate unique and duplicate counts
        unique_count = int(total_events * (1 - duplicate_ratio))
        duplicate_count = total_events - unique_count
        
        self.stats["unique_events"] = unique_count
        self.stats["duplicate_events"] = duplicate_count
        
        print(f"\nGenerating events:")
        print(f"  Unique events: {unique_count}")
        print(f"  Duplicate events: {duplicate_count}")
        
        # Generate unique events
        unique_events = [self.generate_event(i) for i in range(unique_count)]
        
        # Create duplicates by randomly selecting from unique events
        all_events = unique_events.copy()
        for _ in range(duplicate_count):
            duplicate = random.choice(unique_events).copy()
            all_events.append(duplicate)
        
        # Shuffle to mix unique and duplicates
        random.shuffle(all_events)
        
        print(f"\nTotal events prepared: {len(all_events)}")
        print(f"Starting to send events...\n")
        
        start_time = time.time()
        
        if batch_size == 1:
            # Single event mode
            if use_threads:
                self._send_single_threaded(all_events, max_workers)
            else:
                self._send_single_sequential(all_events)
        else:
            # Batch mode
            if use_threads:
                self._send_batch_threaded(all_events, batch_size, max_workers)
            else:
                self._send_batch_sequential(all_events, batch_size)
        
        elapsed_time = time.time() - start_time
        
        # Print results
        self.print_results(elapsed_time)
    
    def _send_single_sequential(self, events: List[Dict[str, Any]]):
        """Send events one by one sequentially"""
        for i, event in enumerate(events):
            self.publish_single(event)
            
            if (i + 1) % 100 == 0:
                print(f"Progress: {i + 1}/{len(events)} events sent")
    
    def _send_single_threaded(self, events: List[Dict[str, Any]], max_workers: int):
        """Send events using multiple threads"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.publish_single, event) for event in events]
            
            for i, future in enumerate(as_completed(futures)):
                future.result()
                
                if (i + 1) % 100 == 0:
                    print(f"Progress: {i + 1}/{len(events)} events sent")
    
    def _send_batch_sequential(self, events: List[Dict[str, Any]], batch_size: int):
        """Send events in batches sequentially"""
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            self.publish_batch(batch)
            
            if (i + batch_size) % (batch_size * 10) == 0:
                print(f"Progress: {i + batch_size}/{len(events)} events sent")
    
    def _send_batch_threaded(self, events: List[Dict[str, Any]], batch_size: int, max_workers: int):
        """Send batches using multiple threads"""
        batches = [events[i:i + batch_size] for i in range(0, len(events), batch_size)]
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.publish_batch, batch) for batch in batches]
            
            for i, future in enumerate(as_completed(futures)):
                future.result()
                
                if (i + 1) % 10 == 0:
                    print(f"Progress: {(i + 1) * batch_size}/{len(events)} events sent")
    
    def print_results(self, elapsed_time: float):
        """Print publishing results"""
        print("\n" + "="*70)
        print("PUBLISHING RESULTS")
        print("="*70)
        print(f"Time taken: {elapsed_time:.2f} seconds")
        print(f"Throughput: {self.stats['total_sent']/elapsed_time:.2f} events/sec")
        print(f"\nEvent Statistics:")
        print(f"  Unique events: {self.stats['unique_events']}")
        print(f"  Duplicate events: {self.stats['duplicate_events']}")
        print(f"  Total sent: {self.stats['total_sent']}")
        print(f"  Successful: {self.stats['successful']}")
        print(f"  Failed: {self.stats['failed']}")
        print(f"  Success rate: {(self.stats['successful']/self.stats['total_sent']*100):.2f}%")
        print("="*70)
        
        # Wait for processing
        print("\nWaiting 5 seconds for event processing...")
        time.sleep(5)
        
        # Check aggregator stats
        self.check_aggregator_stats()
    
    def check_aggregator_stats(self):
        """Check aggregator statistics"""
        try:
            response = requests.get(f"{self.base_url}/stats", timeout=5)
            
            if response.status_code == 200:
                stats = response.json()
                
                print("\n" + "="*70)
                print("AGGREGATOR STATISTICS")
                print("="*70)
                print(f"Received: {stats['received']}")
                print(f"Unique processed: {stats['unique_processed']}")
                print(f"Duplicates dropped: {stats['duplicate_dropped']}")
                print(f"Topics: {', '.join(stats['topics'])}")
                print(f"Queue size: {stats['queue_size']}")
                print(f"Uptime: {stats['uptime']:.2f}s")
                print("="*70)
                
                # Validation
                print("\n" + "="*70)
                print("VALIDATION")
                print("="*70)
                
                expected_unique = self.stats['unique_events']
                expected_duplicates = self.stats['duplicate_events']
                
                if stats['received'] == self.stats['successful']:
                    print(f"✓ All sent events received ({stats['received']})")
                else:
                    print(f"✗ Mismatch: Sent {self.stats['successful']}, Received {stats['received']}")
                
                if stats['unique_processed'] == expected_unique:
                    print(f"✓ Unique events correct ({stats['unique_processed']})")
                else:
                    print(f"⚠ Unique events: Expected {expected_unique}, Got {stats['unique_processed']}")
                
                if stats['duplicate_dropped'] == expected_duplicates:
                    print(f"✓ Duplicates detected correctly ({stats['duplicate_dropped']})")
                else:
                    print(f"⚠ Duplicates: Expected {expected_duplicates}, Got {stats['duplicate_dropped']}")
                
                print("="*70)
            else:
                print(f"Failed to get stats: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"Error checking stats: {e}")


def main():
    parser = argparse.ArgumentParser(description="Event Publisher with At-Least-Once Delivery Simulation")
    
    parser.add_argument(
        "--url",
        default="http://localhost:5000",
        help="Aggregator service URL (default: http://localhost:5000)"
    )
    
    parser.add_argument(
        "--events",
        type=int,
        default=5000,
        help="Total number of events to send (default: 5000)"
    )
    
    parser.add_argument(
        "--duplicate-ratio",
        type=float,
        default=0.2,
        help="Ratio of duplicate events (default: 0.2 = 20%%)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Batch size for sending (1 = single mode, >1 = batch mode)"
    )
    
    parser.add_argument(
        "--threads",
        action="store_true",
        help="Use multi-threading for sending"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Max worker threads (default: 5)"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.duplicate_ratio < 0 or args.duplicate_ratio > 1:
        print("Error: duplicate-ratio must be between 0 and 1")
        sys.exit(1)
    
    if args.events < 1:
        print("Error: events must be at least 1")
        sys.exit(1)
    
    # Create publisher
    publisher = EventPublisher(base_url=args.url)
    
    # Check if service is available
    try:
        response = requests.get(f"{args.url}/health", timeout=5)
        if response.status_code != 200:
            print(f"Error: Service not healthy (status: {response.status_code})")
            sys.exit(1)
        print(f"✓ Connected to aggregator at {args.url}")
    except requests.exceptions.RequestException as e:
        print(f"Error: Cannot connect to aggregator at {args.url}")
        print(f"Details: {e}")
        sys.exit(1)
    
    # Run simulation
    publisher.simulate_at_least_once(
        total_events=args.events,
        duplicate_ratio=args.duplicate_ratio,
        batch_size=args.batch_size,
        use_threads=args.threads,
        max_workers=args.max_workers
    )


if __name__ == "__main__":
    main()