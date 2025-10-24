import pytest
import sys
import os
import json
from datetime import datetime
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set testing mode
os.environ["TESTING"] = "True"

from src.app import app
from src.database import SessionLocal
from src.models import Event, Statistics

@pytest.fixture
def client(clean_db):
    """Create test client with clean database"""
    app.config['TESTING'] = True
    
    with app.test_client() as client:
        yield client

def test_publish_single_event(client):
    """Test publishing a single event"""
    event = {
        "topic": "user.created",
        "event_id": "evt-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "user-service",
        "payload": {"user_id": 123, "email": "test@example.com"}
    }
    
    response = client.post('/publish', 
                          data=json.dumps(event),
                          content_type='application/json')
    
    assert response.status_code == 202
    data = json.loads(response.data)
    assert data['count'] == 1
    
    # Wait for processing
    time.sleep(1)

def test_publish_batch_with_wrapper(client):
    """Test publishing batch events with wrapper"""
    batch = {
        "events": [
            {
                "topic": "order.placed",
                "event_id": f"evt-{i}",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "order-service",
                "payload": {"order_id": i}
            }
            for i in range(5)
        ]
    }
    
    response = client.post('/publish',
                          data=json.dumps(batch),
                          content_type='application/json')
    
    assert response.status_code == 202
    data = json.loads(response.data)
    assert data['count'] == 5
    
    # Wait for processing
    time.sleep(1)

def test_get_events_by_topic(client):
    """Test retrieving events by topic"""
    # Publish events first
    event = {
        "topic": "test.topic",
        "event_id": "evt-test-001",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "test-service",
        "payload": {"data": "test"}
    }
    
    client.post('/publish', data=json.dumps(event), content_type='application/json')
    
    # Wait for processing
    time.sleep(1)
    
    response = client.get('/events?topic=test.topic')
    assert response.status_code == 200

def test_stats_endpoint(client):
    """Test statistics endpoint"""
    response = client.get('/stats')
    assert response.status_code == 200
    
    data = json.loads(response.data)
    assert 'received' in data
    assert 'unique_processed' in data
    assert 'duplicate_dropped' in data
    assert 'topics' in data
    assert 'uptime' in data
    assert 'queue_size' in data

def test_health_check(client):
    """Test health check endpoint"""
    response = client.get('/health')
    assert response.status_code == 200
    
    data = json.loads(response.data)
    assert data['status'] == 'healthy'
    assert 'queue_size' in data