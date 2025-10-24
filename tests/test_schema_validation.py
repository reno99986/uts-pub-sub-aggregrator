import pytest
import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set testing mode
os.environ["TESTING"] = "True"

from src.app import app

@pytest.fixture
def client(clean_db):
    """Create test client with clean database"""
    app.config['TESTING'] = True
    
    with app.test_client() as client:
        yield client

def test_missing_required_fields(client):
    """Test validation when required fields are missing"""
    
    print("\n" + "="*70)
    print("TEST: MISSING REQUIRED FIELDS VALIDATION")
    print("="*70)
    
    test_cases = [
        # Missing topic
        {
            "data": {
                "event_id": "evt-001",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test",
                "payload": {}
            },
            "missing_field": "topic"
        },
        # Missing event_id
        {
            "data": {
                "topic": "test.topic",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test",
                "payload": {}
            },
            "missing_field": "event_id"
        },
        # Missing timestamp
        {
            "data": {
                "topic": "test.topic",
                "event_id": "evt-001",
                "source": "test",
                "payload": {}
            },
            "missing_field": "timestamp"
        },
        # Missing source
        {
            "data": {
                "topic": "test.topic",
                "event_id": "evt-001",
                "timestamp": datetime.utcnow().isoformat(),
                "payload": {}
            },
            "missing_field": "source"
        },
        # Missing payload
        {
            "data": {
                "topic": "test.topic",
                "event_id": "evt-001",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test"
            },
            "missing_field": "payload"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n[Test {i}] Missing field: {test_case['missing_field']}")
        
        response = client.post('/publish',
                              data=json.dumps(test_case['data']),
                              content_type='application/json')
        
        print(f"Response status: {response.status_code}")
        
        assert response.status_code == 400, f"Should return 400 for missing {test_case['missing_field']}"
        
        data = json.loads(response.data)
        assert 'error' in data, "Should contain error message"
        print(f"✓ Correctly rejected missing {test_case['missing_field']}")
    
    print("\n✓ All missing field validations passed!")
    print("="*70)

def test_invalid_field_types(client):
    """Test validation when field types are invalid"""
    
    print("\n" + "="*70)
    print("TEST: INVALID FIELD TYPES VALIDATION")
    print("="*70)
    
    test_cases = [
        # Invalid timestamp format
        {
            "data": {
                "topic": "test.topic",
                "event_id": "evt-001",
                "timestamp": "not-a-timestamp",
                "source": "test",
                "payload": {}
            },
            "invalid_field": "timestamp (invalid format)"
        },
        # Empty topic
        {
            "data": {
                "topic": "",
                "event_id": "evt-001",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test",
                "payload": {}
            },
            "invalid_field": "topic (empty string)"
        },
        # Empty event_id
        {
            "data": {
                "topic": "test.topic",
                "event_id": "",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test",
                "payload": {}
            },
            "invalid_field": "event_id (empty string)"
        },
        # Payload not a dict
        {
            "data": {
                "topic": "test.topic",
                "event_id": "evt-001",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test",
                "payload": "not-a-dict"
            },
            "invalid_field": "payload (not a dict)"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n[Test {i}] Invalid: {test_case['invalid_field']}")
        
        response = client.post('/publish',
                              data=json.dumps(test_case['data']),
                              content_type='application/json')
        
        print(f"Response status: {response.status_code}")
        
        assert response.status_code == 400, f"Should return 400 for invalid {test_case['invalid_field']}"
        
        data = json.loads(response.data)
        assert 'error' in data, "Should contain error message"
        print(f"✓ Correctly rejected invalid {test_case['invalid_field']}")
    
    print("\n✓ All invalid type validations passed!")
    print("="*70)

def test_valid_event_schema(client):
    """Test that valid events are accepted"""
    
    print("\n" + "="*70)
    print("TEST: VALID EVENT SCHEMA")
    print("="*70)
    
    valid_event = {
        "topic": "user.registered",
        "event_id": "evt-valid-001",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "auth-service",
        "payload": {
            "user_id": 12345,
            "email": "test@example.com",
            "metadata": {
                "ip": "192.168.1.1",
                "user_agent": "Mozilla/5.0"
            }
        }
    }
    
    response = client.post('/publish',
                          data=json.dumps(valid_event),
                          content_type='application/json')
    
    print(f"Response status: {response.status_code}")
    
    assert response.status_code == 202, "Valid event should be accepted"
    
    data = json.loads(response.data)
    assert data['status'] == 'accepted'
    assert data['count'] == 1
    
    print("✓ Valid event accepted successfully!")
    print("="*70)

def test_batch_with_mixed_validity(client):
    """Test batch with some valid and some invalid events"""
    
    print("\n" + "="*70)
    print("TEST: BATCH WITH MIXED VALIDITY")
    print("="*70)
    
    # Batch with one invalid event
    batch = [
        {
            "topic": "valid.event",
            "event_id": "evt-001",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "test",
            "payload": {}
        },
        {
            # Missing event_id
            "topic": "invalid.event",
            "timestamp": datetime.utcnow().isoformat(),
            "source": "test",
            "payload": {}
        }
    ]
    
    response = client.post('/publish',
                          data=json.dumps(batch),
                          content_type='application/json')
    
    print(f"Response status: {response.status_code}")
    
    # Should reject entire batch if any event is invalid
    assert response.status_code == 400, "Should reject batch with invalid events"
    
    data = json.loads(response.data)
    assert 'error' in data
    print("✓ Batch with invalid event correctly rejected!")
    print("="*70)