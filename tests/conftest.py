import pytest
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set testing mode BEFORE importing anything else
os.environ["TESTING"] = "True"

from src.database import Base, engine, SessionLocal
from src.models import Event, Statistics

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment once for all tests"""
    print("\n" + "="*70)
    print("SETTING UP TEST ENVIRONMENT")
    print("="*70)
    
    # Verify we're using test database
    from src.database import DATABASE_URL
    print(f"Test Database: {DATABASE_URL}")
    assert "test" in DATABASE_URL.lower(), "Should use test database!"
    
    yield
    
    print("\n" + "="*70)
    print("TEARING DOWN TEST ENVIRONMENT")
    print("="*70)

@pytest.fixture(scope="function")
def clean_db():
    """Clean database before each test"""
    # Drop all tables
    Base.metadata.drop_all(bind=engine)
    
    # Recreate tables
    Base.metadata.create_all(bind=engine)
    
    # Clear any data
    db = SessionLocal()
    try:
        db.query(Event).delete()
        db.query(Statistics).delete()
        db.commit()
    finally:
        db.close()
    
    yield
    
    # Cleanup after test
    db = SessionLocal()
    try:
        db.query(Event).delete()
        db.query(Statistics).delete()
        db.commit()
    finally:
        db.close()