from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Determine database URL based on environment
def get_database_url():
    """Get database URL with proper path handling"""
    
    # Check if we're in testing mode
    if os.getenv("TESTING", "False").lower() == "true":
        db_path = "test_aggregator.db"
    else:
        db_path = os.getenv("DATABASE_PATH", "aggregator.db")
    
    # Get absolute path to ensure database is created in correct location
    if not os.path.isabs(db_path):
        # If running from src/ directory, go up one level
        if os.path.basename(os.getcwd()) == 'src':
            base_dir = os.path.dirname(os.getcwd())
        else:
            base_dir = os.getcwd()
        
        # Create data directory if it doesn't exist
        data_dir = os.path.join(base_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        
        # Full path to database file
        db_path = os.path.join(data_dir, db_path)
    
    # Ensure parent directory exists
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        logger.info(f"Created database directory: {db_dir}")
    
    # Convert to SQLite URL format
    db_url = f"sqlite:///{db_path}"
    
    logger.info(f"Using database: {db_path}")
    return db_url

DATABASE_URL = get_database_url()

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
    echo=False,
    pool_pre_ping=True  # Check connection health
)

SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Initialize database - create all tables"""
    try:
        # Import models to register them with Base
        from src import models
        
        logger.info(f"Creating tables in database: {DATABASE_URL}")
        Base.metadata.create_all(bind=engine)
        logger.info("Tables created successfully")
        
        # Initialize statistics if not exists
        db = SessionLocal()
        try:
            stats = db.query(models.Statistics).first()
            if not stats:
                stats = models.Statistics(
                    received_count=0,
                    unique_processed=0,
                    duplicate_dropped=0
                )
                db.add(stats)
                db.commit()
                logger.info("Initialized statistics table")
        except Exception as e:
            logger.error(f"Error initializing statistics: {e}")
            db.rollback()
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def reset_db():
    """Drop all tables and recreate (for testing)"""
    try:
        from src import models
        logger.warning("Dropping all tables...")
        Base.metadata.drop_all(bind=engine)
        logger.info("Recreating tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database reset complete")
    except Exception as e:
        logger.error(f"Error resetting database: {e}")
        raise