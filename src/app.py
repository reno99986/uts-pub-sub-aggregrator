from flask import Flask, request, jsonify
from flask_cors import CORS
from pydantic import ValidationError
import asyncio
import threading
from datetime import datetime
import logging
import os
import traceback

# Setup logging FIRST
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger(__name__)

# Import database and models BEFORE init
from src.database import init_db, SessionLocal
from src.models import Event, Statistics
from src.schemas import EventSchema, EventBatchSchema
from src.event_processor import event_processor

app = Flask(__name__)
CORS(app)

# Initialize database - models must be imported first
logger.info("Initializing database...")
init_db()
logger.info("Database initialized successfully")

# Start event processor in background thread
def run_event_processor():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(event_processor.start())

processor_thread = threading.Thread(target=run_event_processor, daemon=True)
processor_thread.start()

@app.route('/publish', methods=['POST'])
def publish_event():
    """Publish single or batch events
    
    Accepts 3 formats:
    1. Single event: {"topic": "...", "event_id": "...", ...}
    2. Batch with wrapper: {"events": [{...}, {...}]}
    3. Batch as array: [{...}, {...}]
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        events = []
        
        # Determine the format and parse accordingly
        if isinstance(data, list):
            # Format 3: Direct array [{...}, {...}]
            logger.debug(f"Processing batch array with {len(data)} events")
            
            for i, event_data in enumerate(data):
                try:
                    event = EventSchema(**event_data)
                    events.append(event)
                except ValidationError as e:
                    logger.error(f"Validation error in array item {i}: {e.errors()}")
                    return jsonify({
                        "error": f"Validation failed for event at index {i}",
                        "details": [
                            {
                                "field": ".".join(str(loc) for loc in err["loc"]),
                                "message": err["msg"],
                                "type": err["type"]
                            }
                            for err in e.errors()
                        ]
                    }), 400
                except Exception as e:
                    logger.error(f"Error parsing event at index {i}: {e}")
                    return jsonify({
                        "error": f"Failed to parse event at index {i}",
                        "message": str(e)
                    }), 400
            
            logger.info(f"Validated batch array with {len(events)} events")
                
        elif isinstance(data, dict):
            if 'events' in data:
                # Format 2: {"events": [{...}, {...}]}
                logger.debug(f"Processing batch object")
                
                try:
                    batch = EventBatchSchema(**data)
                    events = batch.events
                    logger.info(f"Validated batch object with {len(events)} events")
                except ValidationError as e:
                    logger.error(f"Validation error in batch object: {e.errors()}")
                    return jsonify({
                        "error": "Validation failed for batch object",
                        "details": [
                            {
                                "field": ".".join(str(loc) for loc in err["loc"]),
                                "message": err["msg"],
                                "type": err["type"]
                            }
                            for err in e.errors()
                        ]
                    }), 400
                except Exception as e:
                    logger.error(f"Error parsing batch: {e}")
                    return jsonify({
                        "error": "Failed to parse batch",
                        "message": str(e)
                    }), 400
            else:
                # Format 1: Single event {"topic": "...", ...}
                logger.debug("Processing single event")
                
                try:
                    event = EventSchema(**data)
                    events = [event]
                    logger.info("Validated single event")
                except ValidationError as e:
                    logger.error(f"Validation error in single event: {e.errors()}")
                    return jsonify({
                        "error": "Validation failed for single event",
                        "details": [
                            {
                                "field": ".".join(str(loc) for loc in err["loc"]),
                                "message": err["msg"],
                                "type": err["type"]
                            }
                            for err in e.errors()
                        ]
                    }), 400
                except Exception as e:
                    logger.error(f"Error parsing event: {e}")
                    return jsonify({
                        "error": "Failed to parse event",
                        "message": str(e)
                    }), 400
        else:
            return jsonify({
                "error": "Invalid data format. Expected object or array"
            }), 400
        
        if not events:
            return jsonify({"error": "No valid events to process"}), 400
        
        # Add events to processor queue
        loop = asyncio.new_event_loop()
        try:
            for event in events:
                loop.run_until_complete(event_processor.add_event(event.model_dump()))
        finally:
            loop.close()
        
        return jsonify({
            "status": "accepted",
            "count": len(events),
            "message": f"{len(events)} event(s) queued for processing"
        }), 202
        
    except ValueError as e:
        # Handle JSON decode errors
        logger.error(f"JSON decode error: {e}")
        return jsonify({
            "error": "Invalid JSON format",
            "message": str(e)
        }), 400
        
    except Exception as e:
        logger.error(f"Unexpected error in publish endpoint: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/events', methods=['GET'])
def get_events():
    """Get processed events filtered by topic"""
    try:
        topic = request.args.get('topic')
        limit = request.args.get('limit', type=int, default=100)
        
        db = SessionLocal()
        try:
            query = db.query(Event)
            
            if topic:
                query = query.filter(Event.topic == topic)
            
            events = query.order_by(Event.received_at.desc()).limit(limit).all()
            
            return jsonify({
                "count": len(events),
                "events": [event.to_dict() for event in events]
            }), 200
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in get_events endpoint: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get aggregator statistics"""
    try:
        db = SessionLocal()
        try:
            stats = db.query(Statistics).first()
            
            if not stats:
                stats = Statistics(received_count=0, unique_processed=0, duplicate_dropped=0)
            
            # Get unique topics
            topics = db.query(Event.topic).distinct().all()
            topic_list = [t[0] for t in topics]
            
            return jsonify({
                "received": stats.received_count,
                "unique_processed": stats.unique_processed,
                "duplicate_dropped": stats.duplicate_dropped,
                "topics": topic_list,
                "uptime": event_processor.get_uptime(),
                "queue_size": event_processor.queue_size(),
                "batch_size": event_processor.batch_size
            }), 200
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in get_stats endpoint: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat(),
        "queue_size": event_processor.queue_size()
    }), 200

if __name__ == '__main__':
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("DEBUG", "True").lower() == "true"
    app.run(host='0.0.0.0', port=port, debug=debug)