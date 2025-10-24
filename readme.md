# Event Aggregator Service

Layanan aggregator event berbasis Python dengan Flask, SQLAlchemy, dan asyncio yang mendukung deduplication, idempotency, dan high-throughput processing.

## 📋 Fitur Utama

- ✅ **Event Publishing**: Single & batch event ingestion
- ✅ **Deduplication**: Automatic duplicate detection berdasarkan (topic, event_id)
- ✅ **Idempotency**: Safe retry mechanism untuk at-least-once delivery
- ✅ **Persistence**: SQLite database dengan volume mounting
- ✅ **Crash Tolerance**: Data persisten setelah container restart
- ✅ **Async Processing**: High-performance event processing dengan asyncio
- ✅ **Real-time Statistics**: Live monitoring untuk received, processed, dan duplicates
- ✅ **Docker Support**: Containerized deployment dengan docker-compose
- ✅ **Performance Tested**: Proven dengan 5000+ events dan 20% duplikasi rate

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (untuk local development)

### Menggunakan Docker (Recommended)

```bash
# Clone repository
git clone https://github.com/reno99986/uts-pub-sub-aggregrator
cd UTS_AGGREGATOR

# Build dan run container
docker-compose up --build

# Atau run di background
docker-compose up -d

# Check logs
docker-compose logs -f aggregator
```

Server akan berjalan di `http://localhost:5000`

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run aplikasi
python -m src.app
```

## 📡 API Endpoints

### 1. Publish Event (Single)

```bash
curl -X POST http://localhost:5000/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "user.login",
    "event_id": "evt-001",
    "timestamp": "2025-10-24T10:00:00Z",
    "source": "web-app",
    "payload": {
      "user_id": "user-123",
      "ip": "192.168.1.1"
    }
  }'
```


### 2. Publish Batch Events

```bash
curl -X POST http://localhost:5000/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "order.created",
        "event_id": "order-001",
        "timestamp": "2025-10-24T10:00:00Z",
        "source": "order-service",
        "payload": {"order_id": 1, "amount": 100}
      },
      {
        "topic": "order.created",
        "event_id": "order-002",
        "timestamp": "2025-10-24T10:01:00Z",
        "source": "order-service",
        "payload": {"order_id": 2, "amount": 250}
      }
    ]
  }'
```


### 3. Query Events

**Get all events:**
```bash
curl http://localhost:5000/events
```

**Filter by topic:**
```bash
curl "http://localhost:5000/events?topic=user.login"
```

**Filter by source:**
```bash
curl "http://localhost:5000/events?source=web-app"
```

**Limit results:**
```bash
curl "http://localhost:5000/events?limit=10"
```

**Response:**
```json
{
  "events": [
    {
      "id": 1,
      "topic": "user.login",
      "event_id": "evt-001",
      "timestamp": "2025-10-24T10:00:00Z",
      "source": "web-app",
      "payload": {"user_id": "user-123"},
      "received_at": "2025-10-24T10:00:01.123456"
    }
  ],
  "count": 1
}
```

### 4. Statistics

```bash
curl http://localhost:5000/stats
```

**Response:**
```json
{
  "received_count": 6000,
  "unique_processed": 5000,
  "duplicate_dropped": 1000,
  "deduplication_rate": 16.67,
  "topics": ["user.login", "user.logout", "order.created", "payment.processed"],
  "uptime": "0:15:32.123456"
}
```

### 5. Reset Statistics

```bash
curl -X POST http://localhost:5000/stats/reset
```

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

### Performance Test

Test dengan **5000+ events** dan **20% duplikasi rate**:

```bash
# Install test dependencies
pip install aiohttp

# Make sure server is running
docker-compose up -d

# Run stress test
python tests/stress_test.py
```

**Expected Output:**
```
Sending 6000 events (5000 unique + 1000 duplicates)...

===== STRESS TEST RESULT =====
Total events sent: 6000
Unique processed: 5000
Duplicates detected: 1000
Timeouts: 0
Errors: 0
Total time: 35.45 seconds
Throughput: 710.06 events/sec
Average response time: 1.41 ms
```

## 🏗️ Arsitektur

### Technology Stack

- **Framework**: Flask 3.0.0
- **Database**: SQLite with SQLAlchemy 2.0.23
- **Async Processing**: Python asyncio
- **Validation**: Pydantic 2.5.0
- **Testing**: pytest, pytest-asyncio
- **Containerization**: Docker

### Deduplication Strategy

```python
# Unique constraint pada database
UniqueConstraint('topic', 'event_id', name='uq_topic_event')
```

**Cara Kerja:**
1. Event masuk dengan (topic, event_id)
2. Check database apakah kombinasi sudah exist
3. Jika exist → Return 409 Conflict, increment duplicate counter
4. Jika baru → Insert ke database, increment processed counter

**Keuntungan:**
- ✅ Atomic operation (database-level constraint)
- ✅ Crash-safe (persisten di disk)
- ✅ Fast lookup dengan index
- ✅ No race condition

### Data Persistence

**Volume Mounting:**
```yaml
volumes:
  - ./data:/app/data
```

- Database file: `./data/aggregator.db`
- Survive container restart
- Easy backup & migration

### Event Processing Flow

```
Client Request
    ↓
Flask Endpoint (/publish)
    ↓
Validation (Pydantic)
    ↓
Database Check (Dedup)
    ↓
├─→ Duplicate? → Return 409 + Stats++
└─→ New? → Insert DB + Return 201 + Stats++
```

## 🔧 Configuration

### Environment Variables

```bash
# docker-compose.yml
environment:
  - BATCH_SIZE=100              # Batch processing size
  - LOG_LEVEL=INFO             # Logging level (DEBUG/INFO/WARNING/ERROR)
  - DATABASE_PATH=/app/data/aggregator.db  # Database location
```

### Performance Tuning

**Untuk high-load scenario:**

1. **Increase batch size:**
```yaml
environment:
  - BATCH_SIZE=500
```

2. **Scale horizontally** (multiple containers):
```yaml
deploy:
  replicas: 3
```

3. **Use PostgreSQL** untuk production:
```python
DATABASE_URL = "postgresql://user:pass@localhost/aggregator"
```

## 📊 Monitoring

### Health Check

```bash
curl http://localhost:5000/stats
```

### Docker Logs

```bash
# Follow logs
docker-compose logs -f aggregator

# Last 100 lines
docker-compose logs --tail=100 aggregator
```

### Database Inspection

```bash
# Enter container
docker-compose exec aggregator sh

# Check database
sqlite3 /app/data/aggregator.db

# Query statistics
SELECT * FROM statistics;
SELECT COUNT(*) FROM events;
SELECT topic, COUNT(*) FROM events GROUP BY topic;
```

## 🐳 Docker Commands

```bash
# Build image
docker-compose build

# Start service
docker-compose up -d

# Stop service
docker-compose down

# View logs
docker-compose logs -f

# Restart service
docker-compose restart

# Remove volumes (CAUTION: deletes data!)
docker-compose down -v

# Rebuild without cache
docker-compose build --no-cache
```

## 📁 Project Structure

```
uts-pub-sub-aggregrator/
├── src/
│   ├── __init__.py
│   ├── app.py                 # Flask application & routes
│   ├── database.py            # Database configuration
│   ├── models.py              # SQLAlchemy models
│   ├── schemas.py             # Pydantic schemas
│   └── event_processor.py     # Async event processing
├── tests/
│   ├── __init__.py
│   ├── test_api.py           # API endpoint tests
│   ├── test_deduplication.py # Dedup logic tests
│   └── stress_test.py        # Performance test
├── data/
│   └── aggregator.db         # SQLite database (generated)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 🎯 Key Features Explained

### 1. Idempotency

Sistem mendukung **idempotent operations** dimana:
- Client dapat mengirim event yang sama berkali-kali
- Server hanya akan menyimpan **satu kali**
- Response 409 untuk duplicate tidak dianggap error
- Safe untuk retry mechanism

**Use case:**
```python
# Client retry dengan event_id yang sama
for attempt in range(3):
    response = send_event(event_id="evt-001")
    if response.status_code in [201, 409]:  # Success or duplicate = OK
        break
```

### 2. At-Least-Once Delivery

- Event dapat dikirim multiple kali (network retry)
- Aggregator menjamin **no data loss**
- Deduplication mencegah double processing

### 3. Crash Tolerance

**Skenario:**
1. Container crash saat processing
2. Restart container: `docker-compose restart`
3. Data masih ada di `./data/aggregator.db`
4. Statistics tetap akurat

### 4. Partial Ordering

- Event dalam **topic yang sama** diurutkan berdasarkan `received_at`
- Event **antar topic berbeda** tidak dijamin order
- Cocok untuk distributed event aggregation

**Query ordered events:**
```bash
curl "http://localhost:5000/events?topic=user.login" | jq '.events | sort_by(.received_at)'
```

## 🚨 Troubleshooting

### Database Permission Error

```bash
# Fix permissions
mkdir -p data
chmod 755 data

# Rebuild
docker-compose down
docker-compose up --build
```

### Port Already in Use

```bash
# Check port 5000
netstat -ano | findstr :5000

# Change port in docker-compose.yml
ports:
  - "5000:5000"
```


## 🔗 References

- [Flask Documentation](https://flask.palletsprojects.com/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Docker Documentation](https://docs.docker.com/)

## Youtube Video
- (https://youtu.be/TU7RK5sRyAE)

