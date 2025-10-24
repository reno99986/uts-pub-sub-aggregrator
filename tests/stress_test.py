import asyncio
import aiohttp
import uuid
import random
import time
from datetime import datetime, timezone

API_URL = "http://localhost:5000/publish"
TOPICS = ["user.login", "user.logout", "order.created", "payment.processed"]
SOURCES = ["web-app", "mobile-app", "api-gateway", "microservice-1"]

TOTAL_EVENTS = 5000
DUPLICATION_RATE = 0.2
CONCURRENCY_LIMIT = 100

def generate_event(topic=None, source=None):
    return {
        "topic": topic or random.choice(TOPICS),
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source or random.choice(SOURCES),
        "payload": {
            "value": random.randint(0, 100),
            "status": random.choice(["ok", "warn", "error"])
        }
    }

async def send_event(session, event, sem):
    async with sem:
        try:
            async with session.post(API_URL, json=event) as resp:
                if resp.status == 201:
                    return "ok"
                elif resp.status == 409:
                    return "duplicate"
                else:
                    return f"error-{resp.status}"
        except Exception as e:
            return f"fail-{e}"

async def main():
    start_time = time.perf_counter()
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

    # Generate events
    events = [generate_event() for _ in range(TOTAL_EVENTS)]
    duplicates = random.sample(events, int(TOTAL_EVENTS * DUPLICATION_RATE))
    all_events = events + duplicates
    random.shuffle(all_events)
    total_to_send = len(all_events)

    print(f"Sending {len(all_events)} events "
          f"({TOTAL_EVENTS} unique + {len(duplicates)} duplicates)...")

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *(send_event(session, event, sem) for event in all_events)
        )

    end_time = time.perf_counter()
    duration = end_time - start_time

    ok = results.count("ok")
    duplicate = results.count("duplicate")
    errors = len([r for r in results if r.startswith("error") or r.startswith("fail")])

    print("\n===== STRESS TEST RESULT =====")
    print(f"Total events sent: {total_to_send}")
    print(f"Total time: {duration:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())