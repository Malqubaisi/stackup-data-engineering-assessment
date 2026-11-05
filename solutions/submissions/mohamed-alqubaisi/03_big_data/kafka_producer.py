
import json
import time
from pathlib import Path
from datetime import datetime, timezone

from kafka import KafkaProducer

# Repository root
ROOT = Path(__file__).resolve().parents[4]

# January 2025 event file
EVENTS_FILE = (
    ROOT
    / "datasets"
    / "events_stream"
    / "events_2025_01.jsonl"
)

TOPIC = "presight.project.events"

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(
        value
    ).encode("utf-8"),
    acks="all",
    retries=3,
)

print("=" * 50)
print("TASK 3.2b - KAFKA EVENT PRODUCER")
print("=" * 50)

start_time = time.time()
sent_count = 0

try:
    with open(EVENTS_FILE, "r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            event = json.loads(line)

            # Add production timestamp
            event["produced_at"] = (
                datetime.now(timezone.utc).isoformat()
            )

            # Send event_type as Kafka message key
            future = producer.send(
                TOPIC,
                key=event["event_type"],
                value=event,
            )

            # Confirm broker acknowledgement
            future.get(timeout=30)
            sent_count += 1

            # Log every 100th message
            if sent_count % 100 == 0:
                print(f"Messages sent: {sent_count}")

            # Simulate streaming throughput
            time.sleep(0.05)

finally:
    producer.flush()
    producer.close()

elapsed = time.time() - start_time

print("\n" + "=" * 50)
print("KAFKA PRODUCER SUMMARY")
print("=" * 50)
print("Topic:", TOPIC)
print("Total messages sent:", sent_count)
print("Execution time:", round(elapsed, 2), "seconds")
print(
    "Messages per second:",
    round(sent_count / elapsed, 2) if elapsed else 0
)

print("KAFKA PRODUCER COMPLETED SUCCESSFULLY!")
