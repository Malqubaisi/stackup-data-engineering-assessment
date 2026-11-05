
import json
import time
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

ROOT = Path(__file__).resolve().parents[4]

SOURCE_TOPIC = "presight.project.events"
CRITICAL_TOPIC = "presight.escalations.critical"

EVENTS_FILE = (
    ROOT / "datasets" / "events_stream"
    / "events_2025_01.jsonl"
)

OUTPUT = ROOT / "outputs" / "kafka"
OUTPUT.mkdir(parents=True, exist_ok=True)

# Count expected messages from January's source file
with open(EVENTS_FILE, "r", encoding="utf-8") as file:
    expected_count = sum(
        1 for line in file if line.strip()
    )

consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None,
    key_deserializer=lambda key: (
        key.decode("utf-8") if key else None
    ),
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

critical_producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(
        value
    ).encode("utf-8"),
    acks="all",
    retries=3,
)

print("=" * 50)
print("TASK 3.2 - KAFKA CONSUMER")
print("=" * 50)

print("Source topic:", SOURCE_TOPIC)
print("Expected messages:", expected_count)

start_time = time.time()

event_counts = Counter()
consumed_count = 0
forwarded_count = 0
idle_start = time.time()

try:
    while consumed_count < expected_count:

        batches = consumer.poll(
            timeout_ms=1000,
            max_records=500
        )

        if not batches:
            if time.time() - idle_start > 30:
                raise TimeoutError(
                    f"Only received {consumed_count} "
                    f"of {expected_count} messages"
                )
            continue

        idle_start = time.time()

        for records in batches.values():
            for message in records:

                if consumed_count >= expected_count:
                    break

                event = message.value
                event_type = event.get("event_type")

                consumed_count += 1
                event_counts[event_type] += 1

                # Forward only Critical escalations
                if (
                    event_type == "escalation_raised"
                    and event.get("payload", {}).get(
                        "severity"
                    ) == "Critical"
                ):
                    critical_producer.send(
                        CRITICAL_TOPIC,
                        key=event_type,
                        value=event
                    ).get(timeout=30)

                    forwarded_count += 1

                # Log every 100 messages
                if consumed_count % 100 == 0:
                    print(
                        "Messages consumed:",
                        consumed_count
                    )

finally:
    critical_producer.flush()
    critical_producer.close()
    consumer.close()

elapsed = time.time() - start_time

summary = {
    "run_timestamp": datetime.now(
        timezone.utc
    ).isoformat(),
    "source_topic": SOURCE_TOPIC,
    "total_messages_consumed": consumed_count,
    "count_per_event_type": dict(
        sorted(event_counts.items())
    ),
    "critical_escalations_forwarded": forwarded_count,
    "throughput_messages_per_second": round(
        consumed_count / elapsed, 2
    ) if elapsed else 0,
    "execution_seconds": round(elapsed, 2),
}

summary_path = OUTPUT / "summary.json"

with open(
    summary_path, "w", encoding="utf-8"
) as file:
    json.dump(summary, file, indent=4)

print("\n" + "=" * 50)
print("KAFKA CONSUMER SUMMARY")
print("=" * 50)

print("Messages consumed:", consumed_count)
print("Critical escalations forwarded:", forwarded_count)
print(
    "Throughput:",
    summary["throughput_messages_per_second"],
    "messages/sec"
)
print("Summary saved to:", summary_path)

print("\nCounts per event type:")

for event_type, count in sorted(
    event_counts.items()
):
    print(f"{event_type}: {count}")

print("\nKAFKA CONSUMER COMPLETED SUCCESSFULLY!")
