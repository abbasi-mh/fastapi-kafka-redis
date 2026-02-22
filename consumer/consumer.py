import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

TOPIC = "orders"
BROKER = "kafka:9092"
GROUP_ID = "order-consumers-v3"  # new group id so it reads from beginning


def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
        request_timeout_ms=20000,
        api_version_auto_timeout_ms=10000,
    )


print("Starting Kafka consumer...", flush=True)

consumer = None
for attempt in range(60):
    try:
        consumer = create_consumer()
        print("✅ Connected to Kafka", flush=True)
        break
    except NoBrokersAvailable:
        print(f"⏳ Kafka not ready yet... retrying ({attempt + 1}/60)", flush=True)
        time.sleep(1)

if consumer is None:
    raise RuntimeError("❌ Could not connect to Kafka after retries")

print(f"📥 Listening on topic '{TOPIC}' as group '{GROUP_ID}'", flush=True)

last_print = time.time()

for msg in consumer:
    print("Received order:", msg.value, flush=True)

    # optional: show it's alive even if messages are rare
    now = time.time()
    if now - last_print > 30:
        print("...still running, waiting for messages...", flush=True)
        last_print = now
