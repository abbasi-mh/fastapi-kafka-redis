import json
import time
from typing import Optional

from fastapi import FastAPI
from redis import Redis
from kafka import KafkaProducer

app = FastAPI()

redis_client: Optional[Redis] = None
producer: Optional[KafkaProducer] = None


def connect_redis(max_retries: int = 30, sleep_s: float = 1.0) -> Redis:
    last_err = None
    for _ in range(max_retries):
        try:
            r = Redis(host="redis", port=6379, decode_responses=True)
            r.ping()
            return r
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)
    raise RuntimeError(f"Redis not reachable after retries: {last_err}") from last_err


def connect_kafka(max_retries: int = 60, sleep_s: float = 1.0) -> KafkaProducer:
    last_err = None
    for _ in range(max_retries):
        try:
            p = KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # These help avoid long hangs
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
            )
            # Force a metadata fetch to verify connectivity
            p.bootstrap_connected()
            return p
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)
    raise RuntimeError(f"Kafka not reachable after retries: {last_err}") from last_err


@app.on_event("startup")
def startup():
    global redis_client, producer
    redis_client = connect_redis()
    producer = connect_kafka()


@app.get("/health")
def health():
    ok_redis = False
    ok_kafka = False

    try:
        if redis_client is not None:
            redis_client.ping()
            ok_redis = True
    except Exception:
        ok_redis = False

    try:
        if producer is not None:
            ok_kafka = bool(producer.bootstrap_connected())
    except Exception:
        ok_kafka = False

    return {"redis": ok_redis, "kafka": ok_kafka}


@app.post("/login/{user_id}")
def login(user_id: str):
    # Store a simple session in Redis (1 hour)
    assert redis_client is not None
    redis_client.setex(f"session:{user_id}", 3600, "active")
    return {"ok": True, "user_id": user_id}


@app.get("/session/{user_id}")
def session(user_id: str):
    assert redis_client is not None
    return {
        "user_id": user_id,
        "active": redis_client.get(f"session:{user_id}") is not None,
    }


@app.post("/order/{order_id}")
def order(order_id: str):
    assert producer is not None
    producer.send("orders", {"order_id": order_id, "status": "placed"})
    producer.flush()
    return {"ok": True, "topic": "orders", "order_id": order_id}
