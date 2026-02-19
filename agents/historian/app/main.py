from __future__ import annotations

import json
import logging
import os
import time

import redis
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Counter, Histogram

from historian.app.vector_memory import (
    ensure_vector_index,
    recall_deep_context,
    store_transcript_embedding,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [historian] %(levelname)s: %(message)s")
logger = logging.getLogger("historian")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TENANT_ID = os.getenv("TENANT_ID", "default")
TRANSCRIPT_TOPIC = "swarm.transcripts"
REASONING_TOPIC = "swarm.reasoning"
WHISPER_STREAM_KEY = "aura:whisper_bus"
GUEST_KEY_PREFIX = "aura:guest:"

WHISPERS_PUBLISHED = Counter(
    "historian_whispers_published_total",
    "Whispers published by the Historian",
)
LOOKUP_LATENCY = Histogram(
    "historian_lookup_latency_seconds",
    "VIP vault lookup latency",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.15],
)
TRANSCRIPTS_PROCESSED = Counter(
    "historian_transcripts_processed_total",
    "Total transcripts consumed from Kafka",
)
VECTOR_RECALLS = Counter(
    "historian_vector_recalls_total",
    "Successful vector memory recalls",
)
EMBEDDINGS_STORED = Counter(
    "historian_embeddings_stored_total",
    "Transcript embeddings stored to vector index",
)


def connect_redis() -> redis.Redis:
    for attempt in range(30):
        try:
            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            client.ping()
            logger.info("Connected to Redis")
            return client
        except redis.ConnectionError:
            logger.warning("Redis not ready, retrying... (attempt %d/30)", attempt + 1)
            time.sleep(2)
    raise ConnectionError("Failed to connect to Redis")


def connect_kafka() -> Consumer:
    for attempt in range(30):
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "group.id": "historian-agent",
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": True,
                }
            )
            consumer.subscribe([TRANSCRIPT_TOPIC])
            logger.info("Subscribed to Kafka topic: %s", TRANSCRIPT_TOPIC)
            return consumer
        except Exception:
            logger.warning("Kafka not ready, retrying... (attempt %d/30)", attempt + 1)
            time.sleep(2)
    raise ConnectionError("Failed to connect to Kafka")


def lookup_guest(r: redis.Redis, phone: str) -> dict | None:
    start = time.time()
    try:
        key = f"{GUEST_KEY_PREFIX}{phone}"
        result = r.json().get(key, "$")
        elapsed = time.time() - start
        LOOKUP_LATENCY.observe(elapsed)
        if result and len(result) > 0:
            logger.info("VIP found: %s (%.1fms)", phone, elapsed * 1000)
            return result[0]
        logger.info("No VIP profile for %s (%.1fms)", phone, elapsed * 1000)
    except Exception as exc:
        elapsed = time.time() - start
        LOOKUP_LATENCY.observe(elapsed)
        logger.warning("VIP lookup error for %s: %s", phone, exc)
    return None


def extract_context(text: str, profile: dict | None) -> dict | None:
    text_lower = text.lower()
    context = {"keywords": [], "summary": ""}

    if profile:
        name = profile.get("name", "Guest")
        tags = profile.get("preference_tags", [])
        context["guest_name"] = name
        context["preferences"] = tags

        if any(kw in text_lower for kw in ["wine", "drink", "beverage", "glass"]):
            wine_prefs = [t for t in tags if "wine" in t.lower()]
            context["keywords"].append("wine_mention")
            context["summary"] = (
                f"{name} is asking about drinks. Known preferences: {', '.join(wine_prefs) if wine_prefs else 'none recorded'}."
            )
            return context

        if any(kw in text_lower for kw in ["table", "reservation", "book", "seat"]):
            table_notes = profile.get("notes", {}).get("preferred_table", "")
            context["keywords"].append("reservation_intent")
            context["summary"] = (
                f"{name} wants a reservation. Preferred table: {table_notes or 'not specified'}."
            )
            return context

        if any(kw in text_lower for kw in ["usual", "same", "regular", "always"]):
            context["keywords"].append("habitual_request")
            context["summary"] = (
                f"{name} is requesting their usual. Tags: {', '.join(tags)}."
            )
            return context

        context["summary"] = f"Returning guest {name}. Preferences: {', '.join(tags)}."
        return context

    if any(kw in text_lower for kw in ["reservation", "book", "table"]):
        context["keywords"].append("reservation_intent")
        context["summary"] = "New caller requesting a reservation."
        return context

    return None


def publish_whisper(
    r: redis.Redis,
    session_id: str,
    payload: dict,
) -> None:
    entry = {
        "agent": "historian",
        "whisper_type": "guest_context",
        "session_id": session_id,
        "payload": json.dumps(payload),
        "timestamp": str(time.time()),
    }
    r.xadd(WHISPER_STREAM_KEY, entry, maxlen=10000, approximate=True)
    session_key = f"aura:whispers:{session_id}"
    r.rpush(session_key, json.dumps(entry))
    r.expire(session_key, 300)
    WHISPERS_PUBLISHED.inc()
    logger.info("Whisper published for session %s", session_id)


def publish_reasoning(producer_ref: list, session_id: str, reasoning: dict) -> None:
    from confluent_kafka import Producer

    if not producer_ref:
        producer_ref.append(Producer({"bootstrap.servers": KAFKA_BOOTSTRAP}))
    p = producer_ref[0]
    event = {
        "agent": "historian",
        "session_id": session_id,
        "reasoning": reasoning,
        "timestamp": time.time(),
    }
    p.produce(
        REASONING_TOPIC,
        key=session_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
    )
    p.flush()


def run() -> None:
    start_http_server(8001)
    logger.info("Prometheus metrics server started on :8001")

    r = connect_redis()
    consumer = connect_kafka()
    producer_ref: list = []

    try:
        ensure_vector_index(r)
        logger.info("Vector memory index ready")
    except Exception as exc:
        logger.warning("Vector index setup deferred: %s", exc)

    logger.info("Historian agent running (tenant=%s). Listening for transcripts...", TENANT_ID)

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Kafka error: %s", msg.error())
            continue

        try:
            value = json.loads(msg.value().decode("utf-8"))
            session_id = value.get("session_id", "")
            caller = value.get("caller_number", "")
            text = value.get("text", "")
            direction = value.get("direction", "")
            tenant_id = value.get("tenant_id", TENANT_ID)

            TRANSCRIPTS_PROCESSED.inc()
            logger.info(
                "Transcript [%s] %s: %s",
                direction,
                caller,
                text[:80],
            )

            try:
                store_transcript_embedding(r, tenant_id, caller, session_id, text, direction)
                EMBEDDINGS_STORED.inc()
            except Exception as exc:
                logger.warning("Embedding storage failed: %s", exc)

            if direction != "inbound":
                continue

            process_start = time.time()

            profile = lookup_guest(r, caller)
            context = extract_context(text, profile)

            deep_memories: list[dict] = []
            try:
                deep_memories = recall_deep_context(r, tenant_id, caller, text, k=5)
                if deep_memories:
                    VECTOR_RECALLS.inc()
                    logger.info("Recalled %d deep memories for %s", len(deep_memories), caller)
            except Exception as exc:
                logger.warning("Deep context recall failed: %s", exc)

            if context:
                if deep_memories:
                    memory_summaries = [
                        m["text"][:100] for m in deep_memories if m.get("text")
                    ]
                    context["deep_memories"] = memory_summaries
                    context["memory_count"] = len(deep_memories)

                publish_whisper(r, session_id, context)

                reasoning = {
                    "input_text": text[:200],
                    "guest_found": profile is not None,
                    "context_generated": context,
                    "deep_memories_found": len(deep_memories),
                    "processing_time_ms": round((time.time() - process_start) * 1000, 2),
                }
                publish_reasoning(producer_ref, session_id, reasoning)

                logger.info(
                    "Processed in %.1fms (memories=%d)",
                    (time.time() - process_start) * 1000,
                    len(deep_memories),
                )

        except Exception as exc:
            logger.error("Error processing message: %s", exc)


if __name__ == "__main__":
    run()
