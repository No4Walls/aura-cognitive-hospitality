from __future__ import annotations

import json
import logging
import os
import time

import redis
from confluent_kafka import Consumer, Producer, KafkaError
from prometheus_client import start_http_server, Counter, Histogram

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [negotiator] %(levelname)s: %(message)s"
)
logger = logging.getLogger("negotiator")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TENANT_ID = os.getenv("TENANT_ID", "default")
TRANSCRIPT_TOPIC = "swarm.transcripts"
REASONING_TOPIC = "swarm.reasoning"
WHISPER_STREAM_KEY = "aura:whisper_bus"
INVENTORY_KEY = "aura:inventory"

AVAILABLE_SLOTS = {
    "friday": ["6:00 PM", "6:30 PM", "8:30 PM", "9:00 PM"],
    "saturday": ["5:30 PM", "8:00 PM", "9:30 PM"],
    "sunday": ["6:00 PM", "7:00 PM", "7:30 PM", "8:00 PM"],
    "monday": ["6:00 PM", "6:30 PM", "7:00 PM", "7:30 PM", "8:00 PM", "8:30 PM"],
    "tuesday": ["6:00 PM", "6:30 PM", "7:00 PM", "7:30 PM", "8:00 PM", "8:30 PM"],
    "wednesday": ["6:00 PM", "6:30 PM", "7:00 PM", "7:30 PM", "8:00 PM"],
    "thursday": ["6:00 PM", "7:00 PM", "8:00 PM", "8:30 PM"],
}

PEAK_HOURS = {"7:00 PM", "7:30 PM", "8:00 PM"}

SEATING_TYPES = {
    "dining_room": {"capacity": 60, "label": "Main Dining Room"},
    "bar": {"capacity": 12, "label": "Bar Seating"},
    "high_top": {"capacity": 16, "label": "High-Top Tables"},
    "patio": {"capacity": 24, "label": "Garden Patio"},
    "private": {"capacity": 10, "label": "Private Dining Room"},
}

REVENUE_PRIORITY = ["private", "dining_room", "patio", "high_top", "bar"]

STRATEGIES_PUBLISHED = Counter(
    "negotiator_strategies_published_total",
    "Strategy hints published by the Negotiator",
)
ANALYSIS_LATENCY = Histogram(
    "negotiator_analysis_latency_seconds",
    "Intent analysis latency",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
)
TRANSCRIPTS_PROCESSED = Counter(
    "negotiator_transcripts_processed_total",
    "Total transcripts analyzed by Negotiator",
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
                    "group.id": "negotiator-agent",
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


def analyze_intent(text: str) -> dict | None:
    text_lower = text.lower()
    intent: dict = {"type": None, "details": {}}

    if any(kw in text_lower for kw in ["reservation", "book", "table", "reserve", "seat"]):
        intent["type"] = "reservation_request"

        requested_day = None
        for day in AVAILABLE_SLOTS:
            if day in text_lower:
                requested_day = day
                break
        intent["details"]["requested_day"] = requested_day

        requested_time = None
        if "7" in text_lower and ("pm" in text_lower or "evening" in text_lower):
            requested_time = "7:00 PM"
        elif "8" in text_lower:
            requested_time = "8:00 PM"
        elif "6" in text_lower:
            requested_time = "6:00 PM"
        elif "9" in text_lower:
            requested_time = "9:00 PM"
        intent["details"]["requested_time"] = requested_time

        party_size = None
        for word in text_lower.split():
            if word.isdigit() and 1 <= int(word) <= 20:
                party_size = int(word)
                break
        intent["details"]["party_size"] = party_size

        return intent

    if any(kw in text_lower for kw in ["cancel", "change", "modify", "reschedule"]):
        intent["type"] = "modification_request"
        return intent

    if any(kw in text_lower for kw in ["special", "birthday", "anniversary", "celebration"]):
        intent["type"] = "special_occasion"
        return intent

    if any(kw in text_lower for kw in ["complaint", "unhappy", "disappointed", "problem"]):
        intent["type"] = "complaint"
        return intent

    if any(kw in text_lower for kw in ["walk in", "walk-in", "walkin", "right now", "available now", "tonight"]):
        intent["type"] = "walk_in"
        return intent

    return None


def _find_seating_alternatives(day: str, requested_time: str | None) -> list[dict]:
    slots = AVAILABLE_SLOTS.get(day, [])
    alternatives: list[dict] = []

    for seating_type in REVENUE_PRIORITY:
        info = SEATING_TYPES[seating_type]
        if seating_type == "dining_room":
            continue
        off_peak = [s for s in slots if s not in PEAK_HOURS]
        available = off_peak if off_peak else slots
        for slot in available[:2]:
            alternatives.append({
                "time": slot,
                "seating": info["label"],
                "seating_type": seating_type,
                "revenue_score": REVENUE_PRIORITY.index(seating_type) + 1,
            })
        if len(alternatives) >= 4:
            break

    if requested_time:
        nearby_times = [s for s in slots if s != requested_time]
        for t in nearby_times[:2]:
            alternatives.append({
                "time": t,
                "seating": SEATING_TYPES["dining_room"]["label"],
                "seating_type": "dining_room",
                "revenue_score": 1,
            })

    return alternatives


def build_strategy(intent: dict) -> dict | None:
    intent_type = intent.get("type")
    details = intent.get("details", {})

    if intent_type == "reservation_request":
        day = details.get("requested_day")
        requested_time = details.get("requested_time")
        party_size = details.get("party_size")

        if day and requested_time:
            slots = AVAILABLE_SLOTS.get(day, [])
            if requested_time in slots:
                return {
                    "suggestion": f"Confirm {day.title()} at {requested_time}. Slot is available.",
                    "action": "confirm_booking",
                    "confidence": 0.95,
                    "alternatives": [],
                    "seating": "dining_room",
                }
            else:
                seat_alts = _find_seating_alternatives(day, requested_time)
                alt_descriptions = []
                for a in seat_alts[:3]:
                    alt_descriptions.append(f"{a['time']} at the {a['seating']}")

                return {
                    "suggestion": (
                        f"{requested_time} on {day.title()} is fully booked. "
                        f"Revenue-positive alternatives: {'; '.join(alt_descriptions)}."
                    ),
                    "action": "offer_alternative",
                    "confidence": 0.85,
                    "alternatives": seat_alts[:3],
                    "full_house": True,
                }

        if day and not requested_time:
            slots = AVAILABLE_SLOTS.get(day, [])
            off_peak = [s for s in slots if s not in PEAK_HOURS]
            recommended = off_peak[0] if off_peak else (slots[0] if slots else "6:00 PM")

            if party_size and party_size >= 6:
                return {
                    "suggestion": (
                        f"Large party ({party_size}) for {day.title()}. "
                        f"Recommend {recommended} at the {SEATING_TYPES['private']['label']} for premium experience."
                    ),
                    "action": "suggest_private",
                    "confidence": 0.88,
                    "alternatives": [{"time": recommended, "seating": "Private Dining Room", "seating_type": "private"}],
                }

            return {
                "suggestion": f"Guest wants {day.title()}. Recommend {recommended} to optimize table turnover.",
                "action": "suggest_time",
                "confidence": 0.80,
                "alternatives": [{"time": s, "seating": "Main Dining Room"} for s in slots[:3]],
            }

        return {
            "suggestion": "Guest wants a reservation. Ask for preferred day and time to find the best slot.",
            "action": "gather_details",
            "confidence": 0.70,
            "alternatives": [],
        }

    if intent_type == "walk_in":
        return {
            "suggestion": (
                "Walk-in detected on a busy night. "
                f"Offer {SEATING_TYPES['bar']['label']} or {SEATING_TYPES['high_top']['label']} for immediate seating."
            ),
            "action": "offer_immediate_seating",
            "confidence": 0.80,
            "alternatives": [
                {"seating": SEATING_TYPES["bar"]["label"], "seating_type": "bar", "wait_time": "0 min"},
                {"seating": SEATING_TYPES["high_top"]["label"], "seating_type": "high_top", "wait_time": "0 min"},
                {"seating": SEATING_TYPES["dining_room"]["label"], "seating_type": "dining_room", "wait_time": "25-35 min"},
            ],
        }

    if intent_type == "modification_request":
        return {
            "suggestion": "Guest wants to modify. Confirm existing reservation details before making changes.",
            "action": "confirm_existing",
            "confidence": 0.80,
            "alternatives": [],
        }

    if intent_type == "special_occasion":
        return {
            "suggestion": (
                "Special occasion detected. Offer complimentary champagne and "
                f"the {SEATING_TYPES['private']['label']} for an exclusive experience."
            ),
            "action": "upsell_experience",
            "confidence": 0.90,
            "alternatives": [{"seating": SEATING_TYPES["private"]["label"], "seating_type": "private"}],
        }

    if intent_type == "complaint":
        return {
            "suggestion": "Complaint detected. Prioritize empathy, offer to connect with manager if needed.",
            "action": "escalate_care",
            "confidence": 0.85,
            "alternatives": [],
        }

    return None


def publish_whisper(r: redis.Redis, session_id: str, strategy: dict) -> None:
    entry = {
        "agent": "negotiator",
        "whisper_type": "strategy_hint",
        "session_id": session_id,
        "payload": json.dumps(strategy),
        "timestamp": str(time.time()),
    }
    r.xadd(WHISPER_STREAM_KEY, entry)
    STRATEGIES_PUBLISHED.inc()
    logger.info("Strategy whisper published for session %s: %s", session_id, strategy["action"])


def publish_reasoning(producer: Producer, session_id: str, reasoning: dict) -> None:
    event = {
        "agent": "negotiator",
        "session_id": session_id,
        "reasoning": reasoning,
        "timestamp": time.time(),
    }
    producer.produce(
        REASONING_TOPIC,
        key=session_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
    )
    producer.flush()


def run() -> None:
    start_http_server(8002)
    logger.info("Prometheus metrics server started on :8002")

    r = connect_redis()
    consumer = connect_kafka()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    logger.info("Negotiator agent running. Listening for transcripts...")

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
            text = value.get("text", "")
            direction = value.get("direction", "")

            TRANSCRIPTS_PROCESSED.inc()

            if direction != "inbound":
                continue

            process_start = time.time()

            intent = analyze_intent(text)
            if not intent:
                continue

            strategy = build_strategy(intent)
            if not strategy:
                continue

            publish_whisper(r, session_id, strategy)

            elapsed_ms = (time.time() - process_start) * 1000
            ANALYSIS_LATENCY.observe(elapsed_ms / 1000)

            reasoning = {
                "input_text": text[:200],
                "detected_intent": intent,
                "strategy": strategy,
                "processing_time_ms": round(elapsed_ms, 2),
            }
            publish_reasoning(producer, session_id, reasoning)

            logger.info("Analyzed in %.1fms -> %s", elapsed_ms, strategy["action"])

        except Exception as exc:
            logger.error("Error processing message: %s", exc)


if __name__ == "__main__":
    run()
