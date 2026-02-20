from __future__ import annotations

import json
import logging
import os
import re
import signal
import time

from confluent_kafka import KafkaError
from prometheus_client import start_http_server, Counter, Histogram

from shared.kafka_utils import (
    connect_kafka_consumer,
    get_producer,
    publish_reasoning,
)
from shared.redis_utils import connect_redis, publish_whisper

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [negotiator] %(levelname)s: %(message)s"
)
logger = logging.getLogger("negotiator")

TENANT_ID = os.getenv("TENANT_ID", "default")
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


_TIME_PATTERN = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*(?:(am|pm))", re.IGNORECASE)
_PARTY_SIZE_PATTERN = re.compile(
    r"(?:party\s+of|table\s+for|group\s+of|seating\s+for|seat\s+for|seats?\s+for|for\s+a\s+party\s+of)\s+(\d{1,2})",
    re.IGNORECASE,
)


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
        time_match = _TIME_PATTERN.search(text_lower)
        if time_match:
            hour = int(time_match.group(1))
            minutes = time_match.group(2) or "00"
            period = time_match.group(3).upper()
            requested_time = f"{hour}:{minutes} {period}"
        intent["details"]["requested_time"] = requested_time

        party_size = None
        size_match = _PARTY_SIZE_PATTERN.search(text_lower)
        if size_match:
            size = int(size_match.group(1))
            if 1 <= size <= 20:
                party_size = size
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
                "revenue_score": len(REVENUE_PRIORITY) - REVENUE_PRIORITY.index(seating_type),
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


_shutdown = False


def _handle_sigterm(_signo: int, _frame: object) -> None:
    global _shutdown
    _shutdown = True
    logger.info("SIGTERM received, shutting down gracefully...")


def run() -> None:
    start_http_server(8002)
    logger.info("Prometheus metrics server started on :8002")

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    r = connect_redis()
    consumer = connect_kafka_consumer("negotiator-agent")
    producer = get_producer()

    logger.info("Negotiator agent running. Listening for transcripts...")

    while not _shutdown:
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

            publish_whisper(r, "negotiator", "strategy_hint", session_id, strategy)
            STRATEGIES_PUBLISHED.inc()

            elapsed_ms = (time.time() - process_start) * 1000
            ANALYSIS_LATENCY.observe(elapsed_ms / 1000)

            reasoning = {
                "input_text": text[:200],
                "detected_intent": intent,
                "strategy": strategy,
                "processing_time_ms": round(elapsed_ms, 2),
            }
            publish_reasoning(producer, "negotiator", session_id, reasoning)

            logger.info("Analyzed in %.1fms -> %s", elapsed_ms, strategy["action"])

        except Exception as exc:
            logger.error("Error processing message: %s", exc)

    consumer.close()
    logger.info("Negotiator agent shut down cleanly")


if __name__ == "__main__":
    run()
