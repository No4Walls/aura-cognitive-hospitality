from __future__ import annotations

import json
import logging
import os
import time

import redis
from confluent_kafka import Consumer, Producer, KafkaError
from prometheus_client import start_http_server, Counter, Histogram

from sommelier.app.ontology import (
    expand_allergen_to_family,
    get_allergen_family,
    get_flavor_category,
    get_wine_pairings,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [sommelier] %(levelname)s: %(message)s"
)
logger = logging.getLogger("sommelier")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TENANT_ID = os.getenv("TENANT_ID", "default")

TRANSCRIPT_TOPIC = "swarm.transcripts"
REASONING_TOPIC = "swarm.reasoning"
WHISPER_STREAM_KEY = "aura:whisper_bus"
MENU_KEY_PREFIX = "aura:menu:"

FILTERS_PUBLISHED = Counter(
    "sommelier_filters_published_total",
    "Menu filter results published by the Sommelier",
)
PAIRINGS_PUBLISHED = Counter(
    "sommelier_pairings_published_total",
    "Wine pairing suggestions published by the Sommelier",
)
FILTER_LATENCY = Histogram(
    "sommelier_filter_latency_seconds",
    "Ingredient filtering latency",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.15],
)
TRANSCRIPTS_PROCESSED = Counter(
    "sommelier_transcripts_processed_total",
    "Total transcripts analyzed by Sommelier",
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
                    "group.id": "sommelier-agent",
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


def load_menu(r: redis.Redis) -> list[dict]:
    menu_items = []
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"{MENU_KEY_PREFIX}*", count=100)
        for key in keys:
            item = r.json().get(key, "$")
            if item and len(item) > 0:
                menu_items.append(item[0])
        if cursor == 0:
            break
    return menu_items


def detect_intent(text: str) -> dict | None:
    text_lower = text.lower()
    intent: dict = {"type": None, "details": {}}

    exclusion_keywords = [
        "no ", "without ", "allergic to ", "allergy ", "can't eat ", "cannot eat ",
        "don't want ", "avoid ", "intolerant ", "sensitive to ", "free from ",
    ]
    for kw in exclusion_keywords:
        if kw in text_lower:
            idx = text_lower.index(kw) + len(kw)
            remainder = text_lower[idx:].strip().rstrip(".,!?")
            excluded = [w.strip() for w in remainder.split(" and ")]
            if len(excluded) == 1:
                excluded = [w.strip() for w in remainder.split(",")]
            intent["type"] = "ingredient_filter"
            intent["details"]["excluded"] = excluded
            return intent

    pairing_keywords = [
        "pair", "wine", "drink with", "goes with", "complement",
        "recommend a wine", "what wine", "beverage",
    ]
    if any(kw in text_lower for kw in pairing_keywords):
        dish_hint = None
        for phrase in ["with the ", "for the ", "with my ", "for my "]:
            if phrase in text_lower:
                dish_hint = text_lower.split(phrase, 1)[1].strip().rstrip(".,!?")
                break
        intent["type"] = "wine_pairing"
        intent["details"]["dish_hint"] = dish_hint
        return intent

    menu_keywords = [
        "menu", "dish", "what do you have", "recommend", "special",
        "what's good", "suggest", "options",
    ]
    if any(kw in text_lower for kw in menu_keywords):
        intent["type"] = "menu_inquiry"
        return intent

    return None


def filter_menu_by_exclusion(
    menu: list[dict], excluded_terms: list[str]
) -> tuple[list[dict], list[dict], list[str]]:
    expanded_exclusions: set[str] = set()
    reasoning_notes: list[str] = []

    for term in excluded_terms:
        family = get_allergen_family(term)
        if family:
            family_members = expand_allergen_to_family(term)
            expanded_exclusions.update(family_members)
            reasoning_notes.append(
                f"'{term}' belongs to the '{family}' family. "
                f"Expanding filter to include: {', '.join(family_members)}."
            )
        else:
            expanded_exclusions.add(term.lower().strip())
            reasoning_notes.append(
                f"'{term}' has no known allergen family. Filtering as exact ingredient."
            )

    safe_items: list[dict] = []
    excluded_items: list[dict] = []

    for item in menu:
        item_ingredients = {i.lower() for i in item.get("ingredients", [])}
        item_allergens = {a.lower() for a in item.get("allergens", [])}
        item_families = {f.lower() for f in item.get("allergen_family", [])}
        all_item_terms = item_ingredients | item_allergens | item_families

        if expanded_exclusions & all_item_terms:
            excluded_items.append(item)
        else:
            safe_items.append(item)

    return safe_items, excluded_items, reasoning_notes


def find_pairing(menu: list[dict], dish_hint: str | None) -> dict | None:
    if not dish_hint:
        return None

    dish_hint_lower = dish_hint.lower()
    matched_dish = None
    for item in menu:
        if dish_hint_lower in item.get("name", "").lower():
            matched_dish = item
            break
        if any(dish_hint_lower in ing.lower() for ing in item.get("ingredients", [])):
            matched_dish = item
            break

    if not matched_dish:
        return None

    flavor_tags = matched_dish.get("flavor_profile", [])
    category = get_flavor_category(flavor_tags)
    if not category:
        return None

    pairings = get_wine_pairings(category)
    return {
        "dish": matched_dish.get("name", ""),
        "flavor_category": category,
        "pairings": pairings,
    }


def publish_whisper(r: redis.Redis, session_id: str, whisper_type: str, payload: dict) -> None:
    entry = {
        "agent": "sommelier",
        "whisper_type": whisper_type,
        "session_id": session_id,
        "payload": json.dumps(payload),
        "timestamp": str(time.time()),
    }
    r.xadd(WHISPER_STREAM_KEY, entry, maxlen=10000, approximate=True)
    session_key = f"aura:whispers:{session_id}"
    r.rpush(session_key, json.dumps(entry))
    r.expire(session_key, 300)
    logger.info("Whisper published [%s] for session %s", whisper_type, session_id)


def publish_reasoning(producer: Producer, session_id: str, reasoning: dict) -> None:
    event = {
        "agent": "sommelier",
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
    start_http_server(8003)
    logger.info("Prometheus metrics server started on :8003")

    r = connect_redis()
    consumer = connect_kafka()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    logger.info("Sommelier agent running. Listening for transcripts...")

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

            intent = detect_intent(text)
            if not intent:
                continue

            process_start = time.time()
            menu = load_menu(r)
            intent_type = intent["type"]

            if intent_type == "ingredient_filter":
                excluded = intent["details"]["excluded"]
                safe, removed, notes = filter_menu_by_exclusion(menu, excluded)
                elapsed_ms = (time.time() - process_start) * 1000
                FILTER_LATENCY.observe(elapsed_ms / 1000)
                FILTERS_PUBLISHED.inc()

                payload = {
                    "filter_type": "ingredient_exclusion",
                    "excluded_terms": excluded,
                    "safe_items": [i.get("name", "") for i in safe],
                    "removed_items": [i.get("name", "") for i in removed],
                    "safe_count": len(safe),
                    "removed_count": len(removed),
                    "reasoning": notes,
                }
                publish_whisper(r, session_id, "menu_suggestion", payload)

                reasoning = {
                    "input_text": text[:200],
                    "intent": intent,
                    "filter_result": payload,
                    "processing_time_ms": round(elapsed_ms, 2),
                }
                publish_reasoning(producer, session_id, reasoning)
                logger.info(
                    "Filtered menu in %.1fms: %d safe, %d removed",
                    elapsed_ms, len(safe), len(removed),
                )

            elif intent_type == "wine_pairing":
                dish_hint = intent["details"].get("dish_hint")
                pairing = find_pairing(menu, dish_hint)
                elapsed_ms = (time.time() - process_start) * 1000
                PAIRINGS_PUBLISHED.inc()

                if pairing:
                    payload = {
                        "pairing_type": "wine",
                        "dish": pairing["dish"],
                        "flavor_category": pairing["flavor_category"],
                        "recommendations": pairing["pairings"],
                    }
                else:
                    payload = {
                        "pairing_type": "wine",
                        "dish": dish_hint or "unknown",
                        "flavor_category": "unknown",
                        "recommendations": [],
                        "note": "Could not match dish to menu. Suggest asking guest for more detail.",
                    }

                publish_whisper(r, session_id, "menu_suggestion", payload)
                reasoning = {
                    "input_text": text[:200],
                    "intent": intent,
                    "pairing_result": payload,
                    "processing_time_ms": round(elapsed_ms, 2),
                }
                publish_reasoning(producer, session_id, reasoning)
                logger.info("Wine pairing in %.1fms for '%s'", elapsed_ms, dish_hint)

            elif intent_type == "menu_inquiry":
                elapsed_ms = (time.time() - process_start) * 1000
                payload = {
                    "inquiry_type": "general_menu",
                    "total_items": len(menu),
                    "highlights": [
                        i.get("name", "") for i in menu
                        if i.get("category") in ("entree", "special")
                    ][:5],
                }
                publish_whisper(r, session_id, "menu_suggestion", payload)
                logger.info("Menu inquiry in %.1fms", elapsed_ms)

        except Exception as exc:
            logger.error("Error processing message: %s", exc)


if __name__ == "__main__":
    run()
