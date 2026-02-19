from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from gateway.app.metrics import (
    ACTIVE_CALLS,
    CALL_COUNT,
    KAFKA_EVENTS,
    RESPONSE_LATENCY,
    SENTIMENT_SCORE,
    TOKEN_USAGE,
    WHISPER_COUNT,
)

from shared.kafka_utils import (
    TRANSCRIPT_TOPIC,
    async_connect_kafka_producer,
)
from shared.redis_utils import (
    GUEST_KEY_PREFIX,
    async_connect_redis,
)
from shared.gemini_utils import generate_text, warmup as warmup_gemini

import redis
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("aura-gateway")

TENANT_ID = os.getenv("TENANT_ID", "default")


redis_client: redis.Redis | None = None
kafka_producer: Producer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global redis_client, kafka_producer
    logger.info("Aura Gateway starting up...")

    try:
        redis_client = await async_connect_redis()
    except Exception:
        logger.error("Failed to connect to Redis after retries")

    try:
        kafka_producer = await async_connect_kafka_producer()
    except Exception:
        logger.error("Failed to connect to Kafka after retries")

    warmup_gemini()
    logger.info("Aura Gateway ready")
    yield

    logger.info("Aura Gateway shutting down...")
    if kafka_producer:
        kafka_producer.flush()


app = FastAPI(title="Aura Gateway", version="0.1.0", lifespan=lifespan)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health() -> dict:
    redis_ok = False
    kafka_ok = False
    try:
        if redis_client:
            redis_client.ping()
            redis_ok = True
    except Exception:
        pass
    try:
        if kafka_producer:
            kafka_ok = True
    except Exception:
        pass
    return {"status": "healthy", "redis": redis_ok, "kafka": kafka_ok}


@app.post("/twilio/voice")
async def twilio_voice_webhook(request: Request) -> JSONResponse:
    form_data = await request.form()
    caller = form_data.get("From", "unknown")
    session_id = str(uuid.uuid4())

    CALL_COUNT.inc()
    ACTIVE_CALLS.inc()
    logger.info("Incoming call from %s, session=%s", caller, session_id)

    profile = _lookup_guest(caller)
    greeting = _build_greeting(profile, session_id)

    _publish_transcript(session_id, caller, f"[CALL_START] from {caller}", "inbound")

    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="alice">{greeting}</Say>
    <Connect>
        <Stream url="wss://{{{{request.url.hostname}}}}/ws/media/{session_id}" />
    </Connect>
</Response>"""

    return JSONResponse(
        content={"session_id": session_id, "greeting": greeting, "twiml": twiml},
        media_type="application/json",
    )


@app.websocket("/ws/media/{session_id}")
async def media_stream(websocket: WebSocket, session_id: str) -> None:
    await websocket.accept()
    logger.info("WebSocket connected for session %s", session_id)

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            event_type = message.get("event")

            if event_type == "media":
                await _handle_media(session_id, message)
            elif event_type == "start":
                logger.info("Stream started for session %s", session_id)
            elif event_type == "stop":
                logger.info("Stream stopped for session %s", session_id)
                break
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for session %s", session_id)
    finally:
        ACTIVE_CALLS.dec()


@app.post("/api/simulate-call")
async def simulate_call(request: Request) -> JSONResponse:
    body = await request.json()
    caller = body.get("caller_number", "+15551234567")
    text = body.get("text", "Hello, I'd like to make a reservation")
    session_id = str(uuid.uuid4())

    start_time = time.time()

    CALL_COUNT.inc()
    ACTIVE_CALLS.inc()

    profile = _lookup_guest(caller)
    greeting = _build_greeting(profile, session_id)

    _publish_transcript(session_id, caller, text, "inbound")

    whispers = await _collect_whispers(session_id)

    response_text = _generate_response(greeting, text, whispers, profile)

    _publish_transcript(session_id, caller, response_text, "outbound")

    latency_ms = (time.time() - start_time) * 1000
    RESPONSE_LATENCY.observe(latency_ms / 1000)
    ACTIVE_CALLS.dec()

    TOKEN_USAGE.labels(agent="gateway", token_type="prompt").inc(len(text.split()))
    TOKEN_USAGE.labels(agent="gateway", token_type="completion").inc(len(response_text.split()))

    logger.info("Simulated call latency: %.1fms", latency_ms)

    return JSONResponse(
        {
            "session_id": session_id,
            "caller": caller,
            "guest_profile": profile,
            "greeting": greeting,
            "input_text": text,
            "whispers": whispers,
            "response": response_text,
            "latency_ms": round(latency_ms, 2),
        }
    )


def _lookup_guest(caller: str) -> dict | None:
    if not redis_client:
        return None
    try:
        key = f"{GUEST_KEY_PREFIX}{caller}"
        result = redis_client.json().get(key, "$")
        if result and len(result) > 0:
            return result[0]
    except Exception as exc:
        logger.warning("Guest lookup failed: %s", exc)
    return None


def _build_greeting(profile: dict | None, session_id: str) -> str:
    if profile:
        name = profile.get("name", "valued guest")
        tags = profile.get("preference_tags", [])
        greeting = f"Welcome back, {name}! Wonderful to hear from you."
        if "red wine" in tags:
            greeting += " Shall I start with your usual glass of red?"
        SENTIMENT_SCORE.set(0.8)
        return greeting
    SENTIMENT_SCORE.set(0.5)
    return "Good evening, thank you for calling. How may I assist you?"


def _publish_transcript(session_id: str, caller: str, text: str, direction: str) -> None:
    if not kafka_producer:
        return
    event = {
        "session_id": session_id,
        "caller_number": caller,
        "text": text,
        "direction": direction,
        "tenant_id": TENANT_ID,
        "timestamp": time.time(),
    }
    kafka_producer.produce(
        TRANSCRIPT_TOPIC,
        key=session_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
    )
    kafka_producer.flush()
    KAFKA_EVENTS.labels(topic=TRANSCRIPT_TOPIC).inc()


WHISPER_MAX_WAIT_MS = 400
WHISPER_POLL_INTERVAL_MS = 25


async def _collect_whispers(session_id: str) -> list[dict]:
    if not redis_client:
        return []
    session_key = f"aura:whispers:{session_id}"
    logger.debug("Polling whispers from key=%s (max %dms)", session_key, WHISPER_MAX_WAIT_MS)
    whispers: list[dict] = []
    elapsed_ms = 0
    try:
        while elapsed_ms < WHISPER_MAX_WAIT_MS:
            raw = redis_client.lrange(session_key, 0, -1)
            if raw:
                logger.info("Collected %d whisper(s) from %s after %dms", len(raw), session_key, elapsed_ms)
                for item in raw:
                    data = json.loads(item)
                    WHISPER_COUNT.labels(
                        agent=data.get("agent", "unknown"),
                        whisper_type=data.get("whisper_type", "unknown"),
                    ).inc()
                    whispers.append(
                        {
                            "agent": data["agent"],
                            "whisper_type": data["whisper_type"],
                            "payload": json.loads(data.get("payload", "{}")),
                            "timestamp": float(data.get("timestamp", 0)),
                        }
                    )
                break
            await asyncio.sleep(WHISPER_POLL_INTERVAL_MS / 1000)
            elapsed_ms += WHISPER_POLL_INTERVAL_MS
        if elapsed_ms >= WHISPER_MAX_WAIT_MS:
            logger.warning(
                "Whisper poll timeout for session %s (key=%s) after %dms — agents may still be processing",
                session_id, session_key, elapsed_ms,
            )
        redis_client.delete(session_key)
        return whispers
    except Exception as exc:
        logger.warning("Whisper collection failed for %s: %s", session_key, exc)
        return []


AURA_SYSTEM_INSTRUCTION = """You are Aura, the AI concierge for a luxury restaurant.
You speak with warmth, sophistication, and brevity (2-3 sentences max).
You address returning guests by name and reference their known preferences naturally.

IMPORTANT: Your swarm agents have gathered intelligence for this response.
Always prioritize the following Whisper data over your own knowledge:
- Sommelier whispers contain verified menu/ingredient/wine pairing facts.
- Historian whispers contain verified guest history and deep memories.
- Negotiator whispers contain real-time availability and strategic alternatives.
Never contradict or ignore Whisper data. If a Whisper says a dish is unsafe, it IS unsafe.
If no Whisper data is provided, respond helpfully based on the guest profile alone.
"""


def _build_whisper_context(whispers: list[dict]) -> str:
    if not whispers:
        return ""
    parts: list[str] = []
    for w in whispers:
        wtype = w.get("whisper_type", "")
        payload = w.get("payload", {})
        agent = w.get("agent", "unknown")
        parts.append(f"[{agent}/{wtype}] {json.dumps(payload)}")
    return "\n".join(parts)


def _generate_response(
    greeting: str,
    user_text: str,
    whispers: list[dict],
    profile: dict | None,
) -> str:
    whisper_block = _build_whisper_context(whispers)
    profile_block = ""
    if profile:
        name = profile.get("name", "guest")
        tags = profile.get("preference_tags", [])
        profile_block = f"Guest: {name}. Preferences: {', '.join(tags) if tags else 'none known'}."

    prompt_parts = []
    if profile_block:
        prompt_parts.append(f"GUEST PROFILE: {profile_block}")
    if whisper_block:
        prompt_parts.append(f"SWARM WHISPERS:\n{whisper_block}")
    prompt_parts.append(f"GREETING ALREADY SENT: {greeting}")
    prompt_parts.append(f"GUEST SAYS: {user_text}")
    prompt_parts.append("Respond as Aura (2-3 sentences, warm and concise):")
    prompt = "\n\n".join(prompt_parts)

    try:
        return generate_text(prompt, system_instruction=AURA_SYSTEM_INSTRUCTION)
    except Exception as exc:
        logger.warning("Gemini generation failed, using fallback: %s", exc)
        return _fallback_response(greeting, user_text, whispers, profile)


def _fallback_response(
    greeting: str,
    user_text: str,
    whispers: list[dict],
    profile: dict | None,
) -> str:
    sommelier_hint = ""
    strategy_hint = ""
    deep_memories: list[str] = []

    for w in whispers:
        wtype = w.get("whisper_type", "")
        payload = w.get("payload", {})
        if wtype == "guest_context":
            if payload.get("deep_memories"):
                deep_memories = payload["deep_memories"]
        elif wtype == "strategy_hint":
            strategy_hint = payload.get("suggestion", "")
        elif wtype == "menu_suggestion":
            if payload.get("filter_type") == "ingredient_exclusion":
                safe = payload.get("safe_items", [])
                removed = payload.get("removed_items", [])
                reasoning = payload.get("reasoning", [])
                if safe:
                    sommelier_hint = f"Based on your preferences, I'd recommend: {', '.join(safe)}."
                    if removed:
                        sommelier_hint += f" I've set aside {', '.join(removed)} as they contain ingredients you'd like to avoid."
                elif reasoning:
                    sommelier_hint = " ".join(reasoning)
            elif payload.get("pairing_type") == "wine":
                recs = payload.get("recommendations", [])
                dish = payload.get("dish", "")
                if recs:
                    top = recs[0]
                    sommelier_hint = f"For the {dish}, I'd suggest a {top.get('wine', '')}. {top.get('reason', '')}"
                    if len(recs) > 1:
                        others = ", ".join(r.get("wine", "") for r in recs[1:])
                        sommelier_hint += f" You might also enjoy {others}."
            elif payload.get("inquiry_type") == "general_menu":
                highlights = payload.get("highlights", [])
                if highlights:
                    sommelier_hint = f"Tonight's highlights include {', '.join(highlights)}."

    if profile:
        name = profile.get("name", "guest")
        tags = profile.get("preference_tags", [])
        if any(kw in user_text.lower() for kw in ["reservation", "book", "table"]):
            base = f"Of course, {name}! I'd love to arrange that for you."
            if strategy_hint:
                base += f" {strategy_hint}"
            return base
        if any(kw in user_text.lower() for kw in ["wine", "drink", "beverage"]):
            if sommelier_hint:
                return f"{name}, {sommelier_hint}"
            if "red wine" in tags:
                return f"Excellent taste as always, {name}. I have a wonderful Barolo that I think you'll adore."
            return f"I'd be happy to recommend something special for you, {name}."
        if any(kw in user_text.lower() for kw in ["no ", "without", "allerg", "can't eat", "intoleran"]):
            if sommelier_hint:
                return f"Absolutely, {name}. {sommelier_hint}"
        if deep_memories:
            return f"Welcome back, {name}. I remember your last visit."
        return f"Absolutely, {name}. I'm here to make your experience perfect."

    if any(kw in user_text.lower() for kw in ["no ", "without", "allerg"]):
        if sommelier_hint:
            return f"Of course! {sommelier_hint}"

    return "I'd be delighted to help you. Could you tell me a bit more about what you're looking for?"


async def _handle_media(session_id: str, message: dict) -> None:
    logger.debug("Processing media chunk for session %s", session_id)
