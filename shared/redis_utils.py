from __future__ import annotations

import json
import os
import time

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

WHISPER_STREAM_KEY = "aura:whisper_bus"
GUEST_KEY_PREFIX = "aura:guest:"


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )


def publish_whisper(
    client: redis.Redis,
    agent: str,
    whisper_type: str,
    session_id: str,
    payload: dict,
) -> str:
    entry = {
        "agent": agent,
        "whisper_type": whisper_type,
        "session_id": session_id,
        "payload": json.dumps(payload),
        "timestamp": str(time.time()),
    }
    message_id: str = client.xadd(WHISPER_STREAM_KEY, entry)
    return message_id


def read_whispers(
    client: redis.Redis,
    session_id: str,
    last_id: str = "0-0",
    count: int = 50,
) -> list[dict]:
    raw = client.xread({WHISPER_STREAM_KEY: last_id}, count=count, block=0)
    whispers = []
    for _stream_name, messages in raw:
        for msg_id, data in messages:
            if data.get("session_id") == session_id:
                whispers.append(
                    {
                        "id": msg_id,
                        "agent": data["agent"],
                        "whisper_type": data["whisper_type"],
                        "session_id": data["session_id"],
                        "payload": json.loads(data["payload"]),
                        "timestamp": float(data["timestamp"]),
                    }
                )
    return whispers


def read_latest_whispers(
    client: redis.Redis,
    session_id: str,
    count: int = 10,
) -> list[dict]:
    raw = client.xrevrange(WHISPER_STREAM_KEY, count=count)
    whispers = []
    for msg_id, data in raw:
        if data.get("session_id") == session_id:
            whispers.append(
                {
                    "id": msg_id,
                    "agent": data["agent"],
                    "whisper_type": data["whisper_type"],
                    "session_id": data["session_id"],
                    "payload": json.loads(data["payload"]),
                    "timestamp": float(data["timestamp"]),
                }
            )
    return whispers


def set_guest_profile(client: redis.Redis, phone: str, profile: dict) -> None:
    key = f"{GUEST_KEY_PREFIX}{phone}"
    client.json().set(key, "$", profile)


def get_guest_profile(client: redis.Redis, phone: str) -> dict | None:
    key = f"{GUEST_KEY_PREFIX}{phone}"
    result = client.json().get(key, "$")
    if result and len(result) > 0:
        return result[0]
    return None
