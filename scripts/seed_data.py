#!/usr/bin/env python3
"""Seed the VIP Identity Vault with sample guest profiles."""
from __future__ import annotations

import json
import sys
import time

import redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379
GUEST_KEY_PREFIX = "aura:guest:"

VIP_GUESTS = [
    {
        "phone_number": "+15551234567",
        "name": "Julian",
        "last_call_date": "2026-02-10",
        "preference_tags": ["red wine", "window table", "quiet seating", "Barolo"],
        "notes": {
            "preferred_table": "table 4",
            "dietary": "no cilantro",
            "anniversary_month": "March",
            "vip_tier": "platinum",
        },
    },
    {
        "phone_number": "+15559876543",
        "name": "Isabella",
        "last_call_date": "2026-02-14",
        "preference_tags": ["champagne", "private dining", "seafood", "celebration"],
        "notes": {
            "preferred_table": "private room A",
            "dietary": "shellfish allergy - lobster only",
            "vip_tier": "gold",
        },
    },
    {
        "phone_number": "+15555551212",
        "name": "Marcus",
        "last_call_date": "2026-01-28",
        "preference_tags": ["whiskey", "bar seating", "steak", "old fashioned"],
        "notes": {
            "preferred_table": "bar seat 3",
            "dietary": "none",
            "vip_tier": "silver",
        },
    },
    {
        "phone_number": "+15553334444",
        "name": "Sophia",
        "last_call_date": "2026-02-01",
        "preference_tags": ["white wine", "garden patio", "vegetarian", "Sancerre"],
        "notes": {
            "preferred_table": "patio table 2",
            "dietary": "vegetarian, gluten-free",
            "vip_tier": "gold",
        },
    },
    {
        "phone_number": "+15557778888",
        "name": "Alexander",
        "last_call_date": "2026-02-16",
        "preference_tags": ["red wine", "corner booth", "tasting menu", "Brunello"],
        "notes": {
            "preferred_table": "booth 7",
            "dietary": "none",
            "vip_tier": "platinum",
            "special": "always orders the tasting menu",
        },
    },
]


def seed(host: str = REDIS_HOST, port: int = REDIS_PORT) -> None:
    print(f"Connecting to Redis at {host}:{port}...")
    r = redis.Redis(host=host, port=port, decode_responses=True)

    for attempt in range(10):
        try:
            r.ping()
            break
        except redis.ConnectionError:
            print(f"  Redis not ready, retrying... ({attempt + 1}/10)")
            time.sleep(2)
    else:
        print("ERROR: Could not connect to Redis")
        sys.exit(1)

    print(f"Seeding {len(VIP_GUESTS)} VIP profiles...")
    for guest in VIP_GUESTS:
        key = f"{GUEST_KEY_PREFIX}{guest['phone_number']}"
        r.json().set(key, "$", guest)
        print(f"  {guest['name']} ({guest['phone_number']}) -> {key}")

    print("\nVerifying seed data...")
    for guest in VIP_GUESTS:
        key = f"{GUEST_KEY_PREFIX}{guest['phone_number']}"
        result = r.json().get(key, "$")
        if result and len(result) > 0:
            print(f"  OK: {guest['name']}")
        else:
            print(f"  FAIL: {guest['name']}")

    print("\nSeed complete.")


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else REDIS_HOST
    port = int(sys.argv[2]) if len(sys.argv) > 2 else REDIS_PORT
    seed(host, port)
