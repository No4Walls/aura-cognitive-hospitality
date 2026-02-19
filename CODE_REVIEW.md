# Project Aura - Deep Code Review

**Reviewer:** Aura Dev Agent (Devin)
**Date:** February 19, 2026
**Scope:** Full codebase (Phase 1 + Phase 2) — 37 files, ~3,400 lines

---

## Critical Issues

### 1. Sommelier Whisper Integration Is Non-Functional

**File:** `gateway/app/main.py:307`

The Gateway extracts `payload.get("recommendation", "")` from Sommelier whispers, but the Sommelier never publishes a field called `recommendation`. The actual payload fields are:

- **ingredient_filter:** `filter_type`, `excluded_terms`, `safe_items`, `removed_items`, `safe_count`, `removed_count`, `reasoning`
- **wine_pairing:** `pairing_type`, `dish`, `flavor_category`, `recommendations`
- **menu_inquiry:** `inquiry_type`, `total_items`, `highlights`

**Impact:** `sommelier_hint` is always an empty string. All Sommelier-dependent branches in `_generate_response()` (lines 318-329, 334-336) are dead code. The Sommelier Agent processes transcripts and publishes whispers correctly, but the Gateway never uses them.

**Fix:** Parse the actual payload structure. For ingredient filters, construct a response from `safe_items`/`removed_items`. For wine pairings, use the `recommendations` list.

---

### 2. `expand_allergen_to_family()` Fails on Family Names

**File:** `agents/sommelier/app/ontology.py:76-80`

When the input IS a family name (e.g., `"dairy"`, `"allium"`), `get_allergen_family("dairy")` returns `None` because `"dairy"` is the key, not a member. The function falls through and returns `["dairy"]` as an exact-match term.

This means `filter_menu_by_exclusion(menu, ["dairy"])` does NOT expand to milk, cream, butter, cheese, etc. It only filters items that literally have `"dairy"` in their ingredients, allergens, or allergen_family lists. The test passes because menu items include `"dairy"` in their `allergen_family` arrays — but a menu item containing `"cream"` without `"dairy"` in its allergen_family would slip through.

**Fix:** Add a direct family-name check:
```python
def expand_allergen_to_family(allergen: str) -> list[str]:
    allergen_lower = allergen.lower().strip()
    if allergen_lower in ALLERGEN_FAMILIES:
        return ALLERGEN_FAMILIES[allergen_lower]
    family = get_allergen_family(allergen)
    if family:
        return ALLERGEN_FAMILIES[family]
    return [allergen_lower]
```

---

### 3. Whisper Bus Has No Size Limit (Memory Leak)

**Files:** All `xadd()` calls in `gateway/app/main.py`, `agents/historian/app/main.py:160`, `agents/negotiator/app/main.py:301`, `agents/sommelier/app/main.py:220`

No `maxlen` parameter is set on any `r.xadd(WHISPER_STREAM_KEY, entry)` call. The Redis Stream grows unboundedly. Under sustained load (50 concurrent calls target), the stream will consume all available Redis memory.

**Fix:** Add `maxlen` with approximate trimming: `r.xadd(WHISPER_STREAM_KEY, entry, maxlen=10000, approximate=True)`

---

### 4. Prometheus Cardinality Explosion on `SENTIMENT_SCORE`

**File:** `gateway/app/metrics.py:31-35`, used at `gateway/app/main.py:232,234`

`SENTIMENT_SCORE` uses `session_id` as a label. Session IDs are UUIDs, creating a new time series for every call. This is a known Prometheus anti-pattern that causes unbounded memory growth in Prometheus and degrades query performance.

**Fix:** Remove the `session_id` label. Track sentiment as a histogram or summary instead, or use a single gauge for the last-known sentiment.

---

### 5. Vector Embeddings Are Not Semantically Meaningful

**File:** `agents/historian/app/vector_memory.py:22-46`

The SHA256-based embedding maps text to a deterministic but cryptographically random vector. Two semantically similar phrases (e.g., "I love red wine" and "Red wine is my favorite") produce completely different embeddings because SHA256 is a one-way hash. The KNN search will effectively return random results, not semantically similar memories.

The keyword boosting (lines 28-40) helps marginally — if two texts share the word "anniversary," they'll both have a boosted value at index 0 — but the base vector is still random noise.

**Impact:** The "deep context recall" feature (anniversary recall, preference patterns) will not work as designed. The ADID target of "Historian recalls anniversary context in >90% of test calls" cannot be met with this embedding approach.

**Fix:** Replace with a real embedding model. Options:
- `sentence-transformers/all-MiniLM-L6-v2` (local, no API cost, 384-dim)
- OpenAI `text-embedding-3-small` (API, cost per token, 1536-dim)
- Document this as a known placeholder in the README

---

## High-Severity Issues

### 6. Negotiator Time/Party-Size Parsing Conflict

**File:** `agents/negotiator/app/main.py:110-126`

Time detection checks for digit characters (e.g., `"7" in text_lower`), and party size detection iterates over words looking for digits. The text "table for 7 on Friday" produces both `requested_time = "7:00 PM"` and `party_size = 7`. There is no disambiguation.

Additionally, "party of 17" would set `party_size = None` because 17 is extracted as the word "17" which passes the `1 <= int(word) <= 20` check, but the time check would match on "7" first.

**Fix:** Parse time patterns more precisely (e.g., regex for "7 pm", "7:00", "at 7") and extract party size from contextual phrases ("party of N", "table for N", "N people").

---

### 7. `_collect_whispers` Is Inefficient and Race-Prone

**File:** `gateway/app/main.py:258-282`

The function reads the last 20 entries globally via `xrevrange` and filters by session_id. Issues:

1. **Race condition:** The 50ms sleep before collection (`WHISPER_POLL_TIMEOUT_MS = 50`) may not be enough for agents to consume from Kafka, process, and write back to Redis. Under load, whispers arrive after the poll window closes.
2. **Scalability:** Reading only the last 20 entries means in a busy system (50 concurrent calls), most session whispers will be outside the window.
3. **No consumer group:** The stream is read via `xrevrange` without tracking position, so whispers can be re-read or missed.

**Fix:** Use Redis consumer groups (`XREADGROUP`) with per-session tracking, or switch to session-keyed Redis keys (e.g., `aura:whispers:{session_id}`) with a TTL.

---

### 8. Gateway Hardcodes Redis Config Instead of Using config.py

**File:** `gateway/app/main.py:36-37`

```python
REDIS_HOST = "redis"
REDIS_PORT = 6379
```

These are hardcoded, ignoring the existing `gateway/app/config.py` which already defines `REDIS_HOST` and `REDIS_PORT` from environment variables. This means the docker-compose `REDIS_HOST` and `REDIS_PORT` env vars for the gateway service have no effect.

**Fix:** Import from config: `from gateway.app.config import REDIS_HOST, REDIS_PORT`

---

### 9. Negotiator Revenue Score Is Inverted

**File:** `agents/negotiator/app/main.py:164`

```python
"revenue_score": REVENUE_PRIORITY.index(seating_type) + 1,
```

`REVENUE_PRIORITY = ["private", "dining_room", "patio", "high_top", "bar"]`. This gives "private" a score of 1 (lowest) and "bar" a score of 5 (highest). The intended ordering is the opposite — private dining generates the most revenue.

**Fix:** Invert the score: `"revenue_score": len(REVENUE_PRIORITY) - REVENUE_PRIORITY.index(seating_type)`

---

### 10. Negotiator Ignores tenant_id

**File:** `agents/negotiator/app/main.py:341-345`

The Negotiator defines `TENANT_ID` at module level (line 20) but never extracts `tenant_id` from the Kafka message, unlike Historian (line 217) and Sommelier (line 27). In a multi-tenant deployment, the Negotiator would process transcripts from all tenants indiscriminately.

**Fix:** Extract and log `tenant_id` from the message value, consistent with other agents.

---

### 11. Sommelier Loads Full Menu on Every Transcript

**File:** `agents/sommelier/app/main.py:274-275`

`load_menu(r)` performs a full `SCAN` of all `aura:menu:*` keys on every inbound transcript. For 14 items this is fast, but it won't scale to larger menus (100+ items) and creates unnecessary Redis load.

**Fix:** Cache the menu in memory with a TTL-based refresh (e.g., reload every 60 seconds). Menu data changes infrequently.

---

## Medium-Severity Issues

### 12. `shared/` Module Is Completely Unused

**Files:** `shared/models.py`, `shared/redis_utils.py`, `shared/kafka_utils.py`

These files define clean dataclasses (`Whisper`, `GuestProfile`, `TranscriptEvent`) and utility functions (`publish_whisper`, `read_whispers`, `get_producer`, `get_consumer`, `ensure_topics`), but no agent or the gateway imports any of them. Each service has its own inline implementations of the same logic.

**Impact:** Significant code duplication. The `connect_redis()` function is copy-pasted across 3 agents with identical retry logic (~11 lines each). The `publish_whisper` function is reimplemented 3 times with slightly different signatures.

**Fix:** Either remove `shared/` or refactor agents to use it. Since each agent runs in its own container without access to `shared/`, this would require either:
- Adding shared as a pip-installable package
- Copying shared into each container's build context
- Using a monorepo build approach

---

### 13. Historian Stores Embeddings for Outbound (Bot) Transcripts

**File:** `agents/historian/app/main.py:227-231`

Embedding storage happens before the `if direction != "inbound": continue` check (line 233). This means every outbound bot response gets stored as a vector memory. Bot-generated text pollutes the vector memory with formulaic responses, diluting the signal from actual guest statements.

**Fix:** Move the embedding storage block after the direction check, or add a direction filter:
```python
if direction == "inbound":
    store_transcript_embedding(...)
```

---

### 14. Docker Compose Port Conflict (Redis vs Historian)

**File:** `docker-compose.yml:8,89`

Redis Stack exposes port `8001` for RedisInsight (`- "8001:8001"`). The Historian's Prometheus metrics server also binds to port `8001` internally. While this doesn't cause a container conflict (Historian doesn't map its port to the host), it means:
- Operators cannot access Historian metrics from the host
- Port 8001 on localhost goes to RedisInsight, not Historian
- Prometheus scrapes work internally via the Docker network

**Fix:** Either change the Historian metrics port to a non-conflicting value (e.g., 8004) or don't expose RedisInsight port 8001 to the host.

---

### 15. Flavor Profile Tags Don't Align With Ontology Keywords

**Files:** `agents/sommelier/app/ontology.py:21-29`, `scripts/seed_menu.py`

Menu items use adjective-style tags (e.g., `"briny"`, `"cream"`, `"herbaceous"`) but `FLAVOR_PROFILES` maps to specific ingredient-style keywords. For example:
- `briny_oceanic` keywords: `["seaweed", "oyster", "clam", "anchovy", "caper", "olive"]` — does not include `"briny"`
- `herbaceous` keywords: `["basil", "thyme", "rosemary", ...]` — does not include `"herbaceous"`

The Grilled Branzino with tags `["briny", "citrus", "butter", "delicate", "herbaceous"]` matches `creamy_mild` (2 hits: butter, delicate) instead of `briny_oceanic` (0 hits). It would get paired with Chardonnay instead of Muscadet/Albarino.

**Fix:** Either add the adjective forms to `FLAVOR_PROFILES` keywords, or change menu tags to use ingredient-level terms from the ontology.

---

### 16. `kafka_producer.flush()` Blocks on Every Message

**File:** `gateway/app/main.py:254`

`_publish_transcript()` calls `kafka_producer.flush()` synchronously after every `produce()`. This blocks the async event loop until Kafka acknowledges the message, adding latency to every API call. The Sommelier and Negotiator also flush after every produce.

**Fix:** Use `poll(0)` instead of `flush()` for non-critical paths, or batch-flush periodically. For the gateway, use an async wrapper or background task.

---

### 17. Historian Uses Unusual Producer Pattern

**File:** `agents/historian/app/main.py:165-170`

```python
def publish_reasoning(producer_ref: list, session_id: str, reasoning: dict) -> None:
    from confluent_kafka import Producer
    if not producer_ref:
        producer_ref.append(Producer({"bootstrap.servers": KAFKA_BOOTSTRAP}))
    p = producer_ref[0]
```

This lazy-initializes a Producer using a mutable list as a poor-man's singleton. It also has a module-level import inside the function body. The Negotiator and Sommelier simply create the Producer at startup. This inconsistency adds cognitive overhead.

**Fix:** Create the Producer at startup in `run()` like the other agents do.

---

## Low-Severity Issues

### 18. No Health Checks for Historian and Negotiator Containers

**File:** `docker-compose.yml`

The Gateway and Sommelier have `HEALTHCHECK` directives in their Dockerfiles. Historian and Negotiator do not. Docker reports them as "running" but has no way to verify they're actually healthy and consuming messages.

**Fix:** Add health checks. Since these agents run a Prometheus HTTP server, use that:
```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/metrics')"
```

---

### 19. No Resource Limits in docker-compose

**File:** `docker-compose.yml`

No `mem_limit`, `cpus`, or `deploy.resources` are set on any service. A runaway agent (e.g., unbounded vector storage) could consume all host resources and crash the entire swarm.

---

### 20. `json` Import Unused in seed_data.py

**File:** `scripts/seed_data.py:5`

`import json` is never used.

---

### 21. `_handle_media` Is a Stub

**File:** `gateway/app/main.py:341-342`

The WebSocket media handler only logs at debug level and does nothing with the audio data. This is expected scaffolding for Phase 3 (Twilio/ElevenLabs integration) but should be noted.

---

### 22. No Input Validation on `/api/simulate-call`

**File:** `gateway/app/main.py:164-209`

The endpoint accepts arbitrary JSON without validation — no length limits on `text`, no format validation on `caller_number`. A request with a 1MB text payload would be published to Kafka, stored as a vector embedding, and processed by all agents.

**Fix:** Add Pydantic request models with field validators.

---

### 23. Global Mutable State in Gateway

**File:** `gateway/app/main.py:46-47`

```python
redis_client: redis.Redis | None = None
kafka_producer: Producer | None = None
```

These module-level globals work for single-process uvicorn but would break with multi-worker deployment (`uvicorn --workers N`). Each worker would have its own copies.

---

### 24. Onion Test Uses `sys.path.insert` Hack

**File:** `scripts/onion_test.py:9`

`sys.path.insert(0, ".")` depends on CWD being the project root. If run from a different directory, imports fail.

**Fix:** Use proper package installation (`pip install -e .`) or a `conftest.py` / `PYTHONPATH` approach.

---

### 25. No Graceful Shutdown for Agent Consumers

**Files:** `agents/historian/app/main.py:201`, `agents/negotiator/app/main.py:331`, `agents/sommelier/app/main.py:249`

All agents run `while True:` loops with no signal handling. On `docker stop`, they receive SIGTERM and are killed after the grace period. The Kafka consumer's `close()` is never called, which means uncommitted offsets may be lost and messages reprocessed on restart.

**Fix:** Add signal handlers to break the loop and call `consumer.close()`.

---

## Positive Observations

1. **Clean separation of concerns** — Each agent has a single responsibility, its own container, and independent scaling potential.
2. **Consistent logging format** — All agents use structured logging with agent name prefix.
3. **Prometheus metrics on every agent** — Good foundation for the Observation Deck.
4. **Allergen family ontology is well-designed** — 15 families with comprehensive member lists. The expansion logic is sound (modulo the family-name bug in item 2).
5. **Wine pairing knowledge base** — The `WINE_PAIRING_MAP` is curated with expert-level accuracy and reasoning notes.
6. **Graceful degradation** — Vector memory failures are caught and logged without crashing the Historian. Menu filtering falls back to exact matching for unknown allergens.
7. **Seed data is realistic** — Menu items have accurate ingredients, allergen tagging, and flavor profiles for a fine-dining restaurant.
8. **Test scripts are comprehensive** — The Julian Test validates the full hot-path. The Onion Test covers multiple allergen families and intent detection.
9. **Docker health checks** — Redis, Kafka, Zookeeper, Gateway, and Sommelier all have proper health checks with appropriate intervals.
10. **`from __future__ import annotations`** — Consistently used for modern type hint syntax.

---

## Summary

| Severity | Count | Key Themes |
|---|---|---|
| **Critical** | 5 | Broken Sommelier integration, allergen expansion bug, memory leak, cardinality explosion, placeholder embeddings |
| **High** | 6 | Time parsing conflicts, race conditions, config duplication, inverted scoring, missing tenant filtering, no menu caching |
| **Medium** | 6 | Unused shared module, bot text in vectors, port conflict, flavor mismatch, sync flushes, inconsistent patterns |
| **Low** | 8 | Missing health checks, no resource limits, unused imports, stubs, no input validation, no graceful shutdown |

The architecture is fundamentally sound — the stigmergic coordination model, containerized isolation, and event backbone are well-implemented. The critical issues are mostly integration bugs (Gateway not reading Sommelier payloads correctly) and scaling concerns (unbounded streams, placeholder embeddings) rather than fundamental design flaws. A focused sprint addressing items 1-5 would significantly improve production-readiness.
