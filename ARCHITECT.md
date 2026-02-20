# Project Aura -- Architectural Reference

> Last updated: 2026-02-19
> Architecture Owner: Cory Cobler
> Engineering Lead: Aura Autonomous Dev Agent (Devin)

---

## 1. System Overview

Project Aura is an AI-powered hospitality concierge built on a **Cognitive Swarm** architecture. Specialized agents (Historian, Negotiator, Sommelier) collaborate through a shared Redis/Kafka backbone to deliver "Warm Recognition" -- an experience where the system instantly knows a returning guest by name, remembers their preferences, and coordinates a personalized response in under 500ms.

The primary deployment target is luxury restaurants. The system handles inbound phone calls via Twilio/ElevenLabs and exposes a simulated-call HTTP API for testing.

---

## 2. Service Map

```
                         +-------------------+
                         |   Twilio / Test    |
                         |    (Inbound)       |
                         +---------+---------+
                                   |
                                   v
                     +-------------+-------------+
                     |       Aura Gateway        |
                     |   FastAPI :8000            |
                     |   Gemini LLM reasoning    |
                     +--+------+------+------+---+
                        |      |      |      |
               publish  | poll |      | read |
            transcript  | whispers   | guest |
                        |      |      |      |
              +---------v--+   |  +---v------+---+
              |   Kafka    |   |  |    Redis      |
              | :9092      |   |  |  Stack :6379  |
              +--+-+-+-----+   |  +--+--+--+-----+
                 | | |         |     |  |  |
        consume  | | |         |     |  |  | publish
                 v v v         |     |  |  | whisper
   +-------------+ | +--------+--+  |  |  |
   |  Historian  | | | Negotiator|  |  |  |
   |  :8001      | | | :8002     |  |  |  |
   +-------------+ | +-----------+  |  |  |
                   v                |  |  |
           +-------+--------+      |  |  |
           |   Sommelier    +------+--+--+
           |   :8003        |
           +----------------+

   +------------------+    +------------------+
   |   Prometheus     +--->|     Grafana      |
   |   :9090          |    |     :3000        |
   +------------------+    +------------------+
```

### 2.1 Container Inventory (docker-compose.yml)

| Container        | Image / Build              | Port(s)     | Role                                      |
|------------------|----------------------------|-------------|-------------------------------------------|
| `aura-gateway`   | `gateway/Dockerfile`       | 8000        | HTTP API, WebSocket media relay, LLM call  |
| `aura-historian`  | `agents/historian/Dockerfile` | 8001 (metrics) | VIP lookup, vector memory, context whispers |
| `aura-negotiator` | `agents/negotiator/Dockerfile` | 8002 (metrics) | Intent analysis, booking strategy           |
| `aura-sommelier`  | `agents/sommelier/Dockerfile` | 8003 (metrics) | Allergen filtering, wine pairing, menu cache |
| `aura-redis`     | `redis/redis-stack:7.4.0-v1` | 6379, 8001  | Whisper Bus, VIP Vault, Vector Index, Menu  |
| `aura-kafka`     | `cp-kafka:7.7.1`           | 9092        | Durable event backbone                     |
| `aura-zookeeper` | `cp-zookeeper:7.7.1`       | 2181        | Kafka coordination                         |
| `aura-prometheus` | `prom/prometheus:v3.1.0`  | 9090        | Metrics aggregation                        |
| `aura-grafana`   | `grafana/grafana:11.4.0`   | 3000        | Observability dashboard                    |

All application containers use `python:3.12-slim` with `librdkafka-dev` for the confluent-kafka C extension. They share the `aura-net` bridge network. Redis and Kafka have healthchecks; application containers use `depends_on: condition: service_healthy`.

---

## 3. The Aura Loop (Request Lifecycle)

Every inbound interaction follows a deterministic six-phase loop.

### Phase 1 -- Identify

The Gateway receives the call (Twilio webhook or `/api/simulate-call`). It immediately looks up the caller's phone number in the **VIP Identity Vault** (RedisJSON).

```
Key format:   aura:guest:{phone_number}
Example:      aura:guest:+15551234567
Lookup:       redis_client.json().get(key, "$")
```

If a profile is found, the Gateway builds a personalized greeting ("Welcome back, Julian!"). If not, it returns a generic greeting.

**File:** `gateway/app/main.py` -- `_lookup_guest()`, `_build_greeting()`

### Phase 2 -- Publish

The Gateway publishes the transcript to Kafka topic `swarm.transcripts` with a synchronous `flush()` to guarantee delivery before whisper polling begins.

```json
{
  "session_id": "uuid",
  "caller_number": "+15551234567",
  "text": "I'd like to book my usual table...",
  "direction": "inbound",
  "tenant_id": "default",
  "timestamp": 1740000000.0
}
```

**File:** `gateway/app/main.py` -- `_publish_transcript()`

### Phase 3 -- Swarm Processing

All three agents consume from `swarm.transcripts` simultaneously via independent Kafka consumer groups:

| Agent       | Consumer Group       | Processing                                                    |
|-------------|----------------------|---------------------------------------------------------------|
| Historian   | `historian-agent`    | VIP lookup, context extraction, whisper publish, then embedding + KNN recall |
| Negotiator  | `negotiator-agent`   | Intent analysis (reservation/walk-in/complaint), strategy generation |
| Sommelier   | `sommelier-agent`    | Ingredient exclusion detection, wine pairing, menu inquiry     |

Each agent writes results to the **Whisper Bus** -- dual-write to both a Redis Stream (for Grafana observability) and a session-keyed Redis list (for Gateway retrieval).

**Critical optimization:** The Historian publishes its `guest_context` whisper immediately after the Redis guest lookup (fast path: ~10-20ms), BEFORE doing the expensive Gemini embedding and KNN recall. This ensures the whisper arrives within the Gateway's polling window.

### Phase 4 -- Collect Whispers

The Gateway polls the session-keyed Redis list every 25ms for up to 400ms:

```
Key format:   aura:whispers:{session_id}
Poll:         redis_client.lrange(key, 0, -1)
TTL:          300 seconds (auto-expire)
```

When whispers arrive, they are parsed and categorized by agent and type. On timeout, the Gateway proceeds with whatever data it has (graceful degradation).

**File:** `gateway/app/main.py` -- `_collect_whispers()`

### Phase 5 -- Synthesize Response

The Gateway assembles a prompt from the guest profile, collected whispers, and caller's text, then calls **Gemini 3 Flash** with a system instruction that defines the Aura persona and explicitly prioritizes Whisper data over the model's own knowledge.

The Gemini call runs in `asyncio.to_thread()` to avoid blocking the event loop.

If the Gemini API is unavailable or no API key is configured, the Gateway falls back to a deterministic rule-based response engine that uses keyword matching and whisper payloads.

**File:** `gateway/app/main.py` -- `_generate_response()`, `_fallback_response()`

### Phase 6 -- Respond

The response is returned to the caller (via Twilio TTS or JSON API response) and published to Kafka as an outbound transcript for the reasoning trace.

---

## 4. Data Architecture

### 4.1 Redis Keyspace

| Key Pattern                        | Type       | Module     | Purpose                                    | TTL     |
|------------------------------------|------------|------------|--------------------------------------------|---------|
| `aura:guest:{phone}`              | JSON       | RedisJSON  | VIP Identity Vault (guest profiles)        | None    |
| `aura:menu:{item_id}`             | JSON       | RedisJSON  | Structured menu items with allergens       | None    |
| `aura:memory:{tenant}:{caller}:{ts}` | JSON    | RedisJSON  | Vector embeddings for KNN search           | None    |
| `aura:whispers:{session_id}`      | List       | Core       | Session-scoped whisper collection           | 300s    |
| `aura:whisper_bus`                 | Stream     | Streams    | Global whisper stream for observability     | maxlen=10000 |

### 4.2 Kafka Topics

| Topic                | Partitions | Producers         | Consumers                                 |
|----------------------|------------|-------------------|--------------------------------------------|
| `swarm.transcripts`  | 3          | Gateway           | Historian, Negotiator, Sommelier           |
| `swarm.reasoning`    | 3          | All agents        | (Observability / audit log)                |

Topics auto-create on first use (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`). Retention is 168 hours (7 days).

### 4.3 RediSearch Vector Index

| Property         | Value                |
|------------------|----------------------|
| Index Name       | `idx:history`        |
| Legacy Names     | `idx:aura_memory` (auto-dropped on startup) |
| Dimensions       | 768                  |
| Distance Metric  | COSINE               |
| Key Prefix       | `aura:memory:`       |
| Index Type       | JSON                 |

**Schema fields:** `tenant_id` (Tag), `caller_id` (Tag), `session_id` (Tag), `text` (Text), `direction` (Tag), `embedding` (Vector FLAT FLOAT32).

Auto-migration: On startup, `ensure_vector_index()` checks for dimension mismatches on the existing index. If the dimensions don't match (e.g., after upgrading from 384 to 768), it drops the old index with `delete_documents=True` and recreates it.

---

## 5. Agent Deep-Dives

### 5.1 Historian Agent

**Purpose:** Memory and identity. The Historian is the system's long-term memory, responsible for VIP recognition and deep context recall.

**Files:**
- `agents/historian/app/main.py` -- Kafka consumer loop, guest lookup, context extraction, whisper publishing
- `agents/historian/app/vector_memory.py` -- Gemini embedding, RediSearch index management, KNN search

**Startup sequence:**
1. Start Prometheus metrics server on :8001
2. Register SIGTERM/SIGINT handlers for graceful shutdown
3. Connect to Redis (with retry loop, 30 attempts, 2s delay)
4. Connect to Kafka consumer group `historian-agent`
5. Create/verify RediSearch vector index (auto-migrate dimensions)
6. Warm up Gemini client (test embedding call)
7. Enter poll loop (`consumer.poll(timeout=1.0)`)

**Processing pipeline (inbound transcripts only):**
1. Look up guest profile in VIP Vault (`aura:guest:{phone}`)
2. Extract context (keyword detection: wine, reservation, habitual request)
3. **Publish `guest_context` whisper immediately** (fast path, Redis-only)
4. Generate 768-dim embedding via Gemini `text-embedding-004`
5. Store embedding in RediSearch vector index
6. KNN recall of deep memories (top 5 similar past interactions)
7. If deep memories found, publish second `deep_memory` whisper
8. Publish reasoning trace to Kafka `swarm.reasoning`

**Context extraction rules:**
- Known guest + wine keywords -> wine preferences summary
- Known guest + reservation keywords -> preferred table info
- Known guest + habitual keywords -> full preference tags
- Known guest + no keyword match -> general preferences
- Unknown guest + reservation keywords -> "New caller requesting reservation"
- No match -> no whisper published

**Embedding pipeline:**
- Text -> `embed_text()` via Gemini API -> 768-dim raw vector
- L2-normalize: `v / sqrt(sum(v^2))` (pure Python, no numpy)
- Store as JSON document with RedisJSON
- KNN query: `struct.pack()` for binary vector, RediSearch `FT.SEARCH` with dialect 2

**Prometheus metrics:**
- `historian_whispers_published_total` (Counter)
- `historian_lookup_latency_seconds` (Histogram)
- `historian_transcripts_processed_total` (Counter)
- `historian_vector_recalls_total` (Counter)
- `historian_embeddings_stored_total` (Counter)

### 5.2 Negotiator Agent

**Purpose:** Revenue optimization. The Negotiator prevents "No" answers by analyzing booking intent and generating strategic alternatives.

**File:** `agents/negotiator/app/main.py`

**Intent detection** (regex + keyword):
- `reservation_request` -- "reservation", "book", "table", "reserve", "seat"
- `walk_in` -- "walk in", "right now", "available now", "tonight"
- `modification_request` -- "cancel", "change", "modify", "reschedule"
- `special_occasion` -- "birthday", "anniversary", "celebration"
- `complaint` -- "complaint", "unhappy", "disappointed"

**Time parsing:** Regex `\d{1,2}(?::\d{2})?\s*(?:am|pm)` -- avoids misinterpreting party sizes as times.

**Party size parsing:** Keyword prefix match (`party of`, `table for`, `group of`) followed by `\d{1,2}`.

**Strategy engine:**
- Available slot match -> Confirm booking (confidence 0.95)
- Fully booked -> Revenue-positive alternatives from `REVENUE_PRIORITY` list (confidence 0.85)
- Day only, large party (6+) -> Suggest private dining (confidence 0.88)
- Day only, no time -> Suggest off-peak for table turnover (confidence 0.80)
- Walk-in -> Offer bar/high-top for immediate seating (confidence 0.80)
- Special occasion -> Upsell private dining + champagne (confidence 0.90)
- Complaint -> Escalate with empathy (confidence 0.85)

**Revenue priority** (highest to lowest score): Private > Dining Room > Patio > High-Top > Bar

**Seating inventory:**
| Type          | Capacity | Label               |
|---------------|----------|---------------------|
| dining_room   | 60       | Main Dining Room    |
| patio         | 24       | Garden Patio        |
| high_top      | 16       | High-Top Tables     |
| bar           | 12       | Bar Seating         |
| private       | 10       | Private Dining Room |

**Prometheus metrics:**
- `negotiator_strategies_published_total` (Counter)
- `negotiator_analysis_latency_seconds` (Histogram)
- `negotiator_transcripts_processed_total` (Counter)

### 5.3 Sommelier Agent

**Purpose:** Food safety and wine expertise. The Sommelier filters menus by allergens/exclusions and recommends wine pairings.

**Files:**
- `agents/sommelier/app/main.py` -- Kafka consumer, intent detection, filtering, pairing
- `agents/sommelier/app/ontology.py` -- Allergen families, flavor profiles, wine pairing map

**Intent detection:**
- `ingredient_filter` -- "no", "without", "allergic to", "can't eat", "avoid", etc.
- `wine_pairing` -- "pair", "wine", "drink with", "complement", etc.
- `menu_inquiry` -- "menu", "dish", "what do you have", "recommend", etc.

**Allergen family expansion:**

When a guest says "no dairy", the system expands "dairy" to all 14 members: milk, cream, butter, cheese, yogurt, whey, casein, ghee, parmesan, mozzarella, ricotta, mascarpone, brie, gruyere.

15 allergen families defined in `ontology.py`:
allium, nightshade, tree_nut, peanut, dairy, gluten, shellfish, fish, egg, soy, sesame, sulfite, celery, mustard, cilantro.

The `expand_allergen_to_family()` function checks if the input IS a family name first (e.g., "dairy" -> dairy family), then falls back to member lookup (e.g., "cream" -> dairy family).

**Wine pairing engine:**

7 flavor categories mapped to wine recommendations:
- rich_savory -> Barolo, Cabernet Sauvignon, Brunello
- bright_acidic -> Sancerre, Chablis, Vermentino
- creamy_mild -> Chardonnay (Burgundy), Viognier, Champagne
- spicy_bold -> Gewurztraminer, Riesling, Zinfandel
- sweet_aromatic -> Sauternes, Moscato d'Asti
- herbaceous -> Sauvignon Blanc, Gruner Veltliner
- briny_oceanic -> Muscadet, Albarino, Champagne

Pairing flow: Match dish to menu item -> extract flavor tags -> find best flavor category (highest keyword overlap) -> return wine pairings.

**Menu caching:** Full menu is cached in a local dict with a 60-second TTL. Refreshes via `SCAN` only when the cache expires. This eliminates per-transcript Redis scans.

**Prometheus metrics:**
- `sommelier_filters_published_total` (Counter)
- `sommelier_pairings_published_total` (Counter)
- `sommelier_filter_latency_seconds` (Histogram)
- `sommelier_transcripts_processed_total` (Counter)

---

## 6. Shared Utilities (`shared/`)

All agents and the Gateway import from `shared/` to avoid code duplication.

### 6.1 `shared/redis_utils.py`

| Function               | Signature                                                          | Purpose                                    |
|------------------------|--------------------------------------------------------------------|--------------------------------------------|
| `connect_redis()`      | `(retries=30, delay=2.0) -> redis.Redis`                         | Synchronous connect with retry loop         |
| `async_connect_redis()` | `(retries=30, delay=2.0) -> redis.Redis`                        | Async-compatible connect (sync client, async retry) |
| `publish_whisper()`    | `(client, agent, whisper_type, session_id, payload) -> str`       | Dual-write to Stream + session List         |
| `read_whispers()`      | `(client, session_id, last_id, count) -> list[dict]`             | Read from Stream by session_id              |
| `set_guest_profile()`  | `(client, phone, profile) -> None`                                | Write to VIP Vault                          |
| `get_guest_profile()`  | `(client, phone) -> dict | None`                                  | Read from VIP Vault                         |

**Constants:**
- `REDIS_HOST` / `REDIS_PORT` -- from env vars, default `redis:6379`
- `WHISPER_STREAM_KEY` = `aura:whisper_bus`
- `GUEST_KEY_PREFIX` = `aura:guest:`
- `MENU_KEY_PREFIX` = `aura:menu:`
- `WHISPER_STREAM_MAXLEN` = 10000
- `WHISPER_SESSION_TTL_S` = 300

**Whisper dual-write pattern:**
```python
# Stream (observability, Grafana)
client.xadd(WHISPER_STREAM_KEY, entry, maxlen=10000, approximate=True)
# Session list (Gateway retrieval)
client.rpush(f"aura:whispers:{session_id}", json.dumps(entry))
client.expire(f"aura:whispers:{session_id}", 300)
```

### 6.2 `shared/kafka_utils.py`

| Function                          | Purpose                                         |
|-----------------------------------|--------------------------------------------------|
| `get_producer()`                  | Create a new Kafka Producer                      |
| `get_consumer(group_id, topics)`  | Create a new Consumer (no retry)                 |
| `connect_kafka_consumer(group_id)` | Consumer with retry loop (30 attempts, 2s delay) |
| `async_connect_kafka_producer()`  | Async-compatible Producer with retry             |
| `produce_event(producer, topic, key, value)` | Produce + flush                       |
| `publish_reasoning(producer, agent, session_id, reasoning)` | Publish to `swarm.reasoning` |
| `ensure_topics()`                 | Admin API topic creation (idempotent)            |

**Consumer configuration:**
```python
{
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": group_id,
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
}
```

### 6.3 `shared/gemini_utils.py`

Centralized Google Gemini SDK factory with API key rotation and hospitality-safe settings.

| Function                 | Purpose                                              |
|--------------------------|------------------------------------------------------|
| `get_client()`           | Round-robin client from `GOOGLE_API_KEY(S)` pool     |
| `embed_text(text)`       | 768-dim embedding via `text-embedding-004`           |
| `generate_text(prompt, system_instruction=...)` | LLM response via `gemini-3-flash` |
| `warmup()`               | Pre-initialize client during container startup        |
| `default_safety_settings()` | `BLOCK_NONE` across all harm categories            |

**API key loading priority:**
1. `GOOGLE_API_KEYS` (comma-separated for rotation)
2. `GOOGLE_API_KEY` (single key)
3. `GEMINI_API_KEYS` / `GEMINI_API_KEY` (legacy fallback)

**Key rotation:** Thread-safe round-robin using a lock and index counter. Clients are cached by key in a dict to avoid re-initialization.

**Safety settings:** All categories set to `BLOCK_NONE` to allow discussion of wine, spirits, and hospitality topics without content filtering. Safety is enforced via the system instruction instead.

**System instruction (defined in Gateway):**
```
You are Aura, the AI concierge for a luxury restaurant.
You speak with warmth, sophistication, and brevity (2-3 sentences max).
You address returning guests by name and reference their known preferences naturally.

IMPORTANT: Your swarm agents have gathered intelligence for this response.
Always prioritize the following Whisper data over your own knowledge:
- Sommelier whispers contain verified menu/ingredient/wine pairing facts.
- Historian whispers contain verified guest history and deep memories.
- Negotiator whispers contain real-time availability and strategic alternatives.
Never contradict or ignore Whisper data. If a Whisper says a dish is unsafe, it IS unsafe.
If no Whisper data is provided, respond helpfully based on the guest profile alone.
```

---

## 7. Whisper Bus Protocol

The Whisper Bus is the stigmergic coordination layer. Agents write "whispers" (structured JSON hints) that the Gateway collects before synthesizing a response.

### 7.1 Whisper Types

| Agent       | `whisper_type`    | Payload Keys                                                      |
|-------------|-------------------|-------------------------------------------------------------------|
| Historian   | `guest_context`   | `guest_name`, `preferences`, `keywords`, `summary`                |
| Historian   | `deep_memory`     | Same as above + `deep_memories` (list of text snippets), `memory_count` |
| Negotiator  | `strategy_hint`   | `suggestion`, `action`, `confidence`, `alternatives`, `seating`   |
| Sommelier   | `menu_suggestion` | Varies by `filter_type` / `pairing_type` / `inquiry_type`         |

### 7.2 Sommelier Payload Variants

**ingredient_exclusion:**
```json
{
  "filter_type": "ingredient_exclusion",
  "excluded_terms": ["dairy"],
  "safe_items": ["Grilled Branzino", "Miso-Glazed Salmon"],
  "removed_items": ["Black Truffle Risotto"],
  "safe_count": 8,
  "removed_count": 6,
  "reasoning": ["'dairy' is part of the 'dairy' family. Expanding filter to include: milk, cream, butter..."]
}
```

**wine pairing:**
```json
{
  "pairing_type": "wine",
  "dish": "Dry-Aged Ribeye",
  "flavor_category": "rich_savory",
  "recommendations": [
    {"wine": "Barolo", "type": "red", "reason": "Bold tannins complement umami..."}
  ]
}
```

**menu inquiry:**
```json
{
  "inquiry_type": "general_menu",
  "total_items": 14,
  "highlights": ["Grilled Branzino", "Dry-Aged Ribeye", "Pan-Seared Diver Scallops"]
}
```

### 7.3 Timing

| Parameter              | Value  | Location                      |
|------------------------|--------|-------------------------------|
| Poll interval          | 25ms   | `WHISPER_POLL_INTERVAL_MS`    |
| Max wait               | 400ms  | `WHISPER_MAX_WAIT_MS`         |
| Session list TTL       | 300s   | `WHISPER_SESSION_TTL_S`       |
| Stream maxlen          | 10000  | `WHISPER_STREAM_MAXLEN`       |

The Gateway deletes the session list after collection (whether whispers were found or not) to prevent stale reads.

---

## 8. VIP Identity Vault

Guest profiles are stored in RedisJSON and seeded via `scripts/seed_data.py`.

### 8.1 Schema

```json
{
  "phone_number": "+15551234567",
  "name": "Julian",
  "last_call_date": "2026-02-10",
  "preference_tags": ["red wine", "window table", "quiet seating", "Barolo"],
  "notes": {
    "preferred_table": "table 4",
    "dietary": "no cilantro",
    "anniversary_month": "March",
    "vip_tier": "platinum"
  }
}
```

### 8.2 Seeded Profiles

| Name       | Phone            | VIP Tier  | Key Preferences                         |
|------------|------------------|-----------|-----------------------------------------|
| Julian     | +15551234567     | Platinum  | Red wine, Barolo, window table, no cilantro |
| Isabella   | +15559876543     | Gold      | Champagne, private dining, seafood      |
| Marcus     | +15555551212     | Silver    | Whiskey, bar seating, steak             |
| Sophia     | +15553334444     | Gold      | White wine, Sancerre, garden patio, vegetarian |
| Alexander  | +15557778888     | Platinum  | Red wine, Brunello, corner booth, tasting menu |

---

## 9. Menu System

14 structured menu items seeded via `scripts/seed_menu.py`. Each item includes:

- `id`, `name`, `category` (appetizer/entree/special/dessert)
- `description`, `price`
- `ingredients` (individual ingredient list)
- `allergens` (allergen identifiers)
- `allergen_family` (family-level tags for expansion matching)
- `flavor_profile` (tags for wine pairing)

The Sommelier reads the full menu from Redis on demand (with 60s cache) and filters/pairs against it.

---

## 10. Gemini Integration

### 10.1 Models

| Purpose      | Model               | Dimensions | Container(s)           |
|--------------|----------------------|------------|------------------------|
| Embeddings   | `text-embedding-004` | 768        | Historian              |
| LLM Reasoning | `gemini-3-flash`   | N/A        | Gateway                |

### 10.2 Embedding Pipeline

```
Text -> Gemini embed_content() -> 768 raw floats -> L2 normalize -> store in RedisJSON
```

Normalization uses pure Python (no numpy):
```python
norm = math.sqrt(sum(v * v for v in raw))
return [v / norm for v in raw] if norm > 0 else raw
```

KNN search packs the query vector into binary with `struct.pack()`:
```python
embedding_bytes = struct.pack(f"{len(embedding)}f", *embedding)
```

### 10.3 LLM Prompt Assembly

The Gateway builds a prompt from four blocks:
1. `GUEST PROFILE:` -- Name and preference tags (if known guest)
2. `SWARM WHISPERS:` -- JSON-formatted whisper payloads from all agents
3. `GREETING ALREADY SENT:` -- The greeting that was already delivered
4. `GUEST SAYS:` -- The caller's actual words

This is sent to Gemini with the system instruction above. Temperature 0.4, max 300 tokens.

### 10.4 Graceful Degradation

If `GOOGLE_API_KEY` is not set or the Gemini API call fails:
- Embedding calls raise `RuntimeError` (caught by callers, logged as warnings)
- `generate_text()` raises `RuntimeError`, caught by `_generate_response()` which falls back to `_fallback_response()` -- a deterministic rule-based engine
- `warmup()` logs a warning and continues (does not block startup)

The fallback response engine handles:
- Known guest + reservation intent -> "Of course, {name}! I'd love to arrange that."
- Known guest + wine intent -> Barolo recommendation or Sommelier hint
- Known guest + allergen intent -> Sommelier-filtered response
- Known guest + deep memories -> "Welcome back, I remember your last visit."
- Unknown guest + allergen -> Sommelier hint if available
- Default -> "I'd be delighted to help you."

---

## 11. Multi-Tenant Architecture

All services support a `TENANT_ID` environment variable (default: `default`) that namespaces data:

- **Kafka events** include `tenant_id` in the payload
- **Vector embeddings** are tagged with `tenant_id` and filtered in KNN queries
- **Redis keys** are not namespaced by tenant (planned for Phase 3)

For full SaaS isolation, the planned approach is Kafka topic namespacing (`{tenant}.swarm.transcripts`) and Redis keyspace prefixing (`{tenant}:aura:guest:...`).

---

## 12. Observability

### 12.1 Prometheus Metrics

**Gateway (`:8000/metrics`):**
| Metric                         | Type      | Labels                     |
|--------------------------------|-----------|----------------------------|
| `aura_calls_total`             | Counter   | --                         |
| `aura_active_calls`            | Gauge     | --                         |
| `aura_response_latency_seconds` | Histogram | -- (buckets: 50ms-2s)     |
| `aura_whispers_total`          | Counter   | `agent`, `whisper_type`    |
| `aura_token_usage_total`       | Counter   | `agent`, `token_type`      |
| `aura_sentiment_score`         | Gauge     | --                         |
| `aura_kafka_events_total`      | Counter   | `topic`                    |

**Historian (`:8001`):**
| Metric                                | Type      |
|---------------------------------------|-----------|
| `historian_whispers_published_total`   | Counter   |
| `historian_lookup_latency_seconds`     | Histogram |
| `historian_transcripts_processed_total` | Counter  |
| `historian_vector_recalls_total`       | Counter   |
| `historian_embeddings_stored_total`    | Counter   |

**Negotiator (`:8002`):**
| Metric                                  | Type      |
|-----------------------------------------|-----------|
| `negotiator_strategies_published_total`  | Counter   |
| `negotiator_analysis_latency_seconds`    | Histogram |
| `negotiator_transcripts_processed_total` | Counter   |

**Sommelier (`:8003`):**
| Metric                                | Type      |
|---------------------------------------|-----------|
| `sommelier_filters_published_total`    | Counter   |
| `sommelier_pairings_published_total`   | Counter   |
| `sommelier_filter_latency_seconds`     | Histogram |
| `sommelier_transcripts_processed_total` | Counter  |

### 12.2 Grafana Dashboard

Pre-provisioned at `localhost:3000` (admin / aura_admin). Dashboard UID: `aura-swarm-v1`.

**Panels:**
1. Active Voice Calls (stat)
2. Total Calls Handled (stat)
3. Response Latency P95 (gauge, thresholds: green <300ms, yellow <500ms, red >500ms)
4. Whispers Published per Agent (stat)
5. Token Usage per Agent (timeseries)
6. Sentiment Heatmap (timeseries, green-yellow-red gradient)
7. Historian VIP Lookup Latency P50/P95 (timeseries)
8. Negotiator Reasoning Trace (transcripts/s, strategies/s, P95 latency)
9. Kafka Events Rate (bar chart by topic)
10. Active Whisper Log (table)

---

## 13. Gateway API Reference

### `GET /health`

Returns service health including Redis and Kafka connectivity.

```json
{"status": "healthy", "redis": true, "kafka": true}
```

### `POST /api/simulate-call`

Simulates an inbound phone call without Twilio.

**Request:**
```json
{
  "caller_number": "+15551234567",
  "text": "Hello, I'd like to book my usual table for Friday evening."
}
```

**Response:**
```json
{
  "session_id": "uuid",
  "caller": "+15551234567",
  "guest_profile": { ... } | null,
  "greeting": "Welcome back, Julian! ...",
  "input_text": "...",
  "whispers": [ ... ],
  "response": "...",
  "latency_ms": 250.0
}
```

### `POST /twilio/voice`

Twilio webhook for inbound calls. Returns TwiML with greeting and WebSocket stream URL.

### `WS /ws/media/{session_id}`

Twilio media stream WebSocket for real-time audio relay.

### `GET /metrics`

Prometheus metrics endpoint (mounted ASGI app).

---

## 14. Configuration Reference

| Variable                  | Default              | Used By            | Purpose                              |
|---------------------------|----------------------|--------------------|--------------------------------------|
| `GOOGLE_API_KEY`          | (none)               | Gateway, Historian | Gemini API authentication            |
| `GEMINI_GENERATIVE_MODEL` | `gemini-3-flash`    | Gateway            | LLM model for response synthesis     |
| `GEMINI_EMBEDDING_MODEL`  | `text-embedding-004` | Historian          | Embedding model for vector memory    |
| `REDIS_HOST`              | `redis`              | All                | Redis hostname                       |
| `REDIS_PORT`              | `6379`               | All                | Redis port                           |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092`         | All                | Kafka broker address                 |
| `TENANT_ID`               | `default`            | All                | Multi-tenant namespace               |
| `TWILIO_ACCOUNT_SID`      | (none)               | Gateway            | Twilio auth (voice calls only)       |
| `TWILIO_AUTH_TOKEN`        | (none)               | Gateway            | Twilio auth (voice calls only)       |
| `ELEVENLABS_API_KEY`       | (none)               | Gateway            | TTS integration (voice calls only)   |
| `ELEVENLABS_VOICE_ID`      | (none)               | Gateway            | TTS voice selection                  |
| `GRAFANA_ADMIN_PASSWORD`   | `aura_admin`         | Grafana            | Dashboard login                      |

---

## 15. Dependency Matrix

### Gateway (`gateway/requirements.txt`)
| Package            | Version   | Purpose                      |
|--------------------|-----------|------------------------------|
| fastapi            | 0.115.6   | HTTP/WebSocket framework     |
| uvicorn[standard]  | 0.34.0    | ASGI server                  |
| redis[hiredis]     | 5.2.1     | Redis client + C parser      |
| confluent-kafka    | 2.6.1     | Kafka producer (C extension) |
| prometheus-client  | 0.21.1    | Metrics exposition           |
| httpx              | 0.28.1    | HTTP client (Twilio calls)   |
| websockets         | 14.1      | WebSocket support            |
| google-genai       | >=1.0.0   | Gemini SDK                   |

### Historian (`agents/historian/requirements.txt`)
| Package            | Version   | Purpose                      |
|--------------------|-----------|------------------------------|
| redis[hiredis]     | 5.2.1     | Redis + RediSearch client    |
| confluent-kafka    | 2.6.1     | Kafka consumer               |
| prometheus-client  | 0.21.1    | Metrics exposition           |
| google-genai       | >=1.0.0   | Gemini embeddings            |

### Negotiator (`agents/negotiator/requirements.txt`)
| Package            | Version   | Purpose                      |
|--------------------|-----------|------------------------------|
| redis[hiredis]     | 5.2.1     | Redis client                 |
| confluent-kafka    | 2.6.1     | Kafka consumer               |
| prometheus-client  | 0.21.1    | Metrics exposition           |

### Sommelier (`agents/sommelier/requirements.txt`)
| Package            | Version   | Purpose                      |
|--------------------|-----------|------------------------------|
| redis[hiredis]     | 5.2.1     | Redis client                 |
| confluent-kafka    | 2.6.1     | Kafka consumer               |
| prometheus-client  | 0.21.1    | Metrics exposition           |

---

## 16. File Structure

```
project-aura/
  docker-compose.yml              # 9-service orchestration
  .env.example                    # Environment variable template
  pyproject.toml                  # Python project metadata
  README.md                       # Quick-start guide
  CODE_REVIEW.md                  # 25-finding code review
  ARCHITECT.md                    # This document

  gateway/
    Dockerfile                    # python:3.12-slim + librdkafka
    requirements.txt
    app/
      main.py                     # FastAPI app, Aura Loop, Gemini LLM
      config.py                   # Environment variable declarations
      metrics.py                  # Prometheus metric definitions

  agents/
    historian/
      Dockerfile
      requirements.txt
      app/
        __init__.py
        main.py                   # Kafka consumer, VIP lookup, whisper pub
        vector_memory.py          # Gemini embeddings, RediSearch KNN

    negotiator/
      Dockerfile
      requirements.txt
      app/
        __init__.py
        main.py                   # Intent analysis, strategy engine

    sommelier/
      Dockerfile
      requirements.txt
      app/
        __init__.py
        main.py                   # Menu filtering, wine pairing
        ontology.py               # Allergen families, flavor profiles

  shared/
    __init__.py
    redis_utils.py                # Connection, whisper pub, guest CRUD
    kafka_utils.py                # Connection, produce, consume, topics
    gemini_utils.py               # Client factory, embed, generate, warmup

  scripts/
    seed_data.py                  # Seed 5 VIP guest profiles
    seed_menu.py                  # Seed 14 menu items
    julian_test.py                # End-to-end "Julian Test"
    onion_test.py                 # Allergen filtering validation

  monitoring/
    prometheus/
      prometheus.yml              # Scrape config for all 4 services
    grafana/
      provisioning/
        datasources/
          datasource.yml          # Prometheus data source
        dashboards/
          dashboard.yml           # Dashboard provider config
          aura-swarm.json         # Pre-built dashboard (10 panels)
```

---

## 17. Startup & Operations

### First-time setup

```bash
# 1. Configure
cp .env.example .env
# Edit .env -- set GOOGLE_API_KEY at minimum

# 2. Start all services
docker compose up --build -d

# 3. Seed data (run from host, connects to localhost:6379)
pip install redis
python scripts/seed_data.py
python scripts/seed_menu.py

# 4. Verify
python scripts/julian_test.py
```

### Re-seeding after rebuild

Redis data persists in the `redis-data` Docker volume. However, if you run `docker compose down -v` (which removes volumes), you must re-seed:

```bash
python scripts/seed_data.py
python scripts/seed_menu.py
```

### Verifying guest data manually

```bash
docker exec -it aura-redis redis-cli JSON.GET aura:guest:+15551234567
```

### Viewing container logs

```bash
docker logs aura-gateway     # Gateway startup, guest lookups, whisper collection
docker logs aura-historian    # VIP lookups, whisper publishing, embedding storage
docker logs aura-negotiator   # Intent detection, strategy generation
docker logs aura-sommelier    # Menu filtering, wine pairing
```

### Grafana

- URL: `http://localhost:3000`
- Credentials: admin / aura_admin (or `GRAFANA_ADMIN_PASSWORD`)
- Dashboard: "Aura Cognitive Swarm" (auto-provisioned)

---

## 18. Known Issues & Diagnostics

### Latency

**Target:** <400ms end-to-end.

**Current state:** ~2500ms on first call (Gemini API cold-start + network latency). Subsequent calls are faster due to connection reuse.

**Warm-up mitigation:** Both Gateway and Historian call `warmup()` during startup, which makes a test embedding call to pre-initialize the Gemini HTTP client. This reduces first-call latency but doesn't eliminate Gemini API round-trip time.

**Event loop blocking:** Gateway wraps the Gemini `generate_text()` call in `asyncio.to_thread()` to prevent blocking the async event loop during the ~200-500ms API call.

### Whisper Race Condition

The Gateway waits 400ms for whispers. The Historian must: receive Kafka message -> look up guest -> extract context -> publish whisper, all within that window.

**Mitigation:** The Historian publishes the basic `guest_context` whisper BEFORE doing the expensive Gemini embedding and KNN recall. The fast path (Redis lookup + whisper publish) takes ~10-20ms. Deep memories arrive as a second `deep_memory` whisper that may miss the 400ms window.

**If 0 whispers are found:** Check Historian logs for "Whisper published for session=..." messages. If absent, the Historian may not be receiving the Kafka message (check consumer group assignment, Kafka connectivity).

### Guest Lookup Failure

If Julian is not recognized, the most common cause is that `seed_data.py` was not run (or Redis data was lost after a volume-destroying rebuild).

**Diagnostics:** Gateway logs will show either:
- `Guest found: key=aura:guest:+15551234567 name=Julian` (working)
- `No guest profile at key=aura:guest:+15551234567` (data missing -- re-seed)
- `Guest lookup failed for key=...: ...` (Redis error -- check connection/module)

### Gemini API Failure

If `GOOGLE_API_KEY` is not set or invalid, all Gemini calls fail and the system degrades gracefully:
- Gateway uses rule-based fallback responses
- Historian skips embeddings (logs warnings)
- Warm-up logs "No GOOGLE_API_KEY set; skipping Gemini warm-up"

---

## 19. Performance Targets (Phase 1 DOD)

| Criterion                        | Target         | Measurement                                    |
|----------------------------------|----------------|------------------------------------------------|
| Container health                 | All 9 healthy  | `docker compose ps`                            |
| VIP warm recognition             | Julian by name | `julian_test.py` step 3                       |
| Wine preference recall           | "red wine"     | `julian_test.py` greeting check                |
| Reasoning trace in Kafka         | Present        | `swarm.reasoning` topic                        |
| P95 vocal response latency       | <500ms         | `aura_response_latency_seconds` P95            |
| Allergen false positive rate     | 0%             | `onion_test.py`                                |

---

## 20. Evolution History

| Phase | What Changed                                                                                  |
|-------|-----------------------------------------------------------------------------------------------|
| 1.0   | Initial 8-service swarm: Gateway, Historian, Negotiator, Redis, Kafka, ZK, Prometheus, Grafana |
| 1.1   | Sommelier Agent added (9 services), allergen ontology, wine pairing, menu seed script         |
| 1.2   | Historian vector memory with local sentence-transformers (384-dim), KNN deep context recall   |
| 1.3   | Multi-tenant `TENANT_ID` support, inventory-aware Negotiator, Onion Test                     |
| 1.4   | Code review (25 findings), Gateway/Sommelier payload integration fix                         |
| 1.5   | Whisper Bus stabilization: stream trimming (maxlen=10000), session-keyed retrieval            |
| 1.6   | Semantic memory fix: SHA256 -> sentence-transformers all-MiniLM-L6-v2 (384-dim)              |
| 1.7   | Industry module fixes: allergen family expansion, menu caching, regex intent parsing, revenue score |
| 1.8   | Global standards: centralized `shared/` utilities, Prometheus hygiene, graceful shutdown      |
| 1.9   | OpenAI config removed, Gemini config placeholders added                                       |
| 2.0   | Gemini migration: `text-embedding-004` (768-dim), `gemini-3-flash` with system_instruction, `shared/gemini_utils.py` factory |
| 2.1   | Post-migration diagnostics: warm-up sequence, whisper audit logging, Docker image shrink (numpy/sentence-transformers removed) |
| 2.2   | Whisper timing fix: Historian publishes before embedding, Gateway async Gemini calls, guest lookup logging |
