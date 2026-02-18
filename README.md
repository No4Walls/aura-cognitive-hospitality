# Project Aura - Cognitive Swarm for Luxury Hospitality

A high-performance hospitality AI system powered by a **Cognitive Swarm** -- a decentralized assembly of specialized agents collaborating through a shared environment. The primary user is the luxury restaurant guest, and the core value proposition is **Warm Recognition**: an AI that instantly knows the caller's identity and preferences without manual input.

---

## Architecture Overview

```
                          +------------------+
                          |   Twilio Voice    |
                          |   (Inbound Call)  |
                          +--------+---------+
                                   |
                          WebSocket (8kHz audio)
                                   |
                          +--------v---------+
                          |  Aura Gateway    |
                          |  (FastAPI)       |
                          |  :8000           |
                          +--+-----+-----+--+
                             |     |     |
              +--------------+     |     +--------------+
              |                    |                    |
     Kafka: swarm.transcripts     |            Redis: Whisper Bus
              |                    |                    |
     +--------v--------+          |          +---------v--------+
     |   Historian      |         |          |   Negotiator      |
     |   Agent          |         |          |   Agent            |
     |   (Container)    |         |          |   (Container)      |
     +--------+---------+         |          +---------+----------+
              |                    |                    |
              +-- Redis: VIP Vault |   Redis: Strategy Hints --+
                                   |
                          +--------v---------+
                          | ElevenLabs TTS   |
                          | (Voice Response) |
                          +--------+---------+
                                   |
                          +--------v---------+
                          |   Twilio Voice    |
                          |   (Audio Out)     |
                          +------------------+
```

### The Aura Loop (Execution Trace)

1. **Twilio** streams audio to the Aura Gateway via WebSocket.
2. **ElevenLabs STT** converts audio to text.
3. **Aura Gateway** publishes the transcript to Kafka (`swarm.transcripts`) with `tenant_id`.
4. **Historian Agent** consumes the transcript, queries the VIP Vault, stores vector embeddings, performs KNN recall of deep memories, and writes a `guest_context` whisper to Redis.
5. **Negotiator Agent** evaluates booking intent, identifies Full House events, calculates seating alternatives, and writes a `strategy_hint` whisper to Redis.
6. **Sommelier Agent** detects ingredient/pairing intent, filters menu with allergen family expansion, finds wine pairings by flavor profile, and writes a `menu_suggestion` whisper to Redis.
7. **Aura Gateway** reads the Redis Whisper Bus (all agent whispers) and generates a final response.
8. **ElevenLabs TTS** synthesizes audio and sends it back through Twilio.

---

## Services

| Service | Container | Port | Description |
|---|---|---|---|
| **Aura Gateway** | `aura-gateway` | `8000` | FastAPI application. WebSocket media relay, Kafka publisher, Whisper Bus reader, Prometheus metrics. Multi-tenant aware. |
| **Historian Agent** | `aura-historian` | `8001` (metrics) | Kafka consumer. VIP Vault lookups, vector embedding storage, KNN deep context recall. Writes `guest_context` whispers with deep memories. |
| **Negotiator Agent** | `aura-negotiator` | `8002` (metrics) | Intent analyzer and booking strategy engine. Inventory-aware seating alternatives (bar, high-top, patio, private). Writes `strategy_hint` whispers. |
| **Sommelier Agent** | `aura-sommelier` | `8003` (metrics) | Ingredient-level reasoning engine. Allergen family expansion, menu filtering, wine pairing by flavor profile. Writes `menu_suggestion` whispers. |
| **Redis Stack** | `aura-redis` | `6379` | Whisper Bus (Streams) + VIP Vault (RedisJSON) + Vector Memory (RediSearch) + Menu Store (RedisJSON). |
| **Kafka** | `aura-kafka` | `9092` | Durable event backbone. Topics: `swarm.transcripts`, `swarm.reasoning`. |
| **Zookeeper** | `aura-zookeeper` | `2181` | Kafka coordination. |
| **Prometheus** | `aura-prometheus` | `9090` | Metrics scraping from all agent services (4 targets). |
| **Grafana** | `aura-grafana` | `3000` | Pre-provisioned observability dashboard. |

All services communicate over the private `aura-net` Docker bridge network.

---

## Technology Stack

| Layer | Technology | Rationale |
|---|---|---|
| Telephony | Twilio Voice Streams | Bi-directional WebSocket streaming of raw 8kHz audio |
| STT / TTS | ElevenLabs | Unified vocal identity; ultra-low latency speech-to-text and synthesis |
| Core Interaction | FastAPI (Python 3.12) | Asynchronous gateway for managing WebSocket media relays |
| Event Backbone | Kafka + Sidecars | Durable, replayable event logs for every reasoning trace |
| Hot Memory | Redis Stack (JSON/Search) | Sub-millisecond Whisper Bus for stigmergic coordination |
| Observability | Prometheus & Grafana | Tracking agent latency, sentiment scores, and LLM costs |

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.12+ (for running scripts)

### 1. Launch the Swarm

```bash
docker compose up --build -d
```

This starts all 9 services. Verify with:

```bash
docker compose ps
```

All containers should report `healthy` or `running` status.

### 2. Seed Data

```bash
pip install redis numpy
python scripts/seed_data.py
python scripts/seed_menu.py
```

This loads 5 VIP guest profiles and 14 structured menu items into Redis.

**VIP Guests:**

| Guest | Phone | Preferences |
|---|---|---|
| Julian | +15551234567 | Red wine, Barolo, window table, no cilantro |
| Isabella | +15559876543 | Champagne, private dining, seafood |
| Marcus | +15555551212 | Whiskey, bar seating, steak |
| Sophia | +15553334444 | White wine, Sancerre, garden patio, vegetarian |
| Alexander | +15557778888 | Red wine, Brunello, corner booth, tasting menu |

### 3. Run Tests

**Julian Test** (Warm Recognition):
```bash
pip install httpx
python scripts/julian_test.py
```

Validates: warm recognition, preference recall, latency < 400ms, reasoning traces.

**Onion Test** (Allergen Safety):
```bash
python scripts/onion_test.py
```

Validates: 0% false positives on allergen filtering, allergen family expansion, latency < 150ms, intent detection accuracy, wine pairing logic.

### 4. Access the Dashboards

| Dashboard | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `aura_admin` |
| Prometheus | http://localhost:9090 | -- |
| Gateway Health | http://localhost:8000/health | -- |
| Gateway Metrics | http://localhost:8000/metrics | -- |

---

## API Reference

### `GET /health`

Returns health status of the gateway and its connections.

```json
{
  "status": "healthy",
  "redis": true,
  "kafka": true
}
```

### `POST /twilio/voice`

Twilio voice webhook. Accepts incoming call form data, performs VIP lookup, returns TwiML with greeting and WebSocket stream URL.

### `WebSocket /ws/media/{session_id}`

Bidirectional WebSocket endpoint for Twilio media streams. Receives audio chunks and coordinates with the swarm for real-time responses.

### `POST /api/simulate-call`

Simulate a voice call without Twilio. Useful for testing and development.

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
  "guest_profile": { "name": "Julian", "preference_tags": ["red wine", "..."] },
  "greeting": "Welcome back, Julian! ...",
  "input_text": "...",
  "whispers": [ { "agent": "historian", "whisper_type": "guest_context", "..." : "..." } ],
  "response": "Of course, Julian! ...",
  "latency_ms": 52.3
}
```

---

## Core Concepts

### Stigmergic Coordination

Agents do **not** call each other directly. Instead, they modify the shared environment (Redis Streams) to "whisper" context to the Gateway. This decoupled pattern allows agents to be added, removed, or scaled independently.

**Whisper Types:**

| Type | Producer | Description |
|---|---|---|
| `guest_context` | Historian | Guest identity, preferences, deep vector memories, and historical context |
| `strategy_hint` | Negotiator | Tactical booking suggestions, seating alternatives, and upsell opportunities |
| `sentiment` | Gateway | Caller sentiment score |
| `menu_suggestion` | Sommelier | Ingredient filtering results, wine pairings, and menu recommendations |

### VIP Identity Vault

Guest profiles are stored in RedisJSON at keys `aura:guest:{phone_number}`:

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

### Sommelier Agent (Phase 2)

The Sommelier provides ingredient-level reasoning with three capabilities:

**Allergen Family Expansion:** When a guest says "no onions," the Sommelier expands the filter to the entire allium family (onion, shallot, garlic, leek, chive, scallion, spring onion). 15 allergen families are defined: allium, nightshade, tree_nut, peanut, dairy, gluten, shellfish, fish, egg, soy, sesame, sulfite, celery, mustard, cilantro.

**Wine Pairing by Flavor Profile:** Menu items are tagged with flavor categories (rich_savory, bright_acidic, creamy_mild, spicy_bold, sweet_aromatic, herbaceous, briny_oceanic). The Sommelier maps these to expert wine recommendations with reasoning.

**Safety-First:** If an ingredient has no known allergen family, the filter applies it as an exact match and warns the user. Unknown ingredients are never silently ignored.

### Semantic Vector Memory (Phase 2)

The Historian stores every transcript as a 128-dimensional vector embedding in RediSearch. On each inbound call, it performs KNN (K-Nearest Neighbor) search to recall related past interactions -- anniversaries, preferences, complaints -- and includes them as `deep_memories` in the whisper. Vectors are tagged with `tenant_id` and `caller_id` for strict multi-tenant isolation.

### Multi-Tenant Architecture (Phase 2)

All services accept a `TENANT_ID` environment variable. Tenant isolation is enforced at:
- **Kafka events:** Every transcript includes `tenant_id` for downstream filtering.
- **Redis vectors:** All embeddings are tagged with `tenant_id` and filtered in KNN queries.
- **Redis keys:** Menu items and guest profiles use tenant-aware key prefixes.

### Inventory-Aware Negotiation (Phase 2)

The Negotiator now understands 5 seating types (Main Dining Room, Bar, High-Top, Patio, Private Dining Room) with capacity and revenue priority. When the dining room is full, it suggests revenue-positive alternatives: bar seating, high-tops, later time slots, or the private room for large parties.

### Event Backbone (Kafka)

Two topics power the durable reasoning layer:

- **`swarm.transcripts`** -- Every inbound/outbound utterance with session ID, caller number, tenant_id, and timestamp.
- **`swarm.reasoning`** -- Agent decision traces (what each agent found/decided, and processing latency).

---

## Grafana Dashboard

The pre-provisioned "Aura Cognitive Swarm" dashboard includes:

- **Active Voice Calls** -- Real-time gauge of concurrent sessions
- **Total Calls Handled** -- Cumulative call counter
- **Response Latency (P95)** -- Gauge with green/yellow/red thresholds at 300ms/500ms
- **Token Usage per Agent** -- Time series of LLM token consumption by agent and type
- **Sentiment Heatmap** -- Live sentiment scores per session
- **Historian VIP Lookup Latency** -- P50/P95 vault query performance
- **Negotiator Reasoning Trace** -- Transcripts processed/s, strategies published/s, analysis latency
- **Kafka Events Rate** -- Event throughput by topic
- **Active Whisper Log** -- Table of all whispers by agent and type

---

## Project Structure

```
project-aura/
├── docker-compose.yml              # 9-service swarm definition
├── .env.example                     # Environment variable template
├── pyproject.toml                   # Python project config (ruff, mypy)
├── gateway/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py                  # FastAPI app (WebSocket, APIs, Whisper Bus)
│       ├── config.py                # Environment configuration
│       └── metrics.py               # Prometheus metric definitions
├── agents/
│   ├── historian/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── main.py              # Kafka consumer, VIP lookup, vector memory, whisper writer
│   │       └── vector_memory.py     # Vector embeddings, RediSearch KNN, deep context recall
│   ├── negotiator/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       └── main.py              # Intent analyzer, inventory-aware strategy engine
│   └── sommelier/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── app/
│           ├── main.py              # Kafka consumer, menu filtering, wine pairing
│           └── ontology.py          # Allergen families, flavor profiles, wine pairing maps
├── shared/
│   ├── models.py                    # Dataclasses (Whisper, GuestProfile, TranscriptEvent)
│   ├── redis_utils.py               # Redis connection, Whisper Bus, VIP Vault helpers
│   └── kafka_utils.py               # Kafka producer/consumer, topic management
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml           # Scrape config for all agents (4 targets)
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           │   └── datasource.yml   # Prometheus datasource
│           └── dashboards/
│               ├── dashboard.yml    # Dashboard provisioning config
│               └── aura-swarm.json  # Full dashboard definition
└── scripts/
    ├── seed_data.py                 # VIP guest profile seeder
    ├── seed_menu.py                 # Restaurant menu seeder (14 items)
    ├── julian_test.py               # End-to-end warm recognition test
    └── onion_test.py                # Allergen filtering safety test
```

---

## Configuration

Copy `.env.example` to `.env` and fill in your API keys:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|---|---|---|
| `TENANT_ID` | Optional | Tenant namespace identifier (default: `default`) |
| `TWILIO_ACCOUNT_SID` | For live calls | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | For live calls | Twilio auth token |
| `TWILIO_PHONE_NUMBER` | For live calls | Your Twilio phone number |
| `ELEVENLABS_API_KEY` | For live calls | ElevenLabs API key |
| `ELEVENLABS_VOICE_ID` | For live calls | ElevenLabs voice ID for Aura's persona |
| `OPENAI_API_KEY` | For LLM reasoning | OpenAI API key |
| `GRAFANA_ADMIN_PASSWORD` | Optional | Grafana admin password (default: `aura_admin`) |

The simulate-call API and Julian Test work without any external API keys.

---

## Design Principles

1. **Stigmergic Coordination** -- Agents modify the shared environment, not each other.
2. **Containerized Isolation** -- Each agent lives in its own OCI container for independent scaling and failure isolation.
3. **Persistence by Default** -- Every decision and transcript is logged to Kafka; every VIP preference is stored in the Redis Identity Vault.
4. **The 500ms Rule** -- Total round-trip latency (user silence to agent response) must stay under 500ms.
5. **Managed Services Focus** -- Twilio and ElevenLabs APIs maximize developer velocity while maintaining local containerized control.

---

## Performance Targets

| Metric | Target | Measured At |
|---|---|---|
| Voice round-trip latency | < 500ms | Gateway -> Agent -> Response |
| VIP vault lookup | < 100ms | Historian -> Redis |
| Historian whisper latency | < 150ms | Kafka consume -> Redis write |
| Sommelier menu filtering | < 150ms | Intent detect -> filter -> whisper |
| Allergen false positive rate | 0% | Onion Test validation |
| Concurrent calls (MVP) | 50 | Docker Compose baseline |

---

## Roadmap

| Phase | Focus | Status |
|---|---|---|
| **Phase 1** | Containerized swarm, Whisper Bus, VIP Vault, Historian, Negotiator, Observability | Complete |
| **Phase 2** | Sommelier Agent, vector memory, multi-tenant isolation, inventory-aware negotiation, Onion Test | Current |
| **Phase 3** | Live Twilio/ElevenLabs integration, SaaS Portal, Redis Clustering | Planned |
| **Phase 4** | Cryptographic agent ownership, multi-region failover, HITL for edge cases | Deferred |

---

## License

Proprietary. All rights reserved.
