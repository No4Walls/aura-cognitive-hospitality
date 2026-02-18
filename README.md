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
3. **Aura Gateway** publishes the transcript to Kafka (`swarm.transcripts`).
4. **Historian Agent** consumes the transcript, queries the VIP Vault, and writes a `guest_context` whisper to Redis.
5. **Negotiator Agent** evaluates booking intent and writes a `strategy_hint` whisper to Redis.
6. **Aura Gateway** reads the Redis Whisper Bus and generates a final response.
7. **ElevenLabs TTS** synthesizes audio and sends it back through Twilio.

---

## Services

| Service | Container | Port | Description |
|---|---|---|---|
| **Aura Gateway** | `aura-gateway` | `8000` | FastAPI application. WebSocket media relay, Kafka publisher, Whisper Bus reader, Prometheus metrics. |
| **Historian Agent** | `aura-historian` | `8001` (metrics) | Kafka consumer on `swarm.transcripts`. Performs VIP Identity Vault lookups and writes `guest_context` whispers. |
| **Negotiator Agent** | `aura-negotiator` | `8002` (metrics) | Intent analyzer and booking strategy engine. Writes `strategy_hint` whispers with tactical suggestions. |
| **Redis Stack** | `aura-redis` | `6379` | Whisper Bus (Redis Streams) + VIP Identity Vault (RedisJSON). |
| **Kafka** | `aura-kafka` | `9092` | Durable event backbone. Topics: `swarm.transcripts`, `swarm.reasoning`. |
| **Zookeeper** | `aura-zookeeper` | `2181` | Kafka coordination. |
| **Prometheus** | `aura-prometheus` | `9090` | Metrics scraping from all agent services. |
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

This starts all 8 services. Verify with:

```bash
docker compose ps
```

All containers should report `healthy` or `running` status.

### 2. Seed the VIP Identity Vault

```bash
pip install redis
python scripts/seed_data.py
```

This loads 5 VIP guest profiles into the RedisJSON vault:

| Guest | Phone | Preferences |
|---|---|---|
| Julian | +15551234567 | Red wine, Barolo, window table, no cilantro |
| Isabella | +15559876543 | Champagne, private dining, seafood |
| Marcus | +15555551212 | Whiskey, bar seating, steak |
| Sophia | +15553334444 | White wine, Sancerre, garden patio, vegetarian |
| Alexander | +15557778888 | Red wine, Brunello, corner booth, tasting menu |

### 3. Run the Julian Test

```bash
pip install httpx
python scripts/julian_test.py
```

This end-to-end test simulates a call from Julian (+15551234567) and validates:
- Warm recognition (greeted by name)
- Preference recall (red wine suggestion)
- Response latency < 400ms
- Reasoning trace logged to Kafka
- Unknown callers receive a generic greeting

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
| `guest_context` | Historian | Guest identity, preferences, and historical context |
| `strategy_hint` | Negotiator | Tactical booking suggestions and upsell opportunities |
| `sentiment` | Gateway | Caller sentiment score |
| `menu_suggestion` | Sommelier (Phase 2) | Wine and menu recommendations |

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

### Event Backbone (Kafka)

Two topics power the durable reasoning layer:

- **`swarm.transcripts`** -- Every inbound/outbound utterance with session ID, caller number, and timestamp.
- **`swarm.reasoning`** -- Agent decision traces (what the Historian found, what the Negotiator decided, and processing latency).

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
├── docker-compose.yml              # 8-service swarm definition
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
│   │       └── main.py              # Kafka consumer, VIP lookup, whisper writer
│   └── negotiator/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── app/
│           └── main.py              # Intent analyzer, strategy engine
├── shared/
│   ├── models.py                    # Dataclasses (Whisper, GuestProfile, TranscriptEvent)
│   ├── redis_utils.py               # Redis connection, Whisper Bus, VIP Vault helpers
│   └── kafka_utils.py               # Kafka producer/consumer, topic management
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml           # Scrape config for all agents
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           │   └── datasource.yml   # Prometheus datasource
│           └── dashboards/
│               ├── dashboard.yml    # Dashboard provisioning config
│               └── aura-swarm.json  # Full dashboard definition
└── scripts/
    ├── seed_data.py                 # VIP guest profile seeder
    └── julian_test.py               # End-to-end warm recognition test
```

---

## Configuration

Copy `.env.example` to `.env` and fill in your API keys:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|---|---|---|
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
| Concurrent calls (MVP) | 50 | Docker Compose baseline |

---

## Roadmap

| Phase | Focus | Status |
|---|---|---|
| **Phase 1** | Containerized swarm, Whisper Bus, VIP Vault, Historian, Negotiator, Observability | Current |
| **Phase 2** | Sommelier Agent, live Twilio/ElevenLabs integration, AutoGen v0.4 agent framework | Planned |
| **Phase 3** | Cryptographic ownership, multi-region failover, HITL for edge cases | Deferred |

---

## License

Proprietary. All rights reserved.
