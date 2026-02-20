from prometheus_client import Counter, Histogram, Gauge

CALL_COUNT = Counter(
    "aura_calls_total",
    "Total number of voice calls handled",
)

ACTIVE_CALLS = Gauge(
    "aura_active_calls",
    "Number of currently active voice calls",
)

RESPONSE_LATENCY = Histogram(
    "aura_response_latency_seconds",
    "End-to-end response latency from silence detection to audio output",
    buckets=[0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0],
)

WHISPER_COUNT = Counter(
    "aura_whispers_total",
    "Total whispers published to the bus",
    ["agent", "whisper_type"],
)

TOKEN_USAGE = Counter(
    "aura_token_usage_total",
    "Total LLM tokens consumed",
    ["agent", "token_type"],
)

SENTIMENT_SCORE = Gauge(
    "aura_sentiment_score",
    "Current caller sentiment score",
)

KAFKA_EVENTS = Counter(
    "aura_kafka_events_total",
    "Total events published to Kafka",
    ["topic"],
)

AUDIO_LATENCY = Histogram(
    "aura_audio_to_audio_latency_seconds",
    "End-of-speech to first AI audio packet latency",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0, 1.5, 2.0],
)
