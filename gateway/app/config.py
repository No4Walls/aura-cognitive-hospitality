from __future__ import annotations

import os


TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "")

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "")
ELEVENLABS_MODEL_ID = os.getenv("ELEVENLABS_MODEL_ID", "eleven_flash_v2_5")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_GENERATIVE_MODEL = os.getenv("GEMINI_GENERATIVE_MODEL", "gemini-3-flash")
GEMINI_EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "text-embedding-004")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

DOMAIN = os.getenv("DOMAIN", "localhost:8000")

WHISPER_POLL_TIMEOUT_MS = 50
MAX_RESPONSE_LATENCY_MS = 500
VOCAL_DELAY_TARGET_MS = 800

# Minimum characters in an interim transcript before triggering barge-in.
# Prevents background noise ("um", "[noise]") from cancelling Aura's speech.
BARGE_IN_THRESHOLD = int(os.getenv("BARGE_IN_THRESHOLD", "3"))

# Binary persistence layer: record inbound (guest) and outbound (Aura) audio
# to WAV files for debugging transcription and barge-in issues.
RECORD_SESSIONS = os.getenv("RECORD_SESSIONS", "true").lower() in ("true", "1", "yes")
RECORDINGS_DIR = os.getenv("RECORDINGS_DIR", "/app/recordings")
