"""
Full-duplex audio bridge: Twilio <-> Deepgram Nova-3 <-> Gemini 3 Flash <-> ElevenLabs

Architecture (three concurrent loops per call session):
  Loop 1 (Twilio In):      Receive mu-law from Twilio -> pipe to Deepgram
  Loop 2 (Deepgram STT):   Monitor transcripts -> trigger Aura Swarm on is_final
  Loop 3 (ElevenLabs TTS): Stream Gemini response -> ElevenLabs -> mu-law -> Twilio
"""
from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import struct
import threading
import time
import wave
from pathlib import Path
from typing import AsyncGenerator, Callable

import websockets

from gateway.app.config import (
    BARGE_IN_THRESHOLD,
    DEEPGRAM_API_KEY,
    ELEVENLABS_API_KEY,
    ELEVENLABS_VOICE_ID,
    ELEVENLABS_MODEL_ID,
    RECORD_SESSIONS,
    RECORDINGS_DIR,
)
from gateway.app.metrics import AUDIO_LATENCY
from shared.gemini_utils import generate_text_stream_iter
from shared.redis_utils import MENU_KEY_PREFIX

logger = logging.getLogger("aura-gateway.audio")

# ---------------------------------------------------------------------------
# Mu-law decode table (ITU-T G.711) -> 16-bit signed PCM
# ---------------------------------------------------------------------------
_MULAW_DECODE: list[int] = [0] * 256
for _i in range(256):
    _inv = ~_i & 0xFF
    _sign = _inv & 0x80
    _exp = (_inv >> 4) & 0x07
    _mantissa = _inv & 0x0F
    _sample = ((_mantissa << 3) + 0x84) << _exp
    _sample -= 0x84
    _MULAW_DECODE[_i] = -_sample if _sign else _sample


def _mulaw_to_pcm(mulaw_bytes: bytes) -> bytes:
    """Convert mu-law audio bytes to 16-bit signed PCM (little-endian)."""
    pcm = bytearray(len(mulaw_bytes) * 2)
    for i, b in enumerate(mulaw_bytes):
        struct.pack_into("<h", pcm, i * 2, _MULAW_DECODE[b])
    return bytes(pcm)


# ---------------------------------------------------------------------------
# Session recorder: buffers audio in memory, writes WAV at session end
# ---------------------------------------------------------------------------
class _SessionRecorder:
    """Buffers inbound (guest) and outbound (Aura) mu-law audio in memory.

    At session end, converts to 16-bit PCM and writes WAV files via
    asyncio.to_thread to avoid blocking the event loop.
    """

    def __init__(self, session_id: str) -> None:
        self._session_id = session_id
        self._inbound = bytearray()   # guest mu-law
        self._outbound = bytearray()  # Aura mu-law
        self._dir = Path(RECORDINGS_DIR) / session_id

    def append_inbound(self, mulaw_chunk: bytes) -> None:
        """Buffer inbound (guest) mu-law audio. Called from async loop."""
        self._inbound.extend(mulaw_chunk)

    def append_outbound(self, mulaw_chunk: bytes) -> None:
        """Buffer outbound (Aura) mu-law audio. Called from async loop."""
        self._outbound.extend(mulaw_chunk)

    def _write_wav(self, pcm_data: bytes, filename: str) -> str:
        """Write PCM data to a WAV file. Returns the file path."""
        self._dir.mkdir(parents=True, exist_ok=True)
        path = self._dir / filename
        with wave.open(str(path), "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)        # 16-bit
            wf.setframerate(8000)
            wf.writeframes(pcm_data)
        return str(path)

    def _flush_sync(self) -> dict[str, str]:
        """Convert buffers to PCM and write WAV files (blocking I/O)."""
        paths: dict[str, str] = {}
        if self._inbound:
            pcm = _mulaw_to_pcm(bytes(self._inbound))
            paths["guest_in"] = self._write_wav(
                pcm, f"guest_in_{self._session_id}.wav",
            )
        if self._outbound:
            pcm = _mulaw_to_pcm(bytes(self._outbound))
            paths["aura_out"] = self._write_wav(
                pcm, f"aura_out_{self._session_id}.wav",
            )
        return paths

    async def flush(self) -> dict[str, str]:
        """Write WAV files in a background thread. Returns path dict."""
        if not self._inbound and not self._outbound:
            return {}
        paths = await asyncio.to_thread(self._flush_sync)
        for label, p in paths.items():
            logger.info("Recorded %s: %s", label, p)
        return paths


def _cleanup_old_recordings(max_age_hours: int = 24) -> int:
    """Delete recording directories older than max_age_hours. Returns count."""
    base = Path(RECORDINGS_DIR)
    if not base.exists():
        return 0
    cutoff = time.time() - max_age_hours * 3600
    removed = 0
    for session_dir in base.iterdir():
        if session_dir.is_dir() and session_dir.stat().st_mtime < cutoff:
            for f in session_dir.iterdir():
                f.unlink(missing_ok=True)
            session_dir.rmdir()
            removed += 1
    if removed:
        logger.info("Cleaned up %d old recording(s)", removed)
    return removed


# ---------------------------------------------------------------------------
# External service endpoints
# ---------------------------------------------------------------------------
_DG_WS_URL = "wss://api.deepgram.com/v1/listen"
_EL_WS_URL = "wss://api.elevenlabs.io/v1/text-to-speech"


def _build_deepgram_url(keywords: list[str] | None = None) -> str:
    """Build Deepgram WebSocket URL with Nova-3 config and optional keywords."""
    params = [
        "model=nova-3",
        "encoding=mulaw",
        "sample_rate=8000",
        "channels=1",
        "interim_results=true",
        "utterance_end_ms=1000",
        "vad_events=true",
        "punctuate=true",
        "smart_format=true",
    ]
    if keywords:
        for kw in keywords[:100]:
            safe = kw.replace(" ", "%20")
            params.append(f"keywords={safe}:2.0")
    return f"{_DG_WS_URL}?{'&'.join(params)}"


def _build_elevenlabs_url() -> str:
    """Build ElevenLabs streaming TTS WebSocket URL."""
    return (
        f"{_EL_WS_URL}/{ELEVENLABS_VOICE_ID}"
        f"/stream-input?model_id={ELEVENLABS_MODEL_ID}"
        f"&output_format=ulaw_8000"
    )


# ---------------------------------------------------------------------------
# Menu keyword extraction (Task B: Nova-3 Keyterm Prompting)
# ---------------------------------------------------------------------------
def _fetch_menu_keywords_sync(redis_client) -> list[str]:
    """Synchronous Redis scan for menu keywords (runs in thread)."""
    keywords: list[str] = []
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(
            cursor, match=f"{MENU_KEY_PREFIX}*", count=100,
        )
        for key in keys:
            item = redis_client.json().get(key, "$")
            if item and len(item) > 0:
                name = item[0].get("name", "")
                if name:
                    keywords.append(name)
                for ing in item[0].get("ingredients", []):
                    if len(ing) > 3:
                        keywords.append(ing)
        if cursor == 0:
            break
    seen: set[str] = set()
    unique: list[str] = []
    for kw in keywords:
        low = kw.lower()
        if low not in seen:
            seen.add(low)
            unique.append(kw)
    return unique[:100]


async def fetch_menu_keywords(redis_client) -> list[str]:
    """Query Redis for current menu item names to inject as Deepgram keywords.

    Runs the blocking Redis scan in a thread to avoid stalling the event loop
    (prevents audio jitter in the Twilio stream).
    """
    if not redis_client:
        return []
    try:
        unique = await asyncio.to_thread(_fetch_menu_keywords_sync, redis_client)
        logger.info("Loaded %d menu keywords for Deepgram Nova-3", len(unique))
        return unique
    except Exception as exc:
        logger.warning("Failed to load menu keywords: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Async wrapper around sync Gemini streaming
# ---------------------------------------------------------------------------
async def _gemini_chunks(
    prompt: str, system_instruction: str,
) -> AsyncGenerator[str, None]:
    """Yield text chunks from Gemini in an async-friendly way."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[str | None] = asyncio.Queue()

    def _produce() -> None:
        try:
            for chunk in generate_text_stream_iter(
                prompt, system_instruction=system_instruction,
            ):
                loop.call_soon_threadsafe(queue.put_nowait, chunk)
        except Exception as exc:
            logger.warning("Gemini streaming error: %s", exc)
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, None)

    thread = threading.Thread(target=_produce, daemon=True)
    thread.start()

    while True:
        chunk = await queue.get()
        if chunk is None:
            break
        yield chunk


# ---------------------------------------------------------------------------
# AudioBridge: manages one call session end-to-end
# ---------------------------------------------------------------------------
class AudioBridge:
    """Full-duplex audio pipeline for a single Twilio call.

    Twilio mu-law -> Deepgram Nova-3 (STT) -> Aura Swarm -> Gemini 3 Flash
                                                          -> ElevenLabs (TTS) -> Twilio mu-law
    """

    def __init__(
        self,
        twilio_ws,
        session_id: str,
        caller: str,
        profile: dict | None,
        greeting: str,
        redis_client,
        kafka_producer,
        *,
        publish_transcript_fn: Callable,
        collect_whispers_fn: Callable,
        build_prompt_fn: Callable,
        system_instruction: str,
    ):
        self._twilio_ws = twilio_ws
        self._session_id = session_id
        self._caller = caller
        self._profile = profile
        self._greeting = greeting
        self._redis = redis_client
        self._kafka = kafka_producer
        self._publish_transcript = publish_transcript_fn
        self._collect_whispers = collect_whispers_fn
        self._build_prompt = build_prompt_fn
        self._system_instruction = system_instruction

        self._stream_sid: str | None = None
        self._speaking = False
        self._cancel_tts = asyncio.Event()
        self._dg_ws = None
        self._turn_count = 0
        self._full_response_text = ""

        # Latency tracking
        self._end_of_speech_ts: float = 0.0
        self._first_audio_ts: float = 0.0

        # Binary persistence layer (WAV recorder)
        self._recorder: _SessionRecorder | None = None
        if RECORD_SESSIONS:
            self._recorder = _SessionRecorder(session_id)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    async def run(self) -> dict:
        """Run the three-loop audio bridge. Returns session stats."""
        keywords = await fetch_menu_keywords(self._redis)
        dg_url = _build_deepgram_url(keywords)
        dg_headers = {"Authorization": f"Token {DEEPGRAM_API_KEY}"}

        try:
            async with websockets.connect(
                dg_url, additional_headers=dg_headers, ping_interval=20,
            ) as dg_ws:
                self._dg_ws = dg_ws
                logger.info(
                    "Deepgram Nova-3 connected for session=%s (%d keywords)",
                    self._session_id, len(keywords),
                )

                # Synthesize and send the greeting as first audio output
                await self._synthesize_and_send(self._greeting)

                # Run Twilio receiver and Deepgram listener concurrently
                await asyncio.gather(
                    self._loop_twilio_in(),
                    self._loop_deepgram_listen(),
                    return_exceptions=True,
                )
        except websockets.exceptions.ConnectionClosed:
            logger.info("Deepgram connection closed for session %s", self._session_id)
        except Exception as exc:
            logger.error("AudioBridge error for session %s: %s", self._session_id, exc)

        # Flush recorded audio to WAV files (non-blocking)
        recording_paths: dict[str, str] = {}
        if self._recorder:
            recording_paths = await self._recorder.flush()

        # Opportunistic cleanup of old recordings (>24h)
        try:
            await asyncio.to_thread(_cleanup_old_recordings)
        except Exception:
            pass

        return {
            "session_id": self._session_id,
            "turns": self._turn_count,
            "recordings": recording_paths,
        }

    # ------------------------------------------------------------------
    # Loop 1: Twilio -> Deepgram
    # ------------------------------------------------------------------
    async def _loop_twilio_in(self) -> None:
        """Receive Twilio media events and pipe mu-law audio to Deepgram."""
        try:
            while True:
                raw = await self._twilio_ws.receive_text()
                msg = json.loads(raw)
                event = msg.get("event")

                if event == "start":
                    self._stream_sid = msg["start"]["streamSid"]
                    logger.info(
                        "Twilio stream started: streamSid=%s session=%s",
                        self._stream_sid, self._session_id,
                    )
                elif event == "media":
                    audio_bytes = base64.b64decode(msg["media"]["payload"])
                    if self._recorder:
                        self._recorder.append_inbound(audio_bytes)
                    if self._dg_ws:
                        await self._dg_ws.send(audio_bytes)
                elif event == "stop":
                    logger.info(
                        "Twilio stream stopped for session %s", self._session_id,
                    )
                    if self._dg_ws:
                        await self._dg_ws.send(json.dumps({"type": "CloseStream"}))
                    break
        except Exception as exc:
            logger.info(
                "Twilio receiver ended for session %s: %s", self._session_id, exc,
            )

    # ------------------------------------------------------------------
    # Loop 2: Deepgram -> Transcript processing
    # ------------------------------------------------------------------
    async def _loop_deepgram_listen(self) -> None:
        """Monitor Deepgram transcripts; handle barge-in and trigger swarm."""
        try:
            async for raw in self._dg_ws:
                msg = json.loads(raw)
                msg_type = msg.get("type", "")

                if msg_type == "Results":
                    alt = msg.get("channel", {}).get("alternatives", [{}])[0]
                    transcript = alt.get("transcript", "")
                    is_final = msg.get("is_final", False)

                    # Barge-in: guest speaks while AI is talking.
                    # Require minimum character threshold to avoid false
                    # positives from background noise ("um", dishes, etc.).
                    stripped = transcript.strip()
                    if (
                        self._speaking
                        and len(stripped) >= BARGE_IN_THRESHOLD
                    ):
                        logger.info(
                            "Barge-in detected in session %s (%d chars), "
                            "cancelling TTS",
                            self._session_id, len(stripped),
                        )
                        self._cancel_tts.set()
                        await self._send_twilio_clear()

                    if is_final and transcript.strip():
                        self._end_of_speech_ts = time.time()
                        logger.info(
                            "Final transcript [session=%s]: %s",
                            self._session_id, transcript[:120],
                        )
                        await self._process_turn(transcript)

                elif msg_type == "UtteranceEnd":
                    logger.debug(
                        "Utterance end for session %s", self._session_id,
                    )
        except websockets.exceptions.ConnectionClosed:
            logger.info(
                "Deepgram listener closed for session %s", self._session_id,
            )

    # ------------------------------------------------------------------
    # Turn processing: Swarm -> Gemini -> ElevenLabs -> Twilio
    # ------------------------------------------------------------------
    async def _process_turn(self, transcript: str) -> None:
        """Handle a complete user utterance end-to-end."""
        self._turn_count += 1
        self._first_audio_ts = 0.0

        # Publish inbound transcript to Kafka (triggers swarm agents)
        self._publish_transcript(
            self._session_id, self._caller, transcript, "inbound",
        )

        # Collect whispers from the swarm
        whispers = await self._collect_whispers(self._session_id)

        # Build prompt and stream response through TTS
        prompt = self._build_prompt(
            self._greeting, transcript, whispers, self._profile,
        )
        await self._synthesize_and_send_streaming(prompt, transcript)

    # ------------------------------------------------------------------
    # ElevenLabs TTS: complete text (used for greeting)
    # ------------------------------------------------------------------
    async def _synthesize_and_send(self, text: str) -> None:
        """Synthesize a complete text string and send audio to Twilio."""
        if not ELEVENLABS_API_KEY or not ELEVENLABS_VOICE_ID:
            logger.warning("ElevenLabs not configured, skipping TTS")
            return

        self._speaking = True
        self._cancel_tts.clear()
        el_url = _build_elevenlabs_url()

        try:
            async with websockets.connect(el_url, ping_interval=20) as el_ws:
                # Init message with voice settings and auth
                await el_ws.send(json.dumps({
                    "text": " ",
                    "voice_settings": {
                        "stability": 0.5,
                        "similarity_boost": 0.75,
                    },
                    "xi_api_key": ELEVENLABS_API_KEY,
                }))

                # Send the full text
                await el_ws.send(json.dumps({"text": text}))

                # Send EOS to flush
                await el_ws.send(json.dumps({"text": ""}))

                # Receive and forward audio chunks
                async for raw in el_ws:
                    if self._cancel_tts.is_set():
                        break
                    msg = json.loads(raw)
                    audio_b64 = msg.get("audio")
                    if audio_b64:
                        await self._send_twilio_audio(audio_b64)
                    if msg.get("isFinal"):
                        break
        except Exception as exc:
            logger.warning("ElevenLabs greeting synthesis failed: %s", exc)
        finally:
            self._speaking = False

    # ------------------------------------------------------------------
    # ElevenLabs TTS: streaming from Gemini (used for responses)
    # ------------------------------------------------------------------
    async def _synthesize_and_send_streaming(
        self, prompt: str, user_text: str,
    ) -> None:
        """Stream Gemini response through ElevenLabs to Twilio."""
        if not ELEVENLABS_API_KEY or not ELEVENLABS_VOICE_ID:
            logger.warning("ElevenLabs not configured, skipping TTS")
            return

        self._speaking = True
        self._cancel_tts.clear()
        el_url = _build_elevenlabs_url()
        response_parts: list[str] = []

        try:
            async with websockets.connect(el_url, ping_interval=20) as el_ws:
                # Init message
                await el_ws.send(json.dumps({
                    "text": " ",
                    "voice_settings": {
                        "stability": 0.5,
                        "similarity_boost": 0.75,
                    },
                    "xi_api_key": ELEVENLABS_API_KEY,
                }))

                # Run text producer and audio consumer concurrently
                await asyncio.gather(
                    self._pipe_gemini_to_elevenlabs(el_ws, prompt, response_parts),
                    self._pipe_elevenlabs_to_twilio(el_ws),
                )
        except Exception as exc:
            logger.warning("Streaming synthesis failed: %s", exc)
        finally:
            self._speaking = False
            self._full_response_text = "".join(response_parts)

            # Publish outbound transcript
            if self._full_response_text:
                self._publish_transcript(
                    self._session_id, self._caller,
                    self._full_response_text, "outbound",
                )

            # Record audio-to-audio latency
            if self._end_of_speech_ts and self._first_audio_ts:
                latency_s = self._first_audio_ts - self._end_of_speech_ts
                AUDIO_LATENCY.observe(latency_s)
                logger.info(
                    "Audio-to-Audio latency: %.1fms [session=%s turn=%d]",
                    latency_s * 1000, self._session_id, self._turn_count,
                )

    async def _pipe_gemini_to_elevenlabs(
        self, el_ws, prompt: str, response_parts: list[str],
    ) -> None:
        """Stream Gemini text chunks into ElevenLabs WebSocket."""
        async for chunk in _gemini_chunks(prompt, self._system_instruction):
            if self._cancel_tts.is_set():
                break
            response_parts.append(chunk)
            await el_ws.send(json.dumps({"text": chunk}))

        # Send empty string EOS to flush the ElevenLabs buffer
        if not self._cancel_tts.is_set():
            await el_ws.send(json.dumps({"text": ""}))

    async def _pipe_elevenlabs_to_twilio(self, el_ws) -> None:
        """Receive ElevenLabs audio chunks and send to Twilio."""
        try:
            async for raw in el_ws:
                if self._cancel_tts.is_set():
                    break
                msg = json.loads(raw)
                audio_b64 = msg.get("audio")
                if audio_b64:
                    if not self._first_audio_ts:
                        self._first_audio_ts = time.time()
                    if self._recorder:
                        self._recorder.append_outbound(
                            base64.b64decode(audio_b64),
                        )
                    await self._send_twilio_audio(audio_b64)
                if msg.get("isFinal"):
                    break
        except websockets.exceptions.ConnectionClosed:
            pass

    # ------------------------------------------------------------------
    # Twilio helpers
    # ------------------------------------------------------------------
    async def _send_twilio_audio(self, audio_b64: str) -> None:
        """Send a media payload back to Twilio."""
        if not self._stream_sid:
            return
        await self._twilio_ws.send_text(json.dumps({
            "event": "media",
            "streamSid": self._stream_sid,
            "media": {"payload": audio_b64},
        }))

    async def _send_twilio_clear(self) -> None:
        """Send a clear event to Twilio to stop playback (barge-in)."""
        if not self._stream_sid:
            return
        await self._twilio_ws.send_text(json.dumps({
            "event": "clear",
            "streamSid": self._stream_sid,
        }))
        logger.info("Sent clear event to Twilio for session %s", self._session_id)
