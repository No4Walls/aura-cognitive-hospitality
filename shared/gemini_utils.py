from __future__ import annotations

import logging
import os
import threading

from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

_DEFAULT_EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "text-embedding-004")
_DEFAULT_GENERATIVE_MODEL = os.getenv("GEMINI_GENERATIVE_MODEL", "gemini-3-flash")


def _load_api_keys() -> list[str]:
    keys_raw = (
        os.getenv("GOOGLE_API_KEYS")
        or os.getenv("GOOGLE_API_KEY")
        or os.getenv("GEMINI_API_KEYS")
        or os.getenv("GEMINI_API_KEY")
    )
    if not keys_raw:
        return []
    keys = [k.strip() for k in keys_raw.split(",")]
    return [k for k in keys if k]


_api_keys = _load_api_keys()
_key_lock = threading.Lock()
_key_idx = 0
_clients: dict[str, genai.Client] = {}


def _next_api_key() -> str:
    global _key_idx
    if not _api_keys:
        raise RuntimeError("No GOOGLE_API_KEY configured")
    with _key_lock:
        key = _api_keys[_key_idx % len(_api_keys)]
        _key_idx += 1
        return key


def get_client() -> genai.Client:
    key = _next_api_key()
    with _key_lock:
        client = _clients.get(key)
        if client is None:
            client = genai.Client(api_key=key)
            _clients[key] = client
        return client


def default_safety_settings() -> list[types.SafetySetting]:
    # Intentionally permissive to avoid blocking hospitality topics (wine/spirits).
    # Safety is primarily enforced via system_instruction guardrails.
    return [
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_CIVIC_INTEGRITY,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_JAILBREAK,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
    ]


def warmup() -> None:
    if not _api_keys:
        logger.warning("No GOOGLE_API_KEY set; skipping Gemini warm-up")
        return
    # Warm up embedding model
    try:
        client = get_client()
        client.models.embed_content(
            model=_DEFAULT_EMBEDDING_MODEL, contents="warmup",
        )
        logger.info("Gemini warm-up: embedding model ready")
    except Exception as exc:
        logger.warning("Gemini embedding warm-up failed: %s", exc)
    # Warm up generative model (pre-warms HTTP/TLS connection pool)
    try:
        client = get_client()
        config = types.GenerateContentConfig(
            system_instruction="You are a test assistant.",
            temperature=0.0,
            max_output_tokens=5,
            safety_settings=default_safety_settings(),
        )
        client.models.generate_content(
            model=_DEFAULT_GENERATIVE_MODEL,
            contents="Say hello.",
            config=config,
        )
        logger.info("Gemini warm-up: generative model ready")
    except Exception as exc:
        logger.warning("Gemini generative warm-up failed: %s", exc)


def embed_text(text: str, model: str | None = None) -> list[float]:
    client = get_client()
    resp = client.models.embed_content(model=model or _DEFAULT_EMBEDDING_MODEL, contents=text)
    if not resp.embeddings:
        raise RuntimeError("Gemini embed_content returned no embeddings")
    return list(resp.embeddings[0].values)


def generate_text(
    prompt: str,
    *,
    system_instruction: str,
    model: str | None = None,
    temperature: float = 0.4,
    max_output_tokens: int = 300,
) -> str:
    client = get_client()
    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        safety_settings=default_safety_settings(),
    )
    resp = client.models.generate_content(
        model=model or _DEFAULT_GENERATIVE_MODEL,
        contents=prompt,
        config=config,
    )
    text = getattr(resp, "text", None)
    if text:
        return text.strip()
    raise RuntimeError("Gemini generate_content returned no text")


def generate_text_stream(
    prompt: str,
    *,
    system_instruction: str,
    model: str | None = None,
    temperature: float = 0.4,
    max_output_tokens: int = 300,
) -> str:
    """Stream tokens from Gemini and return the assembled response.

    Uses ``generate_content_stream`` so that the first tokens begin arriving
    before the full response is ready, reducing perceived latency (TTFB).
    The final assembled string is returned for callers that need the complete
    text.  Future callers (e.g. live audio) can switch to iterating over the
    generator directly.
    """
    client = get_client()
    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        safety_settings=default_safety_settings(),
    )
    chunks: list[str] = []
    for chunk in client.models.generate_content_stream(
        model=model or _DEFAULT_GENERATIVE_MODEL,
        contents=prompt,
        config=config,
    ):
        part_text = getattr(chunk, "text", None)
        if part_text:
            chunks.append(part_text)
    result = "".join(chunks).strip()
    if result:
        return result
    raise RuntimeError("Gemini streaming returned no text")
