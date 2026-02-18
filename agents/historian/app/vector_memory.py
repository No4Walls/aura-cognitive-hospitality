from __future__ import annotations

import hashlib
import json
import logging
import time

import numpy as np
import redis
from redis.commands.search.field import TagField, TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

logger = logging.getLogger("historian.vector_memory")

VECTOR_DIM = 128
MEMORY_KEY_PREFIX = "aura:memory:"
MEMORY_INDEX_NAME = "idx:aura_memory"
DISTANCE_METRIC = "COSINE"


def _text_to_embedding(text: str) -> list[float]:
    text_lower = text.lower().strip()
    h = hashlib.sha256(text_lower.encode()).digest()
    raw = np.frombuffer(h * (VECTOR_DIM // 32 + 1), dtype=np.uint8)[:VECTOR_DIM]
    embedding = (raw.astype(np.float32) / 255.0) * 2 - 1

    keyword_boosts: dict[str, int] = {
        "anniversary": 0, "birthday": 1, "celebration": 2,
        "wine": 3, "red": 4, "white": 5,
        "table": 6, "reservation": 7, "book": 8,
        "allergy": 9, "vegetarian": 10, "vegan": 11,
        "complaint": 12, "happy": 13, "love": 14,
        "special": 15, "usual": 16, "regular": 17,
    }

    for keyword, idx in keyword_boosts.items():
        if keyword in text_lower:
            target = idx % VECTOR_DIM
            embedding[target] = min(embedding[target] + 0.5, 1.0)

    norm = np.linalg.norm(embedding)
    if norm > 0:
        embedding = embedding / norm

    return embedding.tolist()


def ensure_vector_index(r: redis.Redis) -> None:
    try:
        r.ft(MEMORY_INDEX_NAME).info()
        logger.info("Vector index '%s' already exists", MEMORY_INDEX_NAME)
        return
    except Exception:
        pass

    schema = (
        TagField("$.tenant_id", as_name="tenant_id"),
        TagField("$.caller_id", as_name="caller_id"),
        TagField("$.session_id", as_name="session_id"),
        TextField("$.text", as_name="text"),
        TagField("$.direction", as_name="direction"),
        VectorField(
            "$.embedding",
            "FLAT",
            {
                "TYPE": "FLOAT32",
                "DIM": VECTOR_DIM,
                "DISTANCE_METRIC": DISTANCE_METRIC,
            },
            as_name="embedding",
        ),
    )
    definition = IndexDefinition(
        prefix=[MEMORY_KEY_PREFIX],
        index_type=IndexType.JSON,
    )
    r.ft(MEMORY_INDEX_NAME).create_index(schema, definition=definition)
    logger.info("Created vector index '%s'", MEMORY_INDEX_NAME)


def store_transcript_embedding(
    r: redis.Redis,
    tenant_id: str,
    caller_id: str,
    session_id: str,
    text: str,
    direction: str,
) -> str:
    embedding = _text_to_embedding(text)
    ts = time.time()
    doc_id = f"{MEMORY_KEY_PREFIX}{tenant_id}:{caller_id}:{int(ts * 1000)}"

    doc = {
        "tenant_id": tenant_id,
        "caller_id": caller_id,
        "session_id": session_id,
        "text": text[:500],
        "direction": direction,
        "embedding": embedding,
        "timestamp": ts,
    }
    r.json().set(doc_id, "$", doc)
    logger.debug("Stored embedding for %s (dim=%d)", caller_id, len(embedding))
    return doc_id


def knn_search(
    r: redis.Redis,
    query_text: str,
    tenant_id: str,
    caller_id: str | None = None,
    k: int = 5,
) -> list[dict]:
    embedding = _text_to_embedding(query_text)
    embedding_bytes = np.array(embedding, dtype=np.float32).tobytes()

    filter_parts = [f"@tenant_id:{{{tenant_id}}}"]
    if caller_id:
        filter_parts.append(f"@caller_id:{{{caller_id}}}")
    filter_str = " ".join(filter_parts)

    q = (
        Query(f"({filter_str})=>[KNN {k} @embedding $vec AS score]")
        .sort_by("score")
        .return_fields("text", "caller_id", "session_id", "score", "timestamp")
        .dialect(2)
    )

    results = r.ft(MEMORY_INDEX_NAME).search(q, query_params={"vec": embedding_bytes})

    memories = []
    for doc in results.docs:
        memories.append({
            "text": doc.text if hasattr(doc, "text") else "",
            "caller_id": doc.caller_id if hasattr(doc, "caller_id") else "",
            "session_id": doc.session_id if hasattr(doc, "session_id") else "",
            "score": float(doc.score) if hasattr(doc, "score") else 1.0,
            "timestamp": float(doc.timestamp) if hasattr(doc, "timestamp") else 0,
        })

    return memories


def recall_deep_context(
    r: redis.Redis,
    tenant_id: str,
    caller_id: str,
    current_text: str,
    k: int = 5,
) -> list[dict]:
    try:
        return knn_search(r, current_text, tenant_id, caller_id, k)
    except Exception as exc:
        logger.warning("KNN search failed (index may be empty): %s", exc)
        return []
