from __future__ import annotations

import logging
import math
import struct
import time

import redis
from redis.commands.search.field import TagField, TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.gemini_utils import embed_text

logger = logging.getLogger("historian.vector_memory")

VECTOR_DIM = 768
MEMORY_KEY_PREFIX = "aura:memory:"
MEMORY_INDEX_NAME = "idx:history"
LEGACY_INDEX_NAMES = ["idx:aura_memory"]
DISTANCE_METRIC = "COSINE"


def _text_to_embedding(text: str) -> list[float]:
    raw = embed_text(text)
    norm = math.sqrt(sum(v * v for v in raw))
    if norm > 0:
        return [v / norm for v in raw]
    return raw


def ensure_vector_index(r: redis.Redis) -> None:
    for legacy in LEGACY_INDEX_NAMES:
        if legacy == MEMORY_INDEX_NAME:
            continue
        try:
            r.ft(legacy).dropindex(delete_documents=True)
        except Exception:
            pass

    try:
        info = r.ft(MEMORY_INDEX_NAME).info()
        attrs = info.get("attributes", [])
        for attr in attrs:
            if isinstance(attr, list) and b"DIM" in attr:
                dim_idx = attr.index(b"DIM") + 1
                existing_dim = int(attr[dim_idx])
                if existing_dim != VECTOR_DIM:
                    logger.warning(
                        "Index dimension mismatch (%d != %d), dropping and recreating",
                        existing_dim, VECTOR_DIM,
                    )
                    r.ft(MEMORY_INDEX_NAME).dropindex(delete_documents=True)
                    raise Exception("re-create needed")
        logger.info("Vector index '%s' already exists (dim=%d)", MEMORY_INDEX_NAME, VECTOR_DIM)
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
    embedding_bytes = struct.pack(f"{len(embedding)}f", *embedding)

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
