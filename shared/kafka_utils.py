from __future__ import annotations

import json
import logging
import os
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSCRIPT_TOPIC = "swarm.transcripts"
REASONING_TOPIC = "swarm.reasoning"


def get_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def get_consumer(group_id: str, topics: list[str] | None = None) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    if topics:
        consumer.subscribe(topics)
    return consumer


def produce_event(producer: Producer, topic: str, key: str, value: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
    )
    producer.flush()


def connect_kafka_consumer(
    group_id: str,
    topics: list[str] | None = None,
    retries: int = 30,
    delay: float = 2.0,
) -> Consumer:
    if topics is None:
        topics = [TRANSCRIPT_TOPIC]
    for attempt in range(retries):
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "group.id": group_id,
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": True,
                }
            )
            consumer.subscribe(topics)
            logger.info("Subscribed to Kafka topics: %s", topics)
            return consumer
        except Exception:
            logger.warning("Kafka not ready, retrying... (attempt %d/%d)", attempt + 1, retries)
            time.sleep(delay)
    raise ConnectionError("Failed to connect to Kafka")


async def async_connect_kafka_producer(
    retries: int = 30,
    delay: float = 2.0,
    flush_timeout: float = 5.0,
) -> Producer:
    import asyncio

    for attempt in range(retries):
        try:
            producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
            producer.flush(timeout=flush_timeout)
            logger.info("Connected to Kafka")
            return producer
        except Exception:
            logger.warning("Kafka not ready, retrying... (attempt %d/%d)", attempt + 1, retries)
            await asyncio.sleep(delay)
    raise ConnectionError("Failed to connect to Kafka")


def publish_reasoning(
    producer: Producer, agent: str, session_id: str, reasoning: dict
) -> None:
    event = {
        "agent": agent,
        "session_id": session_id,
        "reasoning": reasoning,
        "timestamp": time.time(),
    }
    producer.produce(
        REASONING_TOPIC,
        key=session_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
    )
    producer.flush()


def ensure_topics() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    new_topics = [
        NewTopic(TRANSCRIPT_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(REASONING_TOPIC, num_partitions=3, replication_factor=1),
    ]
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
        except Exception:
            pass
