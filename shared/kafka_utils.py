from __future__ import annotations

import json
import os

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

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
