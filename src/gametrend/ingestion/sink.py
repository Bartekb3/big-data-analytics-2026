"""Where events go: Kafka (normal run) or stdout (`--dry-run`).

Kafka rules (architecture/ingestion/README.md, Design / rules): key = game id as a string of digits,
`partitioner=murmur2_random` (the Java partitioner, so one game stays on one partition), record timestamp left
unset. A failed send shows only in the delivery callback, so success and failure are reported from there.
"""

import json
import logging
import sys
import threading
from typing import Callable, Protocol

log = logging.getLogger(__name__)

Delivered = Callable[[bool], None]  # called once per event: True when delivered


def encode(event: dict) -> bytes:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class Sink(Protocol):
    def send(self, topic: str, event: dict, delivered: Delivered) -> None: ...

    def poll(self) -> None: ...

    def flush(self, timeout: float) -> int: ...


class KafkaSink:
    def __init__(self, bootstrap: str):
        from confluent_kafka import Producer  # imported here so unit tests do not need the client library

        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "partitioner": "murmur2_random",
            "linger.ms": 50,
            "enable.idempotence": True,
            "client.id": "gametrend-producers",
        })

    def send(self, topic: str, event: dict, delivered: Delivered) -> None:
        def on_delivery(err, _msg):
            if err is not None:
                log.warning("not delivered to %s: %s", topic, err)
            delivered(err is None)

        for attempt in range(2):
            try:
                self._producer.produce(topic, key=str(event["id"]).encode(), value=encode(event),
                                       on_delivery=on_delivery)
                break
            except BufferError:  # local queue full: let it drain once, then give up on this record
                if attempt:
                    log.warning("local Kafka queue full, record to %s dropped", topic)
                    delivered(False)
                    return
                self._producer.poll(1.0)
        self._producer.poll(0)

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout: float) -> int:
        return self._producer.flush(timeout)


class PrintSink:
    """`--dry-run`: one JSON line per event on stdout (`<topic>\\t<event>`), nothing sent."""

    def __init__(self, out=None):
        if out is None:
            out = sys.stdout
            if hasattr(out, "reconfigure"):
                out.reconfigure(encoding="utf-8")  # the Windows console default (cp1250) cannot write emoji
        self._out = out
        self._lock = threading.Lock()

    def send(self, topic: str, event: dict, delivered: Delivered) -> None:
        with self._lock:
            self._out.write(f"{topic}\t{encode(event).decode('utf-8')}\n")
        delivered(True)

    def poll(self) -> None:
        pass

    def flush(self, timeout: float) -> int:
        self._out.flush()
        return 0
