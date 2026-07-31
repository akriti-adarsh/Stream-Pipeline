"""Replay a DLQ topic back to its source topic after a fix.

The cycle this closes: a poison message lands in ``<topic>.dlq`` wrapped in an
envelope carrying its exact raw bytes; the operator fixes the cause (deploys a
tolerant consumer, or the bytes themselves are repairable); this tool reads the
envelopes, optionally filters and repairs, and re-produces the original bytes
to the source topic, where they flow through the normal path.

Usage:
    uv run python scripts/replay_dlq.py --topic rides.events --dry-run
    uv run python scripts/replay_dlq.py --topic rides.events --repair-magic-byte
    uv run python scripts/replay_dlq.py --topic rides.events \
        --filter "e.error.startswith('SerializationError')" \
        --transform mypkg.fixes:mend

``--filter`` is a Python expression over the envelope ``e`` (operator-supplied,
runs locally with your own privileges, exactly like a shell command).
``--repair-magic-byte`` restores the Confluent framing byte the generator's
corruption flips, which is the repair for this platform's canonical poison.
``--transform module:function`` imports a callable ``bytes -> bytes`` for
anything richer. Replayed messages get fresh offsets; the DLQ topic itself is
never mutated, so replay is safe to re-run (downstream idempotency dedupes).
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, Producer

from common.kafka import consumer_config, producer_config
from common.topics import dlq_topic
from dlq.envelope import Envelope, parse_envelope

Transform = Callable[[bytes], bytes]
Predicate = Callable[[Envelope], bool]


def repair_magic_byte(payload: bytes) -> bytes:
    """Restore the Confluent wire-format magic byte (0x00) in position 0."""
    return b"\x00" + payload[1:] if payload else payload


@dataclass
class ReplayStats:
    seen: int = 0
    filtered_out: int = 0
    replayed: int = 0
    dry_run: bool = False

    def as_json(self) -> str:
        return json.dumps(
            {
                "seen": self.seen,
                "filtered_out": self.filtered_out,
                "replayed": self.replayed,
                "dry_run": self.dry_run,
            }
        )


def replay(
    envelopes: list[Envelope],
    produce: Callable[[str, bytes | None, bytes], None],
    *,
    predicate: Predicate | None = None,
    transform: Transform | None = None,
    dry_run: bool = False,
) -> ReplayStats:
    """Pure replay core: filter, repair, re-produce. Fully unit tested."""
    stats = ReplayStats(dry_run=dry_run)
    for envelope in envelopes:
        stats.seen += 1
        if predicate is not None and not predicate(envelope):
            stats.filtered_out += 1
            continue
        payload = envelope.payload
        if transform is not None:
            payload = transform(payload)
        if not dry_run:
            produce(envelope.source_topic, envelope.key, payload)
        stats.replayed += 1
    return stats


def drain_dlq(consumer: Any, topic: str, idle_polls: int = 5) -> list[Envelope]:
    """Read every envelope currently on the DLQ topic, then stop."""
    consumer.subscribe([dlq_topic(topic)])
    envelopes: list[Envelope] = []
    quiet = 0
    while quiet < idle_polls:
        msgs = consumer.consume(500, 1.0)
        if not msgs:
            quiet += 1
            continue
        quiet = 0
        for msg in msgs:
            if msg.error() is not None:
                continue
            envelopes.append(parse_envelope(msg.value()))
    return envelopes


def load_transform(spec: str) -> Transform:
    module_name, _, func_name = spec.partition(":")
    if not func_name:
        raise SystemExit(f"--transform must be module:function, got {spec!r}")
    module = importlib.import_module(module_name)
    transform = getattr(module, func_name)
    if not callable(transform):
        raise SystemExit(f"{spec} is not callable")
    return transform  # type: ignore[no-any-return]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="replay_dlq", description=__doc__)
    parser.add_argument("--topic", required=True, help="source topic whose .dlq to replay")
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--filter", default=None, help="Python expression over envelope `e`")
    parser.add_argument("--transform", default=None, help="module:function bytes -> bytes")
    parser.add_argument(
        "--repair-magic-byte",
        action="store_true",
        help="restore the Confluent framing byte flipped by the canonical corruption",
    )
    args = parser.parse_args(argv)

    predicate: Predicate | None = None
    if args.filter is not None:
        # Operator-supplied expression, evaluated locally with the operator's
        # own privileges; identical trust model to running any shell command.
        code = compile(args.filter, "<filter>", "eval")

        def predicate(e: Envelope) -> bool:
            return bool(eval(code, {"__builtins__": {}}, {"e": e}))

    transform: Transform | None = None
    if args.repair_magic_byte:
        transform = repair_magic_byte
    elif args.transform is not None:
        transform = load_transform(args.transform)

    consumer = Consumer(consumer_config(args.bootstrap, f"dlq-replay-{uuid.uuid4().hex[:8]}"))
    try:
        envelopes = drain_dlq(consumer, args.topic)
    finally:
        consumer.close()

    producer = Producer(producer_config(args.bootstrap))

    def produce(topic: str, key: bytes | None, payload: bytes) -> None:
        producer.produce(topic=topic, key=key, value=payload)
        producer.poll(0)

    stats = replay(
        envelopes, produce, predicate=predicate, transform=transform, dry_run=args.dry_run
    )
    producer.flush(15)
    print(stats.as_json())
    return 0


if __name__ == "__main__":
    sys.exit(main())
