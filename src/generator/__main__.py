"""Generator CLI.

``python -m generator --speed 60 --duration 600 --seed 42`` replays a
compressed day of ride-hailing traffic. The same seed and anchor always
produce the same stream; ``--speed`` only changes how fast it is replayed.

Imperfections ship on by default with documented rates (see GeneratorConfig);
``--perfect`` disables them all, which is what the exactly-once kill test
uses. ``--evolve-after N`` switches the stream to payload_version 2 (adding
promo_code) after N real seconds, live, without restarting anything.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from common.logging import configure_logging, with_ctx
from generator.config import GeneratorConfig
from generator.events import SourceEvent
from generator.producer import KafkaTransport
from generator.simulate import simulate_ticks, ticks_for, wall_anchor_ms


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="generator", description=__doc__)
    parser.add_argument("--speed", type=float, default=60.0, help="sim seconds per real second")
    parser.add_argument("--duration", type=float, default=600.0, help="real seconds to run")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--anchor",
        type=int,
        default=None,
        help="sim origin as epoch millis; defaults to the current wall minute",
    )
    parser.add_argument("--no-pace", action="store_true", help="emit as fast as possible")
    parser.add_argument("--sink", choices=["stdout", "kafka"], default="stdout")
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--registry", default="http://localhost:18081")
    parser.add_argument(
        "--acked-manifest",
        type=Path,
        default=None,
        help="write broker-acked event ids here (jsonl); ground truth for the kill test",
    )
    parser.add_argument("--rides-per-min", type=float, default=40.0)
    parser.add_argument("--drivers", type=int, default=300)
    parser.add_argument("--dup-rate", type=float, default=0.005)
    parser.add_argument("--ooo-rate", type=float, default=0.02)
    parser.add_argument("--late-rate", type=float, default=0.01)
    parser.add_argument("--malformed-rate", type=float, default=0.002)
    parser.add_argument("--null-rate", type=float, default=0.01)
    parser.add_argument("--abandon-rate", type=float, default=0.003)
    parser.add_argument("--perfect", action="store_true", help="disable every imperfection")
    parser.add_argument(
        "--evolve-after",
        type=float,
        default=None,
        help="real seconds until the stream switches to payload_version 2",
    )
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> GeneratorConfig:
    cfg = GeneratorConfig(
        seed=args.seed,
        speed=args.speed,
        duration_sec=args.duration,
        anchor_ms=args.anchor,
        base_rides_per_min=args.rides_per_min,
        drivers_total=args.drivers,
        dup_rate=args.dup_rate,
        ooo_rate=args.ooo_rate,
        late_rate=args.late_rate,
        malformed_rate=args.malformed_rate,
        null_field_rate=args.null_rate,
        abandon_rate=args.abandon_rate,
        evolve_after_sec=args.evolve_after,
    )
    return cfg.perfect() if args.perfect else cfg


class StdoutSink:
    def __init__(self) -> None:
        self.sent = 0

    def send(self, event: SourceEvent) -> None:
        record = {
            "topic": event.topic,
            "key": event.key,
            "ts": event.ts_ms,
            "value": event.value,
        }
        if event.corrupt:
            record["corrupt"] = True
        sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
        self.sent += 1

    def close(self) -> None:
        sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    log = configure_logging("generator")
    cfg = config_from_args(args)
    anchor = cfg.anchor_ms if cfg.anchor_ms is not None else wall_anchor_ms()
    total_ticks = ticks_for(cfg)
    pause = 0.0 if args.no_pace else (cfg.tick_ms / 1000.0) / cfg.speed

    if args.sink == "kafka":
        sink: KafkaTransport | StdoutSink = KafkaTransport(
            args.bootstrap, args.registry, acked_manifest=args.acked_manifest
        )
    else:
        sink = StdoutSink()

    emitted = 0
    log.info(
        "generator starting",
        extra=with_ctx(
            seed=cfg.seed,
            speed=cfg.speed,
            ticks=total_ticks,
            anchor_ms=anchor,
            sink=args.sink,
            perfect=args.perfect,
            evolve_after=args.evolve_after,
        ),
    )
    try:
        for _, batch in simulate_ticks(cfg, ticks=total_ticks, anchor_ms=anchor):
            for event in batch:
                sink.send(event)
                emitted += 1
            if pause:
                time.sleep(pause)
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()
    log.info("generator finished", extra=with_ctx(events=emitted))
    return 0


if __name__ == "__main__":
    sys.exit(main())
