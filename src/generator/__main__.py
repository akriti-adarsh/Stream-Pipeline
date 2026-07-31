"""Generator CLI.

``python -m generator --speed 60 --duration 600 --seed 42`` replays a
compressed day of ride-hailing traffic. ``--sink stdout`` (default until the
Kafka transport milestone) writes JSON lines; the same seed and anchor always
produce the same stream.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from common.logging import configure_logging, with_ctx
from generator.clock import SimClock
from generator.config import GeneratorConfig
from generator.simulate import build_simulator, ticks_for, wall_anchor_ms


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
    parser.add_argument("--sink", choices=["stdout"], default="stdout")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    log = configure_logging("generator")
    cfg = GeneratorConfig(
        seed=args.seed, speed=args.speed, duration_sec=args.duration, anchor_ms=args.anchor
    )
    anchor = cfg.anchor_ms if cfg.anchor_ms is not None else wall_anchor_ms()
    clock = SimClock(anchor, cfg.tick_ms)
    sim = build_simulator(cfg, anchor)
    total_ticks = ticks_for(cfg)
    pause = 0.0 if args.no_pace else clock.real_seconds_per_tick(cfg.speed)
    emitted = 0
    log.info(
        "generator starting",
        extra=with_ctx(seed=cfg.seed, speed=cfg.speed, ticks=total_ticks, anchor_ms=anchor),
    )
    try:
        while clock.ticks < total_ticks:
            for event in sim.on_tick(clock.now_ms):
                sys.stdout.write(
                    json.dumps(
                        {
                            "topic": event.topic,
                            "key": event.key,
                            "ts": event.ts_ms,
                            "value": event.value,
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                emitted += 1
            clock.advance()
            if pause:
                time.sleep(pause)
    except KeyboardInterrupt:
        pass
    log.info(
        "generator finished",
        extra=with_ctx(
            events=emitted,
            completed=sim.completed_rides,
            cancelled=sim.cancelled_rides,
            active_left=sim.active_count(),
        ),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
