"""Register every subject in the Schema Registry. Idempotent; safe to run at every boot.

Usage: uv run python scripts/register_schemas.py [--url http://localhost:18081]
"""

from __future__ import annotations

import argparse
import sys

from common.logging import configure_logging
from common.schemas import register_all


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default="http://localhost:18081",
        help="Schema Registry base URL (Redpanda built-in registry)",
    )
    args = parser.parse_args()
    log = configure_logging("register-schemas")
    registered = register_all(args.url)
    for subject, schema_id in sorted(registered.items()):
        log.info("registered subject", extra={"ctx": {"subject": subject, "schema_id": schema_id}})
    return 0


if __name__ == "__main__":
    sys.exit(main())
