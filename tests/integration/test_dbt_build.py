"""`dbt build` runs clean against the live, platform-seeded database:
every model, snapshot, seed, schema test, and singular test."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.timeout(600)]

REPO = Path(__file__).resolve().parents[2]


def test_dbt_build_all_green() -> None:
    result = subprocess.run(
        ["uv", "run", "dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=540,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-3000:]
    summary = re.search(r"PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+)", result.stdout)
    assert summary is not None, "no dbt summary line found"
    passed, _warned, errored, skipped = (int(g) for g in summary.groups())
    assert errored == 0 and skipped == 0
    assert passed >= 100, f"expected the full battery (>=100 results), got {passed}"
