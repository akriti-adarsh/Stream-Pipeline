# CLAUDE.md — standing rules and state
The spec is docs/BUILD_SPEC.md. This file is rules and state; the spec defines the work.

## Rules — non-negotiable
1. No TODO, FIXME, NotImplementedError, or stub bodies anywhere in src/. Ever.
2. Dependency versions come from the resolver (`uv add` / `npm install`); commit the lockfile.
   Never hand-type a version number the resolver has not produced.
3. Nothing is "done" until its command has run in THIS session with the real output shown —
   the actual pytest summary line, the actual exit status. "Should pass" is not a status.
4. Every number in a README or doc must exist in a committed artifact (eval_results/,
   benchmarks/results/, a CI log). An estimated or remembered number is a defect.
5. When a library, API, or dataset differs from the spec — renamed function, changed endpoint,
   auth now required — adapt to reality and add one line to DEVIATIONS.md
   (spec said / reality is / what was done). Never mock a real path to fake compliance.
6. Never weaken, skip, or delete a test to make it pass. Fix the code or flag the conflict.
7. One commit per plan milestone; the full test suite runs green before every commit.
8. If the next milestone will not fit in the session's remaining capacity, stop at the last
   green commit and update State. Do not start work you cannot finish.

## State (update at every commit)
- Plan position: 28 of 28 COMPLETE (30 commits total: 28 milestones + 2 hardening/observability extras). Build finished 2026-07-31.
- Suite at last commit: 129 passed, 10 deselected · Coverage: 83.4% · ruff + mypy --strict clean
- Acceptance: e2e cold start green in 227s (all 6 layers); kill matrix 3 consecutive full runs green (12/12 SIGKILL trials); dbt 121/121; DQ gates proven failing on injected bad data (screenshot committed); throughput 3,636 msg/s sustained, produce-to-queryable p50 0.312s / p95 0.360s (benchmarks/results/)
- Open deviations: 6, all recorded in DEVIATIONS.md
- Notes for next session: kill matrix needs Flink stopped (core profile); screenshots via CDP attach (docs/img/); stack may be left running under docker compose --profile full
