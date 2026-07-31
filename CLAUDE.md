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
- Plan position: 10 of 28 committed; 11-26 BUILT and verified live, commits pending in order.
- Suite at working tree: 129 passed, 10 deselected · Coverage: 83.4%
- Open deviations: 4 · Kill test: all 4 SIGKILL variants passed live; Flink 3 pipelines RUNNING with late.events proven; Iceberg 18571 rows + evolution + compaction proven; dbt 121/121; GE gates live; DLQ replay cycle passed; ride ids now run-token-scoped (cross-run collision fix)
- Waiting on: observability agent (metrics+prometheus+grafana), then commits 11-28, app containerization, screenshots, final README
- Live smoke: compose core healthy; generator kafka sink 15s -> 9609 events (~640/s), 4 subjects, 9 topics; stack left RUNNING for processor work
- Infra pre-verified by agents: compose core healthy in 15.2s (Redpanda v25.3.15, PG 16.14, MinIO), flink 1.20.5 image + jars built, iceberg-rest-fixture 1.10.1; host ports 19092/18081/5433/19000/18080
- Notes for next session: Windows host; make available in Git Bash; Docker 29.6.2 with 16.4 GB
