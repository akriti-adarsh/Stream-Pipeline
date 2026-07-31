# One image for every Python service in the platform (generator, sessionizer,
# pg-sink, iceberg-sink, dq-runner, lag-exporter, streamlit ui); compose picks
# the entrypoint per service. Build context is the repo root.
FROM python:3.12.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.10.7 /uv /uvx /bin/

WORKDIR /app
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1

# Dependency layer first: cache survives source edits.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY . .
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "generator", "--help"]
