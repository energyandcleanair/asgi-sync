# syntax=docker/dockerfile:1

FROM python:3.12.8-slim-bookworm AS builder

ENV POETRY_VERSION=1.8.4 \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

WORKDIR /app
COPY pyproject.toml poetry.lock ./
COPY src ./src
COPY sync-policy.toml README.md ./

RUN poetry install --only main --no-root && poetry install --only-root

FROM builder AS test

ENV PATH="/app/.venv/bin:$PATH"

RUN poetry install --no-root && poetry install --only-root
COPY tests ./tests

WORKDIR /app
CMD ["pytest", "-q", "-o", "addopts=-q"]

FROM python:3.12.8-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home --uid 1000 agsi
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/sync-policy.toml /app/sync-policy.toml

USER agsi
ENTRYPOINT ["agsi"]
CMD ["sync"]
