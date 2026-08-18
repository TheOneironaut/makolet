# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

FROM ghcr.io/astral-sh/uv:0.12.3@sha256:2d890623d310b57771ce840f0da5eed5fc6d657da05ffaa45d82797b53fa3abc AS uv

FROM python:3.14.7-slim-bookworm@sha256:23c59390fc717bf09f9336908199a0ae75d9c4264bf296123f94ad772fea3b52 AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_BUILD_CONSTRAINT=/opt/makolet/build-constraints.txt \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /opt/makolet

COPY --from=uv /uv /uvx /usr/local/bin/
COPY pyproject.toml uv.lock build-constraints.txt README.md LICENSE THIRD_PARTY_NOTICES.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src/ src/
COPY benchmarks/ benchmarks/
COPY migrations/ migrations/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.14.7-slim-bookworm@sha256:23c59390fc717bf09f9336908199a0ae75d9c4264bf296123f94ad772fea3b52 AS runtime

ARG APP_UID=10001
ARG APP_GID=10001

LABEL org.opencontainers.image.title="Makolet" \
      org.opencontainers.image.description="Self-hosted Israeli supermarket price-transparency platform" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="0.1.0"

ENV PATH=/opt/makolet/.venv/bin:$PATH \
    PYTHONPATH=/opt/makolet/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONHASHSEED=random \
    TZ=Asia/Jerusalem

RUN groupadd --gid "${APP_GID}" makolet \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin makolet \
    && install -d -o "${APP_UID}" -g "${APP_GID}" \
        /opt/makolet /var/lib/makolet/raw /var/lib/makolet/exports

WORKDIR /opt/makolet
COPY --from=builder --chown=${APP_UID}:${APP_GID} /opt/makolet/.venv .venv
COPY --from=builder --chown=${APP_UID}:${APP_GID} /opt/makolet/src src
COPY --chown=${APP_UID}:${APP_GID} \
    THIRD_PARTY_NOTICES.md sbom.cdx.json sbom.build.cdx.json \
    sbom.runtime-linux.cdx.json build-constraints.txt ./
COPY --chown=${APP_UID}:${APP_GID} alembic.ini ./
COPY --chown=${APP_UID}:${APP_GID} migrations/ migrations/
COPY --chown=${APP_UID}:${APP_GID} deployment/ deployment/

USER ${APP_UID}:${APP_GID}

CMD ["makolet", "--help"]
