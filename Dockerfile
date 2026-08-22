# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN python -m pip wheel --wheel-dir /wheels . \
    && python -m venv /opt/mc-failover \
    && /opt/mc-failover/bin/python -m pip install \
        --no-compile --no-index --find-links=/wheels /wheels/minecraft_failover_proxy-*.whl

FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="Minecraft Python Failover Proxy" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/leonardgrimm13-netizen/Minecraft-Python-Failover-Proxy"

ENV PATH="/opt/mc-failover/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 mcfailover \
    && useradd --uid 10001 --gid 10001 --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin mcfailover \
    && install -d -o root -g 10001 -m 0750 /config \
    && install -d -o root -g root -m 0755 /app

COPY --from=builder /opt/mc-failover /opt/mc-failover

WORKDIR /app
USER 10001:10001

EXPOSE 25565/tcp
STOPSIGNAL SIGTERM

# Probe the already-running monitoring handler, not a fresh config parse alone.
# The shipped example enables loopback-only monitoring for this purpose.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["mc-failover", "--config", "/config/config.toml", "--probe-live"]

ENTRYPOINT ["mc-failover"]
CMD ["--config", "/config/config.toml"]
