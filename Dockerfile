# Build from the repository root:
#   docker build -t wotpy-modbus:latest .

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/wotpy

COPY pyproject.toml setup.py README.md LICENSE ./
COPY wotpy ./wotpy

RUN pip install --upgrade pip && pip install .

COPY examples/zenoh-ttc /app

RUN useradd --create-home --uid 10001 wotpy && chown -R wotpy:wotpy /app
USER wotpy

WORKDIR /app
