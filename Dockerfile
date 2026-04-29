# Multi-stage build: 빌드 단계에서 paddle 등 무거운 deps 설치 → 런타임 슬림화
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir -e .

# ----- 런타임 -----
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    poppler-utils \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY src/ ./src/
COPY pyproject.toml ./

ENV PYTHONPATH=/app/src
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8765

EXPOSE 8765
VOLUME ["/app/data"]

CMD ["python", "-m", "mcp_server.server"]
