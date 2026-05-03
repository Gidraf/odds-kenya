# ============================================================
# Stage 1: Builder
# ============================================================
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt \
    gunicorn gevent playwright

# ============================================================
# Stage 2: Final
# ============================================================
FROM python:3.11-slim

RUN groupadd -r appuser && useradd -r -g appuser -m -d /home/appuser appuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # Playwright stores browsers here — must be writable by appuser
    PLAYWRIGHT_BROWSERS_PATH=/home/appuser/.cache/ms-playwright

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 curl ffmpeg \
    # Playwright system deps (chromium)
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libcairo2 libxshmfence1 \
    libglib2.0-0 libdbus-1-3 libexpat1 libx11-6 libx11-xcb1 \
    libxcb1 libxext6 libxcb-dri3-0 fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app

RUN mkdir -p /app/logs && chown -R appuser:appuser /app

COPY --chown=appuser:appuser . .

USER appuser

# Install Playwright browsers (chromium only to keep image smaller)
RUN python -m playwright install chromium

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:5000/health || exit 1

EXPOSE 5000

CMD ["gunicorn", "-k", "gevent", "-w", "1", \
     "--graceful-timeout", "30", \
     "--timeout", "300", \
     "--keep-alive", "5", \
     "--worker-connections", "1000", \
     "-b", "0.0.0.0:5000", \
     "run:flask_app"]