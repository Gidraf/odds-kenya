# ============================================================
# Stage 1: Builder (compile dependencies, install tools)
# ============================================================
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt gunicorn gevent

# ============================================================
# Stage 2: Final (runtime image)
# ============================================================
FROM python:3.11-slim

# Create a non-root user WITH a home directory
# -m creates /home/appuser, -d sets the home directory
RUN groupadd -r appuser && useradd -r -g appuser -m -d /home/appuser appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install only runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Create app directory and set ownership
WORKDIR /app

# ------------------------------------------------------------------
# Create logs directory and ensure appuser owns everything
# ------------------------------------------------------------------
RUN mkdir -p /app/logs && chown -R appuser:appuser /app

# Copy application code (as appuser)
COPY --chown=appuser:appuser . .

# Switch to non-root user
USER appuser

# Healthcheck (assumes your app has a /health endpoint)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:5000/health || exit 1

EXPOSE 5000

# Run gunicorn with the correct application object: "flask_app" from run.py
CMD ["gunicorn", "-k", "gevent", "-w", "4", "--graceful-timeout", "30", "-b", "0.0.0.0:5000", "run:flask_app"]