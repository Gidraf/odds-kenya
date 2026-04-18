# ============================================================
# Stage 1: Builder (compile dependencies, install tools)
# ============================================================
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies (needed for psycopg2 compilation if you ever switch from binary)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first for better caching
COPY requirements.txt .

# Install Python packages into a temporary directory
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt gunicorn gevent

# ============================================================
# Stage 2: Final (runtime image)
# ============================================================
FROM python:3.11-slim

# Create a non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install only runtime system dependencies (no build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    # If you need ffmpeg for video processing (odds feeds)
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Create app directory and set ownership
WORKDIR /app
RUN chown -R appuser:appuser /app

# Copy application code (as appuser)
COPY --chown=appuser:appuser . .

# Optional: Install Playwright browsers only if needed (uncomment if required)
# If you use Playwright for scraping, install browsers at runtime.
# RUN playwright install --with-deps chromium && chown -R appuser:appuser /ms-playwright
# ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Switch to non-root user
USER appuser

# Healthcheck (adjust endpoint as needed)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:5000/health || exit 1

# Expose port
EXPOSE 5000

# Run gunicorn (production settings)
CMD ["gunicorn", "-k", "gevent", "-w", "4", "--graceful-timeout", "30", "-b", "0.0.0.0:5000", "run:flask_app"]