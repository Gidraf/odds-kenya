FROM python:3.11-slim

WORKDIR /app

# 1. Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 2. Install base system dependencies AND ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    postgresql-client \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# 3. Copy requirements first
COPY requirements.txt .

# 4. Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt gunicorn gevent "psycopg[binary]" playwright

# 5. Install Playwright browsers and dependencies
RUN playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*

# 6. Copy application code
COPY . .

# 7. Expose internal port
EXPOSE 5000

# 8. Default command
CMD ["gunicorn", "-k", "gevent", "-w", "4", "-b", "0.0.0.0:5000", "run:app"]