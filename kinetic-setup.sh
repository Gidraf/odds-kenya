#!/usr/bin/env bash
# =============================================================================
# kinetic-setup.sh
# RabbitMQ + Flower setup and monitoring commands
# Run: bash kinetic-setup.sh
# =============================================================================

set -euo pipefail

echo "
╔═══════════════════════════════════════════════════════════════╗
║         Kinetic Odds Engine — Infrastructure Setup            ║
╚═══════════════════════════════════════════════════════════════╝
"

# ─── 1. Write .env if it doesn't exist ───────────────────────────────────────

if [ ! -f .env ]; then
cat > .env << 'EOF'
# ── Database ──────────────────────────────────────────────────────────────────
DATABASE_URL=postgresql://kinetic:kinetic_pass@localhost:5432/kinetic

# ── Redis (cache + pubsub — NOT broker) ───────────────────────────────────────
REDIS_PASSWORD=%40Winners1127
REDIS_URL=redis://:%40Winners1127@localhost:6382/0
REDIS_CACHE_URL=redis://:%40Winners1127@localhost:6382/2

# ── RabbitMQ (Celery broker) ───────────────────────────────────────────────────
RABBITMQ_USER=kinetic
RABBITMQ_PASS=kinetic_rabbit_pass
CELERY_BROKER_URL=amqp://kinetic:kinetic_rabbit_pass@localhost:5672/kinetic
CELERY_RESULT_BACKEND=redis://:%40Winners1127@localhost:6382/1

# ── Flower (task monitor) ──────────────────────────────────────────────────────
FLOWER_USER=admin
FLOWER_PASS=flower_secure_pass

# ── MinIO ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT=localhost:6500
MINIO_ACCESS_KEY=gidraf
MINIO_SECRET_KEY=Winners1127

# ── App ───────────────────────────────────────────────────────────────────────
SECRET_KEY=your_secret_key_here
FLASK_ENV=production

# ── Notifications ─────────────────────────────────────────────────────────────
MAIL_SERVER=smtp.gmail.com
MAIL_PORT=587
MAIL_USE_TLS=true
MAIL_USERNAME=your@email.com
MAIL_PASSWORD=your_app_password

# ── Frontend ──────────────────────────────────────────────────────────────────
NEXT_PUBLIC_API_URL=https://kinetic-api.gidraf.dev/api
NEXT_PUBLIC_WS_URL=wss://kinetic-api.gidraf.dev
EOF
echo "✅ .env created — edit it with your values before continuing"
fi

# ─── 2. Start infrastructure ──────────────────────────────────────────────────

echo "🐳 Starting infrastructure containers..."
# docker compose up -d redis6382 rabbitmq

# echo "⏳ Waiting for RabbitMQ to be ready..."
# until docker exec kinetic-rabbitmq rabbitmq-diagnostics ping > /dev/null 2>&1; do
#   printf '.'
#   sleep 3
# done
# echo ""
# echo "✅ RabbitMQ is ready"

# ─── 3. Start application ─────────────────────────────────────────────────────

echo "🚀 Starting application containers..."
docker compose up -d web celery-harvest celery-results celery-analytics celery-beat flower nginx_lb

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Kinetic Odds Engine is running!"
echo ""
echo "  📊 RabbitMQ Management UI:  http://localhost:15672"
echo "     Username: \$(grep RABBITMQ_USER .env | cut -d= -f2)"
echo "     Password: \$(grep RABBITMQ_PASS .env | cut -d= -f2)"
echo ""
echo "  🌸 Flower (Celery Monitor): http://localhost:5555/flower"
echo "     Username: \$(grep FLOWER_USER .env | cut -d= -f2)"
echo "     Password: \$(grep FLOWER_PASS .env | cut -d= -f2)"
echo ""
echo "  🌐 Kinetic API:             https://kinetic-api.gidraf.dev"
echo "  💻 Kinetic Frontend:        http://localhost:3050"
echo "═══════════════════════════════════════════════════════════════"


# =============================================================================
# MONITORING COMMANDS — run these to see worker status
# =============================================================================

cat << 'MONITORING'

═══════════════════════════════════════════════════════════════
MONITORING COMMANDS
═══════════════════════════════════════════════════════════════

── See all running workers ──────────────────────────────────────
docker compose ps

── Check which tasks are registered on each worker ──────────────
docker exec odds-kenya-celery-harvest-1 \
  celery -A app.workers.celery_app inspect registered

── See currently active tasks ───────────────────────────────────
docker exec odds-kenya-celery-harvest-1 \
  celery -A app.workers.celery_app inspect active

── See scheduled/reserved tasks ─────────────────────────────────
docker exec odds-kenya-celery-harvest-1 \
  celery -A app.workers.celery_app inspect scheduled

── Ping all workers ─────────────────────────────────────────────
docker exec odds-kenya-celery-harvest-1 \
  celery -A app.workers.celery_app inspect ping

── Check queue depths in RabbitMQ ───────────────────────────────
docker exec kinetic-rabbitmq \
  rabbitmqctl list_queues name messages consumers

── View Redis keys (what's in cache) ────────────────────────────
docker exec redis6382 redis-cli -p 6382 -a '@Winners1127' \
  keys "sp:upcoming:*"

docker exec redis6382 redis-cli -p 6382 -a '@Winners1127' \
  keys "*upcoming*"

── Count matches in a key ───────────────────────────────────────
docker exec redis6382 redis-cli -p 6382 -a '@Winners1127' \
  strlen "sp:upcoming:soccer"

── Debug which Redis keys exist per sport ────────────────────────
curl "https://kinetic-api.gidraf.dev/api/monitor/redis-keys?sport=soccer"

── View harvest health from DB ──────────────────────────────────
docker exec -it odds-kenya-web-1 flask harvest-health

── View failing markets ─────────────────────────────────────────
docker exec -it odds-kenya-web-1 flask market-failures --top 20

── Manually trigger BT+OD harvest ──────────────────────────────
docker exec -it odds-kenya-web-1 flask shell << 'EOF'
from app.workers.tasks_upcoming import bt_od_harvest_all_upcoming
result = bt_od_harvest_all_upcoming.apply_async(queue="harvest")
print("Dispatched:", result)
EOF

── Tail harvest worker logs ─────────────────────────────────────
docker logs odds-kenya-celery-harvest-1 --tail 100 -f

── Tail results worker logs ─────────────────────────────────────
docker logs odds-kenya-celery-results-1 --tail 100 -f

── Full restart ─────────────────────────────────────────────────
docker compose down && docker compose up -d

MONITORING