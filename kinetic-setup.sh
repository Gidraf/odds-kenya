#!/usr/bin/env bash
# =============================================================================
# kinetic-setup.sh — Kinetic Odds Engine management script
#
# Usage:
#   bash kinetic-setup.sh start      # start everything
#   bash kinetic-setup.sh stop       # stop app containers (keep Redis)
#   bash kinetic-setup.sh stop-all   # stop everything including Redis
#   bash kinetic-setup.sh restart    # stop app + start again
#   bash kinetic-setup.sh status     # show container status + connectivity
#   bash kinetic-setup.sh logs       # tail all logs
#   bash kinetic-setup.sh logs web   # tail specific service
#   bash kinetic-setup.sh clean      # stop app + remove images + log files (Redis kept)
#   bash kinetic-setup.sh nuke       # remove EVERYTHING including Redis data
# =============================================================================

set -euo pipefail
cd "$(dirname "$0")"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}▶ $*${RESET}"; }
success() { echo -e "${GREEN}✅ $*${RESET}"; }
warn()    { echo -e "${YELLOW}⚠️  $*${RESET}"; }
err()     { echo -e "${RED}❌ $*${RESET}"; exit 1; }
header()  { echo -e "\n${BOLD}══════════════════════════════════════════════${RESET}"; echo -e "${BOLD}  $*${RESET}"; echo -e "${BOLD}══════════════════════════════════════════════${RESET}\n"; }

# ── Docker compose command ────────────────────────────────────────────────────
if docker compose version &>/dev/null 2>&1; then
  DC="docker compose"
else
  DC="docker-compose"
fi

# ── Service groups ────────────────────────────────────────────────────────────
APP_SERVICES="web nginx_lb celery-harvest celery-results celery-analytics celery-beat flower"
ALL_SERVICES="redis6382 $APP_SERVICES"
REDIS_PASS="${REDIS_PASSWORD:-Winners1127}"

# ── Ensure .env ───────────────────────────────────────────────────────────────
ensure_env() {
  if [ ! -f .env ]; then
    warn ".env not found — creating template. Edit it before starting!"
    cat > .env << 'ENVEOF'
DATABASE_URL=postgresql://kinetic:kinetic_pass@localhost:5432/kinetic
REDIS_PASSWORD=Winners1127
REDIS_URL=redis://:Winners1127@localhost:6382/0
REDIS_CACHE_URL=redis://:Winners1127@localhost:6382/2
RABBITMQ_USER=kinetic
RABBITMQ_PASS=kinetic_rabbit_pass
CELERY_BROKER_URL=amqp://kinetic:kinetic_rabbit_pass@localhost:5672/kinetic
CELERY_RESULT_BACKEND=redis://:Winners1127@localhost:6382/1
FLOWER_USER=admin
FLOWER_PASS=flower_secure_pass
MINIO_ENDPOINT=localhost:6500
MINIO_ACCESS_KEY=gidraf
MINIO_SECRET_KEY=Winners1127
SECRET_KEY=change_me_to_something_random_32chars
FLASK_ENV=production
RESPONSE_SIGNING_SECRET=change_me
JWT_PRIVATE_KEY_PEM=
JWT_PUBLIC_KEY_PEM=
ENVEOF
    success ".env created"
  fi
  # Source it so $REDIS_PASSWORD is available
  set -a; source .env; set +a
  REDIS_PASS="${REDIS_PASSWORD:-Winners1127}"
}

# =============================================================================
# START
# =============================================================================
cmd_start() {
  header "🚀 Starting Kinetic Odds Engine"
  ensure_env

  # ── Step 1: Redis ──────────────────────────────────────────────────────────
  info "Starting Redis..."
  $DC up -d redis6382

  info "Waiting for Redis to become healthy..."
  local attempts=0
  until docker exec redis6382 redis-cli -p 6382 -a "$REDIS_PASS" ping 2>/dev/null | grep -q PONG; do
    attempts=$((attempts + 1))
    [ $attempts -gt 25 ] && err "Redis never became healthy after 50s"
    printf '.'
    sleep 2
  done
  echo ""
  success "Redis healthy"

  # ── Step 2: Web ────────────────────────────────────────────────────────────
  info "Starting web..."
  $DC up -d web

  info "Waiting for web to respond on :5000..."
  attempts=0
  until docker exec odds-kenya-web-1 curl -sf http://localhost:5000/ &>/dev/null \
     || docker exec odds-kenya-web-1 curl -sf http://localhost:5000/health &>/dev/null; do
    attempts=$((attempts + 1))
    [ $attempts -gt 30 ] && { warn "Web slow — continuing anyway"; break; }
    printf '.'
    sleep 2
  done
  echo ""
  success "Web is up"

  # ── Step 3: Nginx ──────────────────────────────────────────────────────────
  info "Starting nginx_lb..."
  $DC up -d nginx_lb

  # ── Step 4: Celery workers ─────────────────────────────────────────────────
  info "Starting Celery workers..."
  $DC up -d celery-harvest celery-results celery-analytics celery-beat

  # ── Step 5: Flower ─────────────────────────────────────────────────────────
  info "Starting Flower..."
  $DC up -d flower

  sleep 2
  echo ""
  cmd_status

  local host_ip
  host_ip=$(hostname -I | awk '{print $1}')
  echo ""
  echo -e "${BOLD}══════════════════════════════════════════════${RESET}"
  echo -e "  ${GREEN}All services started!${RESET}"
  echo ""
  echo -e "  🌐 API (nginx):    ${CYAN}http://${host_ip}:5050${RESET}"
  echo -e "  🔒 API (SSL):      ${CYAN}https://kinetic-api.gidraf.dev${RESET}"
  echo -e "  🌸 Flower:         ${CYAN}http://${host_ip}:5555/flower${RESET}"
  echo -e "  🔴 Redis:          ${CYAN}${host_ip}:6382${RESET}"
  echo -e "${BOLD}══════════════════════════════════════════════${RESET}"
}

# =============================================================================
# STOP — keep Redis running
# =============================================================================
cmd_stop() {
  header "🛑 Stopping app containers (Redis stays running)"
  info "Stopping: $APP_SERVICES"
  $DC stop $APP_SERVICES 2>/dev/null || true
  $DC rm -f $APP_SERVICES 2>/dev/null || true
  success "App containers stopped. Redis is still running."
}

# =============================================================================
# STOP ALL — including Redis
# =============================================================================
cmd_stop_all() {
  header "🛑 Stopping ALL containers"
  warn "Redis data volume is preserved (use 'nuke' to delete it)"
  $DC stop $ALL_SERVICES 2>/dev/null || true
  $DC rm -f $ALL_SERVICES 2>/dev/null || true
  success "All containers stopped."
}

# =============================================================================
# RESTART
# =============================================================================
cmd_restart() {
  header "🔄 Restarting app containers"
  cmd_stop
  sleep 2
  cmd_start
}

# =============================================================================
# CLEAN — stop app, remove images + log files, keep Redis data
# =============================================================================
cmd_clean() {
  header "🧹 Clean — remove app images and logs (Redis kept)"
  warn "This will remove app Docker images and log files."
  warn "Redis container and its data volume will NOT be touched."
  echo ""
  read -rp "  Continue? [y/N] " confirm
  [[ "${confirm,,}" == "y" ]] || { echo "Aborted."; exit 0; }

  # Stop app containers
  info "Stopping app containers..."
  $DC stop $APP_SERVICES 2>/dev/null || true
  $DC rm -f $APP_SERVICES 2>/dev/null || true

  # Remove project images (web, celery workers — not redis)
  info "Removing project Docker images..."
  local project
  project=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/-/g')
  docker images --format "{{.Repository}} {{.ID}}" | \
    grep -E "^(${project}|odds-kenya)" | \
    awk '{print $2}' | \
    xargs -r docker rmi -f 2>/dev/null || true
  docker image prune -f 2>/dev/null || true

  # Remove log files
  info "Removing log files..."
  rm -f logs/*.log logs/*.txt 2>/dev/null || true
  rm -f celerybeat-schedule celerybeat.pid 2>/dev/null || true

  success "Clean complete. Redis still running with data intact."
  echo "  To rebuild: bash kinetic-setup.sh start"
}

# =============================================================================
# NUKE — remove absolutely everything
# =============================================================================
cmd_nuke() {
  header "💣 NUKE — remove ALL containers, images, volumes, logs"
  echo -e "${RED}  ⚠️  This deletes Redis data (all cached odds).${RESET}"
  echo -e "${RED}  ⚠️  This cannot be undone.${RESET}"
  echo ""
  read -rp "  Type 'yes' to confirm: " confirm
  [[ "$confirm" == "yes" ]] || { echo "Aborted."; exit 0; }

  info "Stopping all containers..."
  $DC down --remove-orphans 2>/dev/null || true

  info "Removing project images..."
  local project
  project=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/-/g')
  docker images --format "{{.Repository}} {{.ID}}" | \
    grep -E "^(${project}|odds-kenya|redis)" | \
    awk '{print $2}' | \
    xargs -r docker rmi -f 2>/dev/null || true

  info "Removing named volumes (including Redis data)..."
  $DC down -v 2>/dev/null || true
  docker volume ls --format "{{.Name}}" | \
    grep -E "(${project}|odds-kenya|redis_data|kinetic)" | \
    xargs -r docker volume rm -f 2>/dev/null || true

  info "Pruning dangling images and build cache..."
  docker image prune -f 2>/dev/null || true
  docker builder prune -f 2>/dev/null || true

  info "Removing log files..."
  rm -f logs/*.log logs/*.txt 2>/dev/null || true
  rm -f celerybeat-schedule celerybeat.pid 2>/dev/null || true

  success "Everything removed. Fresh slate."
  echo "  To start fresh: bash kinetic-setup.sh start"
}

# =============================================================================
# STATUS
# =============================================================================
cmd_status() {
  header "📊 Container Status"
  $DC ps 2>/dev/null
  echo ""

  # Quick connectivity tests
  local redis_status web_status nginx_status
  if docker exec redis6382 redis-cli -p 6382 -a "$REDIS_PASS" ping 2>/dev/null | grep -q PONG; then
    redis_status="${GREEN}✅ healthy${RESET}"
  else
    redis_status="${RED}❌ unreachable${RESET}"
  fi

  if docker exec odds-kenya-web-1 curl -sf http://localhost:5000/ &>/dev/null \
  || docker exec odds-kenya-web-1 curl -sf http://localhost:5000/health &>/dev/null; then
    web_status="${GREEN}✅ responding${RESET}"
  else
    web_status="${RED}❌ not responding${RESET}"
  fi

  if curl -sf http://127.0.0.1:5050/ &>/dev/null; then
    nginx_status="${GREEN}✅ responding${RESET}"
  else
    nginx_status="${RED}❌ not responding${RESET}"
  fi

  echo -e "  Redis  :6382  →  $redis_status"
  echo -e "  Web    :5000  →  $web_status"
  echo -e "  Nginx  :5050  →  $nginx_status"
}

# =============================================================================
# LOGS
# =============================================================================
cmd_logs() {
  local svc="${1:-}"
  if [ -n "$svc" ]; then
    info "Tailing logs for: $svc"
    $DC logs -f --tail=100 "$svc"
  else
    info "Tailing logs for all services (Ctrl+C to stop)"
    $DC logs -f --tail=30 $ALL_SERVICES
  fi
}

# =============================================================================
# DISPATCH
# =============================================================================
case "${1:-help}" in
  start)     cmd_start ;;
  stop)      cmd_stop ;;
  stop-all)  cmd_stop_all ;;
  restart)   cmd_restart ;;
  clean)     cmd_clean ;;
  nuke)      cmd_nuke ;;
  status)    cmd_status ;;
  logs)      cmd_logs "${2:-}" ;;
  *)
    echo ""
    echo -e "${BOLD}  Kinetic Odds Engine — Management Script${RESET}"
    echo ""
    echo "  Usage:  bash kinetic-setup.sh <command> [service]"
    echo ""
    echo -e "  ${CYAN}start${RESET}          Start all services in order"
    echo -e "  ${CYAN}stop${RESET}           Stop app containers, Redis stays running"
    echo -e "  ${CYAN}stop-all${RESET}       Stop all containers including Redis"
    echo -e "  ${CYAN}restart${RESET}        Stop app + rebuild + start"
    echo -e "  ${CYAN}status${RESET}         Show status + connectivity checks"
    echo -e "  ${CYAN}logs [svc]${RESET}     Tail logs — all or one service"
    echo -e "  ${RED}clean${RESET}          Stop app + remove images + logs (Redis kept)"
    echo -e "  ${RED}nuke${RESET}           Remove EVERYTHING including Redis data ⚠️"
    echo ""
    echo "  Service names for logs:"
    echo "    web  nginx_lb  celery-harvest  celery-results"
    echo "    celery-analytics  celery-beat  flower  redis6382"
    echo ""
    echo "  Examples:"
    echo "    bash kinetic-setup.sh start"
    echo "    bash kinetic-setup.sh logs web"
    echo "    bash kinetic-setup.sh logs celery-harvest"
    echo "    bash kinetic-setup.sh stop"
    echo "    bash kinetic-setup.sh clean"
    ;;
esac