#!/usr/bin/env bash
# =============================================================================
# scripts/dev_start.sh
# ---------------------
# Starts Flask + all Celery services in a tmux session.
# All logs are written to the logs/ directory AND streamed at:
#   http://localhost:5050/api/monitor/dashboard
# =============================================================================
set -e

GREEN="\033[92m"; YELLOW="\033[93m"; CYAN="\033[96m"; RESET="\033[0m"
info() { echo -e "${CYAN}→${RESET} $1"; }
ok()   { echo -e "${GREEN}✔${RESET} $1"; }
warn() { echo -e "${YELLOW}⚠${RESET} $1"; }

# ── Config ────────────────────────────────────────────────────────────────────
FLASK_APP="${FLASK_APP:-app}"
FLASK_PORT="${FLASK_PORT:-5500}"
REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
LOG_DIR="${LOG_DIR:-logs}"
CELERY_APP="app.workers.celery_tasks"

mkdir -p "$LOG_DIR"

# ── Checks ────────────────────────────────────────────────────────────────────
info "Checking dependencies …"
command -v redis-cli >/dev/null 2>&1 || { warn "redis-cli not found"; exit 1; }
redis-cli -u "$REDIS_URL" ping >/dev/null 2>&1 && ok "Redis reachable" || { warn "Redis not responding"; exit 1; }
command -v celery >/dev/null 2>&1 || { warn "celery not found"; exit 1; }
command -v flask  >/dev/null 2>&1 || { warn "flask not found";  exit 1; }
ok "All dependencies found"
echo ""

# ── Commands ──────────────────────────────────────────────────────────────────
CMD_FLASK="FLASK_APP=${FLASK_APP} FLASK_ENV=development ENABLE_HARVESTER=1 \
  flask run --host=0.0.0.0 --port=${FLASK_PORT} --debug 2>&1 | tee ${LOG_DIR}/flask.log"

# Single combined worker+beat — verbose + logfile
CMD_CELERY="celery -A ${CELERY_APP} worker -B \
  --queues=harvest,live,results,notify,ev_arb,default \
  --concurrency=8 \
  --loglevel=debug \
  --logfile=${LOG_DIR}/celery.log \
  --hostname=all@%h"

# Flower dashboard
# CMD_FLOWER="celery -A ${CELERY_APP} flower --port=5555 --loglevel=info"

# ── tmux ──────────────────────────────────────────────────────────────────────
if command -v tmux >/dev/null 2>&1; then
  SESSION="oddspedia"
  info "Starting in tmux session '${SESSION}' …"

  tmux kill-session -t "$SESSION" 2>/dev/null || true
  tmux new-session -d -s "$SESSION" -n "flask"
  tmux send-keys -t "$SESSION:flask" "$CMD_FLASK" Enter

  tmux new-window -t "$SESSION" -n "celery"
  tmux send-keys -t "$SESSION:celery" "$CMD_CELERY" Enter

  tmux new-window -t "$SESSION" -n "flower"
  tmux send-keys -t "$SESSION:flower" "$CMD_FLOWER" Enter

  # Tail window — watch all logs at once
  tmux new-window -t "$SESSION" -n "logs"
  tmux send-keys -t "$SESSION:logs" \
    "tail -f ${LOG_DIR}/flask.log ${LOG_DIR}/celery.log ${LOG_DIR}/tasks.log ${LOG_DIR}/harvest_jobs.log 2>/dev/null" Enter

  echo ""
  ok "All services launched"
  echo ""
  echo -e "  ${CYAN}tmux windows:${RESET}"
  echo "    Ctrl+B 0  — Flask API"
  echo "    Ctrl+B 1  — Celery worker + beat"
  echo "    Ctrl+B 2  — Flower dashboard"
  echo "    Ctrl+B 3  — Live log tail"
  echo ""
  echo -e "  ${CYAN}URLs:${RESET}"
  echo "    Flask API  → http://localhost:${FLASK_PORT}"
  echo "    Monitor    → http://localhost:${FLASK_PORT}/api/monitor/dashboard  ← OPEN THIS"
  echo "    Flower     → http://localhost:5555"
  echo ""
  echo -e "  ${CYAN}Log files:${RESET}"
  echo "    ${LOG_DIR}/flask.log"
  echo "    ${LOG_DIR}/celery.log"
  echo "    ${LOG_DIR}/tasks.log"
  echo "    ${LOG_DIR}/harvest_jobs.log"
  echo ""
  tmux attach -t "$SESSION"
  exit 0
fi

# ── Fallback: print manual commands ───────────────────────────────────────────
warn "tmux not found — run these in separate terminals:"
echo ""
echo "# Terminal 1 — Flask"
echo "FLASK_APP=${FLASK_APP} ENABLE_HARVESTER=1 flask run --host=0.0.0.0 --port=${FLASK_PORT} --debug"
echo ""
echo "# Terminal 2 — Celery (worker + beat)"
echo "celery -A ${CELERY_APP} worker -B \\"
echo "  --queues=harvest,live,results,notify,ev_arb,default \\"
echo "  --concurrency=8 --loglevel=debug \\"
echo "  --logfile=${LOG_DIR}/celery.log --hostname=all@%h"
echo ""
echo "# Terminal 3 — Flower"
# echo "celery -A ${CELERY_APP} flower --port=5555"
echo ""
echo "# Terminal 4 — Log tail"
echo "tail -f ${LOG_DIR}/flask.log ${LOG_DIR}/celery.log ${LOG_DIR}/harvest_jobs.log"
echo ""
echo "Monitor dashboard → http://localhost:${FLASK_PORT}/api/monitor/dashboard"