#!/usr/bin/env bash
# =============================================================================
# scripts/dev_start.sh
# ---------------------
# Starts Flask + all Celery services in a tmux session.
#
# Services:
#   flask    — API server
#   celery   — combined worker + beat
#              queues: harvest, live, results, notify, ev_arb, default
#   flower   — Celery monitoring dashboard (optional)
#   logs     — live tail of all log files
#
# All logs written to logs/ and viewable at:
#   http://localhost:5500/api/monitor/dashboard
#
# Market Alignment:
#   Runs automatically every 15 min (beat) and 60s after each harvest.
#   Merges BT + SP + OD markets into one best-price view per match.
#   Status visible in health dashboard: align_last_run, align_aligned, align_arbs
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
redis-cli -u "$REDIS_URL" ping >/dev/null 2>&1 \
  && ok "Redis reachable" \
  || { warn "Redis not responding at $REDIS_URL"; exit 1; }
command -v celery >/dev/null 2>&1 || { warn "celery not found — pip install celery"; exit 1; }
command -v flask  >/dev/null 2>&1 || { warn "flask not found — pip install flask";   exit 1; }
ok "All dependencies found"
echo ""

# ── Commands ──────────────────────────────────────────────────────────────────

# Flask API
CMD_FLASK="FLASK_APP=${FLASK_APP} FLASK_ENV=development ENABLE_HARVESTER=1 \
  flask run --host=0.0.0.0 --port=${FLASK_PORT} --debug \
  2>&1 | tee ${LOG_DIR}/flask.log"

# Celery combined worker + beat
# Queues:
#   harvest  — SP/BT/OD/SBO/B2B harvest tasks (long-running, up to 1h)
#   results  — persist_combined_batch + market alignment tasks
#   ev_arb   — EV and arbitrage detection
#   notify   — email / WhatsApp notifications
#   default  — health checks, health report
#   live     — live harvest tasks (disabled, reserved for future)
#
# concurrency=8 handles:
#   • Up to 8 concurrent harvest tasks (one per sport per bookmaker)
#   • Alignment tasks run in parallel with harvests (separate queue)
#   • SP/BT tasks are I/O bound (HTTP calls) so 8 is a good balance
CMD_CELERY="celery -A ${CELERY_APP} worker -B \
  --queues=harvest,live,results,notify,ev_arb,default \
  --concurrency=8 \
  --loglevel=info \
  --logfile=${LOG_DIR}/celery.log \
  --hostname=all@%h"

# Flower monitoring dashboard (uncomment to enable)
CMD_FLOWER="celery -A ${CELERY_APP} flower \
  --port=5555 \
  --loglevel=info \
  2>&1 | tee ${LOG_DIR}/flower.log"

# ── tmux ──────────────────────────────────────────────────────────────────────
if command -v tmux >/dev/null 2>&1; then
  SESSION="oddspedia"
  info "Starting in tmux session '${SESSION}' …"

  # Kill any existing session cleanly
  tmux kill-session -t "$SESSION" 2>/dev/null || true
  sleep 1

  # Window 0 — Flask API
  tmux new-session  -d -s "$SESSION" -n "flask"
  tmux send-keys    -t "$SESSION:flask" "$CMD_FLASK" Enter

  # Window 1 — Celery (worker + beat + alignment service)
  tmux new-window   -t "$SESSION" -n "celery"
  tmux send-keys    -t "$SESSION:celery" "$CMD_CELERY" Enter

  # Window 2 — Flower (commented out by default)
  tmux new-window   -t "$SESSION" -n "flower"
  tmux send-keys    -t "$SESSION:flower" "echo 'Flower disabled — uncomment CMD_FLOWER in dev_start.sh to enable'" Enter
  # Uncomment to enable Flower:
  # tmux send-keys  -t "$SESSION:flower" "$CMD_FLOWER" Enter

  # Window 3 — Live log tail (all logs at once)
  tmux new-window   -t "$SESSION" -n "logs"
  tmux send-keys    -t "$SESSION:logs" \
    "tail -f \
      ${LOG_DIR}/flask.log \
      ${LOG_DIR}/celery.log \
      ${LOG_DIR}/harvest_jobs.log \
      ${LOG_DIR}/flower.log \
      2>/dev/null" \
    Enter

  echo ""
  ok "All services launched"
  echo ""
  echo -e "  ${CYAN}tmux windows:${RESET}"
  echo "    Ctrl+B 0  — Flask API (port ${FLASK_PORT})"
  echo "    Ctrl+B 1  — Celery worker + beat (harvest / align / persist)"
  echo "    Ctrl+B 2  — Flower dashboard (disabled — see dev_start.sh)"
  echo "    Ctrl+B 3  — Live log tail (all services)"
  echo ""
  echo -e "  ${CYAN}URLs:${RESET}"
  echo "    Flask API    →  http://localhost:${FLASK_PORT}"
  echo "    Monitor      →  http://localhost:${FLASK_PORT}/api/monitor/dashboard"
  echo "    Odds API     →  http://localhost:${FLASK_PORT}/api/odds/upcoming/soccer"
  echo "    Flower       →  http://localhost:5555  (enable in dev_start.sh)"
  echo ""
  echo -e "  ${CYAN}Log files:${RESET}"
  echo "    ${LOG_DIR}/flask.log"
  echo "    ${LOG_DIR}/celery.log"
  echo "    ${LOG_DIR}/harvest_jobs.log"
  echo ""
  echo -e "  ${CYAN}Market alignment:${RESET}"
  echo "    Runs automatically every 15 min (beat schedule)"
  echo "    Also triggers 60s after each harvest completes"
  echo "    Status: GET /api/monitor/dashboard → alignment section"
  echo "    Manual: celery -A ${CELERY_APP} call tasks.align.all"
  echo "    Single: celery -A ${CELERY_APP} call tasks.align.sport --args='[\"soccer\"]'"
  echo ""
  echo -e "  ${CYAN}Harvest schedule:${RESET}"
  echo "    SP / BT / OD  —  every 1 hour (long-running, up to 15k matches)"
  echo "    B2B / SBO     —  every 3-8 min (fast)"
  echo "    Alignment     —  every 15 min + triggered after each harvest"
  echo ""

  tmux attach -t "$SESSION"
  exit 0
fi

# ── Fallback: print manual commands ───────────────────────────────────────────
warn "tmux not found — run these in separate terminals:"
echo ""
echo "# Terminal 1 — Flask"
echo "FLASK_APP=${FLASK_APP} ENABLE_HARVESTER=1 \\"
echo "  flask run --host=0.0.0.0 --port=${FLASK_PORT} --debug"
echo ""
echo "# Terminal 2 — Celery (worker + beat)"
echo "celery -A ${CELERY_APP} worker -B \\"
echo "  --queues=harvest,live,results,notify,ev_arb,default \\"
echo "  --concurrency=8 \\"
echo "  --loglevel=info \\"
echo "  --logfile=${LOG_DIR}/celery.log \\"
echo "  --hostname=all@%h"
echo ""
echo "# Terminal 3 — Flower (optional)"
echo "# celery -A ${CELERY_APP} flower --port=5555"
echo ""
echo "# Terminal 4 — Log tail"
echo "tail -f ${LOG_DIR}/flask.log ${LOG_DIR}/celery.log ${LOG_DIR}/harvest_jobs.log"
echo ""
echo -e "Monitor dashboard  →  ${CYAN}http://localhost:${FLASK_PORT}/api/monitor/dashboard${RESET}"
echo -e "Odds API           →  ${CYAN}http://localhost:${FLASK_PORT}/api/odds/upcoming/soccer${RESET}"
echo ""
echo -e "Trigger alignment manually:"
echo "  celery -A ${CELERY_APP} call tasks.align.all"
echo "  celery -A ${CELERY_APP} call tasks.align.sport --args='[\"soccer\"]'"