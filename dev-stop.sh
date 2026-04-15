#!/usr/bin/env bash
# =============================================================================
# scripts/dev_stop.sh
# --------------------
# Gracefully stops all dev services:
#   • tmux session 'oddspedia'
#   • Celery workers + beat  (includes market alignment tasks)
#   • Flask dev server
#   • Flower (if running)
#   • Gunicorn (if running instead of flask run)
# =============================================================================

GREEN="\033[92m"; YELLOW="\033[93m"; CYAN="\033[96m"; RED="\033[91m"; RESET="\033[0m"
ok()   { echo -e "${GREEN}✔${RESET}  $1"; }
warn() { echo -e "${YELLOW}⚠${RESET}  $1"; }
info() { echo -e "${CYAN}→${RESET}  $1"; }
fail() { echo -e "${RED}✗${RESET}  $1"; }

echo ""
echo -e "${CYAN}Stopping Odds Kenya dev services…${RESET}"
echo ""

# ── 1. Kill tmux session ──────────────────────────────────────────────────────
if command -v tmux >/dev/null 2>&1; then
  if tmux has-session -t oddspedia 2>/dev/null; then
    tmux kill-session -t oddspedia
    ok "tmux session 'oddspedia' killed"
  else
    warn "tmux session 'oddspedia' not found (already stopped?)"
  fi
fi

# ── 2. Celery workers ─────────────────────────────────────────────────────────
# This stops harvest, alignment, persist, and all other Celery tasks.
CELERY_PIDS=$(pgrep -f "celery.*worker" 2>/dev/null)
if [ -n "$CELERY_PIDS" ]; then
  info "Sending SIGTERM to Celery workers (allows in-flight tasks to finish)…"
  echo "$CELERY_PIDS" | xargs kill -TERM 2>/dev/null
  sleep 4

  # Give long-running harvest tasks a little more time
  CELERY_STILL=$(pgrep -f "celery.*worker" 2>/dev/null)
  if [ -n "$CELERY_STILL" ]; then
    warn "Workers still alive after 4s — sending SIGKILL"
    echo "$CELERY_STILL" | xargs kill -9 2>/dev/null
  fi
  ok "Celery workers stopped"
else
  warn "No Celery workers found"
fi

# ── 3. Celery beat ────────────────────────────────────────────────────────────
BEAT_PIDS=$(pgrep -f "celery.*beat" 2>/dev/null)
if [ -n "$BEAT_PIDS" ]; then
  echo "$BEAT_PIDS" | xargs kill -TERM 2>/dev/null
  ok "Celery beat stopped"
else
  warn "No Celery beat process found"
fi

# ── 4. Flower ─────────────────────────────────────────────────────────────────
FLOWER_PIDS=$(pgrep -f "celery.*flower" 2>/dev/null)
if [ -n "$FLOWER_PIDS" ]; then
  echo "$FLOWER_PIDS" | xargs kill -TERM 2>/dev/null
  ok "Flower stopped"
else
  warn "No Flower process found"
fi

# ── 5. Flask dev server ───────────────────────────────────────────────────────
FLASK_PIDS=$(pgrep -f "flask run" 2>/dev/null)
if [ -n "$FLASK_PIDS" ]; then
  echo "$FLASK_PIDS" | xargs kill -TERM 2>/dev/null
  ok "Flask stopped"
else
  warn "No Flask process found"
fi

# ── 6. Gunicorn (if running instead of flask run) ────────────────────────────
GUNICORN_PIDS=$(pgrep -f "gunicorn" 2>/dev/null)
if [ -n "$GUNICORN_PIDS" ]; then
  echo "$GUNICORN_PIDS" | xargs kill -TERM 2>/dev/null
  ok "Gunicorn stopped"
fi

# ── 7. Clean up Celery schedule / pid files ───────────────────────────────────
for f in celerybeat-schedule celerybeat.pid /tmp/celerybeat-schedule-inline; do
  [ -f "$f" ] && rm -f "$f" && ok "Removed $f"
done

# ── 8. Verify ─────────────────────────────────────────────────────────────────
echo ""
info "Verifying all processes stopped…"

REMAINING=$(pgrep -f "celery|flask run|flower|gunicorn" 2>/dev/null)
if [ -n "$REMAINING" ]; then
  fail "Some processes still running:"
  echo "$REMAINING" | while read -r pid; do
    ps -p "$pid" -o pid,cmd --no-headers 2>/dev/null | sed "s/^/      /"
  done
  echo ""
  echo -e "  Force-kill all with:  ${YELLOW}kill -9 $REMAINING${RESET}"
else
  ok "All processes stopped cleanly"
fi

echo ""
echo -e "${GREEN}Done.${RESET}"
echo ""
echo -e "  Restart with:  ${CYAN}./scripts/dev_start.sh${RESET}"
echo ""
echo -e "  Log files are preserved in:  ${CYAN}logs/${RESET}"
echo "    logs/flask.log"
echo "    logs/celery.log"
echo "    logs/harvest_jobs.log"
echo ""