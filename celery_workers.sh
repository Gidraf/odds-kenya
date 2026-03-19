#!/usr/bin/env bash
# celery_workers.sh — CPU + memory aware Celery worker manager
#
# Usage:
#   ./celery_workers.sh start
#   ./celery_workers.sh stop
#   ./celery_workers.sh restart
#   ./celery_workers.sh status
#   ./celery_workers.sh logs harvest|live|ev_arb|results|notify|default|beat|all

set -euo pipefail

APP="app.workers.celery_tasks"
LOG_DIR="logs/celery"
PID_DIR="run/celery"

mkdir -p "$LOG_DIR" "$PID_DIR"

# ── Colours ───────────────────────────────────────────────────────────────────
green()  { echo -e "\033[32m$*\033[0m"; }
yellow() { echo -e "\033[33m$*\033[0m"; }
red()    { echo -e "\033[31m$*\033[0m"; }
cyan()   { echo -e "\033[36m$*\033[0m"; }

pid_file() { echo "$PID_DIR/$1.pid"; }
log_file() { echo "$LOG_DIR/$1.log"; }

is_running() {
  local pf
  pf=$(pid_file "$1")
  [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null
}

# =============================================================================
# CPU + Memory detection
# =============================================================================

detect_resources() {
  # ── CPU cores ──────────────────────────────────────────────────────────────
  CPU_CORES=1
  if command -v nproc &>/dev/null; then
    CPU_CORES=$(nproc)
  elif [ -f /proc/cpuinfo ]; then
    CPU_CORES=$(grep -c ^processor /proc/cpuinfo)
  elif command -v sysctl &>/dev/null; then
    CPU_CORES=$(sysctl -n hw.logicalcpu 2>/dev/null || echo 1)
  fi

  # ── Total RAM in MB ────────────────────────────────────────────────────────
  MEM_MB=512
  if [ -f /proc/meminfo ]; then
    MEM_MB=$(awk '/MemTotal/ { printf "%d", $2/1024 }' /proc/meminfo)
  elif command -v sysctl &>/dev/null; then
    MEM_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo 536870912)
    MEM_MB=$(( MEM_BYTES / 1024 / 1024 ))
  fi

  # ── Available RAM in MB (free + reclaimable cache) ─────────────────────────
  MEM_AVAIL_MB=$MEM_MB
  if [ -f /proc/meminfo ]; then
    MEM_AVAIL_MB=$(awk '/MemAvailable/ { printf "%d", $2/1024 }' /proc/meminfo)
  fi

  # ── Reserve 20% of available RAM for the OS / Flask / Redis ───────────────
  MEM_BUDGET_MB=$(( MEM_AVAIL_MB * 80 / 100 ))

  # ── Each Celery worker process uses ~80-150 MB with Flask app context.
  #    We assume 110 MB per process (conservative) to avoid OOM.
  MB_PER_WORKER=110
  MAX_BY_MEM=$(( MEM_BUDGET_MB / MB_PER_WORKER ))
  [ "$MAX_BY_MEM" -lt 1 ] && MAX_BY_MEM=1

  # ── Total worker slots: min(2×CPU, budget_by_mem) ─────────────────────────
  MAX_TOTAL=$(( CPU_CORES * 2 ))
  [ "$MAX_TOTAL" -gt "$MAX_BY_MEM" ] && MAX_TOTAL=$MAX_BY_MEM
  # Hard floor: at least 4 total slots so beat tasks can run
  [ "$MAX_TOTAL" -lt 4 ] && MAX_TOTAL=4
}

# =============================================================================
# Concurrency allocation
#
# Budget is split proportionally across 6 worker types.
# Ratios (must sum to 100):
#   harvest  35%  — most I/O, highest throughput needed
#   live     25%  — low latency, second priority
#   ev_arb   15%  — CPU-bound math, moderate
#   results   8%  — occasional DB writes
#   notify    8%  — low frequency email/push
#   default   9%  — health, expiry — very light
# =============================================================================

allocate_concurrency() {
  detect_resources

  # Proportional split (integer arithmetic, floor)
  C_HARVEST=$(( MAX_TOTAL * 35 / 100 )); [ "$C_HARVEST" -lt 1 ] && C_HARVEST=1
  C_LIVE=$(   ( MAX_TOTAL * 25 / 100 )); [ "$C_LIVE"    -lt 1 ] && C_LIVE=1
  C_EVARB=$(  ( MAX_TOTAL * 15 / 100 )); [ "$C_EVARB"   -lt 1 ] && C_EVARB=1
  C_RESULTS=$(( MAX_TOTAL *  8 / 100 )); [ "$C_RESULTS" -lt 1 ] && C_RESULTS=1
  C_NOTIFY=$( ( MAX_TOTAL *  8 / 100 )); [ "$C_NOTIFY"  -lt 1 ] && C_NOTIFY=1
  C_DEFAULT=$(( MAX_TOTAL *  9 / 100 )); [ "$C_DEFAULT" -lt 1 ] && C_DEFAULT=1

  # Distribute any rounding remainder to harvest
  ALLOCATED=$(( C_HARVEST + C_LIVE + C_EVARB + C_RESULTS + C_NOTIFY + C_DEFAULT ))
  REMAINDER=$(( MAX_TOTAL - ALLOCATED ))
  C_HARVEST=$(( C_HARVEST + REMAINDER ))

  # Absolute ceilings so a single queue can't monopolise a big machine
  [ "$C_HARVEST" -gt 4 ] && C_HARVEST=4
  [ "$C_LIVE"    -gt 2 ] && C_LIVE=2
  [ "$C_EVARB"   -gt  1 ] && C_EVARB=1
  [ "$C_RESULTS" -gt 1 ] && C_RESULTS=1
  [ "$C_NOTIFY"  -gt  2 ] && C_NOTIFY=1
  [ "$C_DEFAULT" -gt  2 ] && C_DEFAULT=2
}

print_resource_summary() {
  allocate_concurrency
  echo ""
  cyan "  ┌─ System resources ────────────────────────────────┐"
  cyan "  │  CPU cores:       ${CPU_CORES}"
  cyan "  │  Total RAM:       ${MEM_MB} MB"
  cyan "  │  Available RAM:   ${MEM_AVAIL_MB} MB"
  cyan "  │  Worker budget:   ${MEM_BUDGET_MB} MB  (~${MB_PER_WORKER} MB/worker)"
  cyan "  │  Max workers:     ${MAX_TOTAL}  (by CPU: $((CPU_CORES*2))  by RAM: ${MAX_BY_MEM})"
  cyan "  └───────────────────────────────────────────────────┘"
  echo ""
  cyan "  Allocated concurrency:"
  cyan "    harvest  → $C_HARVEST"
  cyan "    live     → $C_LIVE"
  cyan "    ev_arb   → $C_EVARB"
  cyan "    results  → $C_RESULTS"
  cyan "    notify   → $C_NOTIFY"
  cyan "    default  → $C_DEFAULT"
  echo ""
}

# =============================================================================
# Worker control
# =============================================================================

start_worker() {
  local name=$1 queues=$2 concurrency=$3
  if is_running "$name"; then
    yellow "[$name] already running (pid $(cat "$(pid_file "$name")"))"
    return
  fi
  if [ "$concurrency" -lt 1 ]; then
    yellow "[$name] skipped — concurrency=0 (insufficient resources)"
    return
  fi
  celery -A "$APP" worker \
    --loglevel=info \
    -c "$concurrency" \
    -Q "$queues" \
    -n "${name}@%h" \
    --logfile="$(log_file "$name")" \
    --pidfile="$(pid_file "$name")" \
    --without-gossip \
    --without-mingle \
    --detach
  green "[$name] started — queues=[$queues] concurrency=$concurrency"
}

cmd_start() {
  print_resource_summary
  allocate_concurrency

  start_worker harvest  harvest  "$C_HARVEST"
  start_worker live     live     "$C_LIVE"
  start_worker ev_arb   ev_arb   "$C_EVARB"
  start_worker results  results  "$C_RESULTS"
  start_worker notify   notify   "$C_NOTIFY"
  start_worker default  default  "$C_DEFAULT"

  if is_running beat; then
    yellow "[beat] already running (pid $(cat "$(pid_file "beat")"))"
  else
    celery -A "$APP" beat \
      --loglevel=info \
      --logfile="$(log_file "beat")" \
      --pidfile="$(pid_file "beat")" \
      --detach
    green "[beat] started"
  fi

  echo ""
  green "All workers running. Logs → $LOG_DIR/"
}

cmd_stop() {
  for name in harvest live ev_arb results notify default beat; do
    local pf
    pf=$(pid_file "$name")
    if is_running "$name"; then
      kill "$(cat "$pf")" 2>/dev/null && rm -f "$pf"
      green "[$name] stopped"
    else
      yellow "[$name] not running"
    fi
  done
}

cmd_status() {
  allocate_concurrency
  echo ""
  printf "%-12s  %-10s  %-8s  %s\n" "WORKER" "STATUS" "PID" "TARGET CONCURRENCY"
  printf "%-12s  %-10s  %-8s  %s\n" "------" "------" "---" "------------------"

  declare -A CONC
  CONC[harvest]=$C_HARVEST
  CONC[live]=$C_LIVE
  CONC[ev_arb]=$C_EVARB
  CONC[results]=$C_RESULTS
  CONC[notify]=$C_NOTIFY
  CONC[default]=$C_DEFAULT
  CONC[beat]="1 (scheduler)"

  for name in harvest live ev_arb results notify default beat; do
    if is_running "$name"; then
      printf "%-12s  \033[32m%-10s\033[0m  %-8s  %s\n" \
        "$name" "running" "$(cat "$(pid_file "$name")")" "${CONC[$name]}"
    else
      printf "%-12s  \033[31m%-10s\033[0m  %-8s  %s\n" \
        "$name" "stopped" "-" "${CONC[$name]}"
    fi
  done

  echo ""
  cyan "  CPUs: ${CPU_CORES}  |  Available RAM: ${MEM_AVAIL_MB} MB  |  Worker slots: ${MAX_TOTAL}"
  echo ""
}

cmd_logs() {
  local target="${1:-all}"
  if [ "$target" = "all" ]; then
    tail -f \
      "$LOG_DIR/harvest.log" \
      "$LOG_DIR/live.log" \
      "$LOG_DIR/ev_arb.log" \
      "$LOG_DIR/results.log" \
      "$LOG_DIR/notify.log" \
      "$LOG_DIR/default.log" \
      "$LOG_DIR/beat.log" \
      2>/dev/null | awk '
        /==> .* <==/ { gsub(/.*\//, "", $2); gsub(/\.log/, "", $2); label=$2; next }
        { print "[" label "] " $0 }
      '
    return
  fi
  local lf
  lf=$(log_file "$target")
  if [ ! -f "$lf" ]; then
    red "No log file: $lf"
    echo "Available: harvest | live | ev_arb | results | notify | default | beat | all"
    exit 1
  fi
  green "=== $lf ==="
  tail -f "$lf"
}

# =============================================================================
case "${1:-help}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  restart) cmd_stop; sleep 2; cmd_start ;;
  status)  cmd_status ;;
  logs)    cmd_logs "${2:-all}" ;;
  plan)    print_resource_summary ;;
  *)
    echo ""
    echo "Usage: $0 {start|stop|restart|status|logs [worker|all]|plan}"
    echo ""
    echo "  start              Detect CPU/RAM, scale workers, start all + beat"
    echo "  stop               Stop everything"
    echo "  restart            Stop + start (re-detects resources)"
    echo "  status             Show running status + allocated concurrency"
    echo "  plan               Show what concurrency would be allocated (dry run)"
    echo "  logs harvest       Tail harvest worker"
    echo "  logs live          Tail live worker"
    echo "  logs ev_arb        Tail EV/Arb worker"
    echo "  logs results       Tail results worker"
    echo "  logs notify        Tail notify worker"
    echo "  logs default       Tail default worker"
    echo "  logs beat          Tail beat scheduler"
    echo "  logs all           Tail all logs merged"
    echo ""
    ;;
esac