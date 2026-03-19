#!/usr/bin/env bash
# Usage:
#   ./celery_workers.sh start
#   ./celery_workers.sh stop
#   ./celery_workers.sh restart
#   ./celery_workers.sh status
#   ./celery_workers.sh logs harvest|live|ev_arb|results|notify|default|beat|all

APP="app.workers.celery_tasks"
LOG_DIR="logs/celery"
PID_DIR="run/celery"

mkdir -p "$LOG_DIR" "$PID_DIR"

green()  { echo -e "\033[32m$*\033[0m"; }
yellow() { echo -e "\033[33m$*\033[0m"; }
red()    { echo -e "\033[31m$*\033[0m"; }

pid_file() { echo "$PID_DIR/$1.pid"; }
log_file() { echo "$LOG_DIR/$1.log"; }

is_running() {
  local pf
  pf=$(pid_file "$1")
  [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null
}

start_worker() {
  local name=$1 queues=$2 concurrency=$3
  if is_running "$name"; then
    yellow "[$name] already running (pid $(cat "$(pid_file "$name")"))"
    return
  fi
  celery -A "$APP" worker \
    --loglevel=info \
    -c "$concurrency" \
    -Q "$queues" \
    -n "${name}@%h" \
    --logfile="$(log_file "$name")" \
    --pidfile="$(pid_file "$name")" \
    --detach
  green "[$name] started — queues=[$queues] concurrency=$concurrency"
}

cmd_start() {
  start_worker harvest  harvest  20
  start_worker live     live     10
  start_worker ev_arb   ev_arb   8
  start_worker results  results  4
  start_worker notify   notify   4
  start_worker default  default  2

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
  echo ""
  printf "%-12s  %-10s  %s\n" "WORKER" "STATUS" "PID"
  printf "%-12s  %-10s  %s\n" "------" "------" "---"
  for name in harvest live ev_arb results notify default beat; do
    if is_running "$name"; then
      printf "%-12s  \033[32m%-10s\033[0m  %s\n" "$name" "running" "$(cat "$(pid_file "$name")")"
    else
      printf "%-12s  \033[31m%-10s\033[0m\n" "$name" "stopped"
    fi
  done
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

case "${1:-help}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  restart) cmd_stop; sleep 2; cmd_start ;;
  status)  cmd_status ;;
  logs)    cmd_logs "${2:-all}" ;;
  *)
    echo ""
    echo "Usage: $0 {start|stop|restart|status|logs [worker|all]}"
    echo ""
    echo "  start              Start all 6 workers + beat (detached)"
    echo "  stop               Stop everything"
    echo "  restart            Stop + start"
    echo "  status             Show running status + PIDs"
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