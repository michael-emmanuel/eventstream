#!/usr/bin/env bash
# Local multi-broker cluster without Docker: 1 controller + N brokers as
# plain processes. Data, logs and PID files live under ./data.
#
#   scripts/cluster.sh start [N]        build and start controller + N brokers (default 3)
#   scripts/cluster.sh stop             graceful stop (SIGTERM)
#   scripts/cluster.sh status           which processes are running
#   scripts/cluster.sh kill BROKER_ID   SIGKILL a broker (crash, no clean shutdown)
#   scripts/cluster.sh pause BROKER_ID  SIGSTOP a broker (frozen process: GC pause / hung disk)
#   scripts/cluster.sh resume BROKER_ID SIGCONT
#   scripts/cluster.sh restart BROKER_ID
#   scripts/cluster.sh kill-controller  SIGKILL the controller
#   scripts/cluster.sh start-controller start the controller again (state reloads from disk)
#   scripts/cluster.sh logs NAME        tail a log (controller | broker-1 | ...)
#   scripts/cluster.sh clean            stop and delete all data
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${DATA:-$ROOT/data}"
BIN="$ROOT/bin"
CTRL_PORT="${CTRL_PORT:-9000}"
BASE_PORT="${BASE_PORT:-9090}"   # broker N listens on BASE_PORT+N
SESSION="${SESSION_TIMEOUT:-6s}"
LEASE="${LEASE:-4s}"
mkdir -p "$DATA/logs" "$DATA/pids"

build() {
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/...)
}

start_controller() {
  "$BIN/controller" -addr ":$CTRL_PORT" -advertise "http://localhost:$CTRL_PORT" \
    -data "$DATA/controller" -session-timeout "$SESSION" -lease "$LEASE" \
    >>"$DATA/logs/controller.log" 2>&1 &
  echo $! >"$DATA/pids/controller"
  echo "controller  pid $!  http://localhost:$CTRL_PORT"
}

start_broker() {
  local id=$1
  local port=$((BASE_PORT + id))
  "$BIN/broker" -id "$id" -addr ":$port" -advertise "http://localhost:$port" \
    -data "$DATA/broker-$id" -controller "http://localhost:$CTRL_PORT" \
    -maintenance-interval 10s ${BROKER_FLAGS:-} \
    >>"$DATA/logs/broker-$id.log" 2>&1 &
  echo $! >"$DATA/pids/broker-$id"
  echo "broker-$id    pid $!  http://localhost:$port"
}

pid_of() { cat "$DATA/pids/$1" 2>/dev/null || true; }

wait_ready() {
  for _ in $(seq 1 50); do
    if curl -sf "http://localhost:$CTRL_PORT/v1/cluster" >/dev/null 2>&1; then
      local alive
      alive=$(curl -s "http://localhost:$CTRL_PORT/v1/cluster" | grep -c '"alive": true' || true)
      if [ "$alive" -ge "$1" ]; then echo "cluster ready: $alive brokers alive"; return 0; fi
    fi
    sleep 0.2
  done
  echo "cluster did not become ready; see $DATA/logs" >&2
  return 1
}

cmd="${1:-help}"
case "$cmd" in
  start)
    n="${2:-3}"
    build
    start_controller
    sleep 0.5
    for id in $(seq 1 "$n"); do start_broker "$id"; done
    wait_ready "$n"
    brokers=""
    for id in $(seq 1 "$n"); do brokers="$brokers${brokers:+,}http://localhost:$((BASE_PORT + id))"; done
    echo "export STREAM_BROKERS=$brokers"
    ;;
  stop)
    for f in "$DATA"/pids/*; do
      [ -e "$f" ] || continue
      pid=$(cat "$f"); kill -CONT "$pid" 2>/dev/null || true; kill "$pid" 2>/dev/null || true
      rm -f "$f"
    done
    sleep 1; echo "stopped"
    ;;
  status)
    for f in "$DATA"/pids/*; do
      [ -e "$f" ] || continue
      pid=$(cat "$f"); name=$(basename "$f")
      if kill -0 "$pid" 2>/dev/null; then echo "$name running (pid $pid)"; else echo "$name DOWN"; fi
    done
    ;;
  kill)
    pid=$(pid_of "broker-$2"); kill -9 "$pid" && echo "broker-$2 killed (SIGKILL, pid $pid)"
    ;;
  pause)
    pid=$(pid_of "broker-$2"); kill -STOP "$pid" && echo "broker-$2 paused"
    ;;
  resume)
    pid=$(pid_of "broker-$2"); kill -CONT "$pid" && echo "broker-$2 resumed"
    ;;
  restart)
    pid=$(pid_of "broker-$2"); kill "$pid" 2>/dev/null || true; sleep 1
    start_broker "$2"
    ;;
  kill-controller)
    pid=$(pid_of controller); kill -9 "$pid" && echo "controller killed (pid $pid)"
    ;;
  start-controller)
    start_controller
    ;;
  logs)
    tail -f "$DATA/logs/$2.log"
    ;;
  clean)
    "$0" stop || true
    rm -rf "$DATA"
    echo "data removed"
    ;;
  *)
    sed -n '2,17p' "$0"
    ;;
esac
