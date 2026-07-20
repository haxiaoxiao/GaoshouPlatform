#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${GAOSHOU_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BACKEND_DIR="$ROOT/backend"
FRONTEND_DIR="$ROOT/frontend"
ENV_FILE="${GAOSHOU_ENV_FILE:-}"
PYTHON="${GAOSHOU_PYTHON:-$BACKEND_DIR/.venv/bin/python}"
LOG_DIR="$ROOT/logs"
NO_PAUSE="${GAOSHOU_SKIP_PAUSE:-0}"
SKIP_OPTIONAL_CHECKS="${GAOSHOU_SKIP_OPTIONAL_CHECKS:-0}"

if [[ -z "$ENV_FILE" ]]; then
  if [[ -f "$ROOT/.env.local" ]]; then
    ENV_FILE="$ROOT/.env.local"
  else
    ENV_FILE="$ROOT/.env"
  fi
fi

load_env_file() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line//[[:space:]]/}" || "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" == *"="* ]] || continue
    local key="${line%%=*}"
    local value="${line#*=}"
    key="$(printf '%s' "$key" | xargs)"
    value="$(printf '%s' "$value" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    value="${value%\"}"
    value="${value#\"}"
    value="${value%\'}"
    value="${value#\'}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    export "$key=$value"
  done < "$file"
}

load_env_file "$ENV_FILE"

BACKEND_HOST="${GAOSHOU_BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${GAOSHOU_BACKEND_PORT:-${BACKEND_PORT:-8800}}"
SYNC_HOST="${GAOSHOU_SYNC_HOST:-127.0.0.1}"
SYNC_PORT="${GAOSHOU_SYNC_PORT:-${SYNC_SERVICE_PORT:-${SYNC_PORT:-8810}}}"
FRONTEND_HOST="${GAOSHOU_FRONTEND_HOST:-127.0.0.1}"
FRONTEND_PORT="${GAOSHOU_FRONTEND_PORT:-${FRONTEND_PORT:-3511}}"
MARKET_DATA_BACKEND="${MARKET_DATA_BACKEND:-parquet}"
REDIS_PORT="${REDIS_PORT:-16379}"
LIVE_TRADING_ENABLE_ORDER_SUBMIT="${LIVE_TRADING_ENABLE_ORDER_SUBMIT:-false}"
LIVE_TRADING_AUTO_EXECUTE_ENABLED="${LIVE_TRADING_AUTO_EXECUTE_ENABLED:-false}"
SYNC_SERVICE_URL="http://$SYNC_HOST:$SYNC_PORT"
export SYNC_SERVICE_URL SYNC_SERVICE_PORT="$SYNC_PORT"

BACKEND_URL="http://$BACKEND_HOST:$BACKEND_PORT/health"
SYNC_URL="http://$SYNC_HOST:$SYNC_PORT/health"
FRONTEND_URL="http://$FRONTEND_HOST:$FRONTEND_PORT"

pause_if_needed() {
  if [[ "$NO_PAUSE" != "1" && "${1:-}" != "--no-pause" ]]; then
    read -r -p "Press Enter to continue..." _
  fi
}

pids_for_port() {
  lsof -nP -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null || true
}

stop_project_processes() {
  local port pid command
  for port in "$BACKEND_PORT" "$SYNC_PORT" "$FRONTEND_PORT"; do
    while IFS= read -r pid; do
      [[ -n "$pid" ]] || continue
      command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
      if [[ "$command" =~ uvicorn[[:space:]]+app\.(main|sync_main):app || "$command" =~ npm[[:space:]]+run[[:space:]]+dev || "$command" =~ node_modules/.bin/vite || "$command" =~ vite/bin/vite ]]; then
        kill "$pid" 2>/dev/null || true
      fi
    done < <(pids_for_port "$port")
  done
}

assert_ports_free() {
  local port busy=0
  for port in "$BACKEND_PORT" "$SYNC_PORT" "$FRONTEND_PORT"; do
    if [[ -n "$(pids_for_port "$port")" ]]; then
      echo "      ERROR: port $port is still listening"
      busy=1
    fi
  done
  [[ "$busy" == "0" ]]
}

resolve_frontend_port() {
  local preferred="$FRONTEND_PORT"
  local candidate
  for candidate in "$preferred" $(seq 3511 3599); do
    if [[ -z "$(pids_for_port "$candidate")" ]]; then
      FRONTEND_PORT="$candidate"
      FRONTEND_URL="http://$FRONTEND_HOST:$FRONTEND_PORT"
      mkdir -p "$ROOT/.runtime"
      printf '%s\n' "$FRONTEND_PORT" >"$ROOT/.runtime/frontend-port.txt"
      [[ "$candidate" == "$preferred" ]] || echo "      WARN: frontend port $preferred is unavailable; using $candidate."
      return 0
    fi
  done
  return 1
}

wait_http_ok() {
  local url="$1"
  local timeout="${2:-60}"
  local deadline=$((SECONDS + timeout))
  until curl -fsS --max-time 2 "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 1
  done
}

wait_service_http_ok() {
  local name="$1"
  local url="$2"
  local logfile="$3"
  local timeout="${4:-60}"
  local pidfile="$LOG_DIR/$name.pid"
  local pid
  local deadline=$((SECONDS + timeout))
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  until curl -fsS --max-time 2 "$url" >/dev/null 2>&1; do
    if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
      echo "      ERROR: $name process exited before health check passed."
      echo "      Last log lines:"
      tail -n 40 "$logfile" 2>/dev/null || true
      return 1
    fi
    if (( SECONDS >= deadline )); then
      echo "      ERROR: $name health check timed out: $url"
      echo "      Last log lines:"
      tail -n 40 "$logfile" 2>/dev/null || true
      return 1
    fi
    sleep 1
  done
}

start_service() {
  local name="$1"
  local cwd="$2"
  local logfile="$3"
  local pidfile="$LOG_DIR/$name.pid"
  local pid
  shift 3
  echo "      log: $logfile"
  (
    cd "$cwd"
    nohup "$@" >"$logfile" 2>&1 &
    echo "$!" >"$pidfile"
  )
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  echo "      $name PID: ${pid:-unknown}"
}

echo "========================================"
echo "  GaoshouPlatform Startup (macOS)"
echo "========================================"
echo "Root:      $ROOT"
echo "Env file:  $ENV_FILE"
echo "Backend:   http://$BACKEND_HOST:$BACKEND_PORT"
echo "Sync:      http://$SYNC_HOST:$SYNC_PORT"
echo "Frontend:  $FRONTEND_URL"
echo "Data mode: $MARKET_DATA_BACKEND  storage=Parquet/DuckDB"
[[ -n "${GAOSHOU_DATA_DIR:-}" ]] && echo "Data root: $GAOSHOU_DATA_DIR"
[[ -n "${PARQUET_DATA_DIR:-}" ]] && echo "Parquet:   $PARQUET_DATA_DIR"
[[ -n "${DATABASE_URL:-}" ]] && echo "SQLite:   $DATABASE_URL"
QMT_ACCOUNT_STATUS="not configured"
[[ -n "${QMT_ACCOUNT_ID:-}" ]] && QMT_ACCOUNT_STATUS="configured"
echo "miniQMT:   account $QMT_ACCOUNT_STATUS  order_submit=$LIVE_TRADING_ENABLE_ORDER_SUBMIT  auto_execute=$LIVE_TRADING_AUTO_EXECUTE_ENABLED"
echo

if [[ ! -d "$ROOT" ]]; then
  echo "[ERROR] Project root not found: $ROOT"
  pause_if_needed "${1:-}"
  exit 1
fi
if [[ ! -x "$PYTHON" ]]; then
  echo "[ERROR] Backend Python not found or not executable: $PYTHON"
  pause_if_needed "${1:-}"
  exit 1
fi
if [[ ! -f "$FRONTEND_DIR/package.json" ]]; then
  echo "[ERROR] Frontend package.json not found: $FRONTEND_DIR/package.json"
  pause_if_needed "${1:-}"
  exit 1
fi

mkdir -p "$LOG_DIR"

echo "[1/8] Stopping stale project processes on configured ports..."
stop_project_processes
sleep 1
if ! resolve_frontend_port; then
  echo "      ERROR: no usable frontend port in 3511..3599"
  pause_if_needed "${1:-}"
  exit 1
fi
if ! assert_ports_free; then
  pause_if_needed "${1:-}"
  exit 1
fi
echo "      OK"

echo "[2/8] Optional Redis handling..."
if [[ "$SKIP_OPTIONAL_CHECKS" == "1" || "${GAOSHOU_SKIP_DOCKER:-0}" == "1" ]]; then
  echo "      SKIP: optional Redis/Docker checks disabled for this startup."
elif command -v docker >/dev/null 2>&1; then
  if docker start redis-server >/dev/null 2>&1 || docker run -d --name redis-server -p "$REDIS_PORT:6379" redis:7-alpine >/dev/null 2>&1; then
    echo "      OK"
  else
    echo "      WARN: Redis is not running. Continue without Redis."
  fi
else
  echo "      WARN: Docker not found. Continue without Redis."
fi

echo "[3/8] Market data storage..."
case "$MARKET_DATA_BACKEND" in
  parquet|PARQUET|Parquet) ;;
  *) echo "      WARN: MARKET_DATA_BACKEND=$MARKET_DATA_BACKEND is ignored; Parquet/DuckDB is the only supported backend." ;;
esac
if [[ -z "${GAOSHOU_DATA_DIR:-}" ]]; then
  echo "      WARN: GAOSHOU_DATA_DIR is not configured in $ENV_FILE."
elif [[ ! -d "$GAOSHOU_DATA_DIR" ]]; then
  echo "      WARN: GAOSHOU_DATA_DIR does not exist: $GAOSHOU_DATA_DIR"
fi
if [[ -z "${PARQUET_DATA_DIR:-}" ]]; then
  echo "      WARN: PARQUET_DATA_DIR is not configured in $ENV_FILE."
elif [[ ! -d "$PARQUET_DATA_DIR" ]]; then
  echo "      WARN: PARQUET_DATA_DIR does not exist: $PARQUET_DATA_DIR"
fi
if [[ -n "${FACTOR_VALUE_STORE_DIR:-}" && ! -f "$FACTOR_VALUE_STORE_DIR/_manifest.json" ]]; then
  echo "      ERROR: FACTOR_VALUE_STORE_DIR has no validated manifest: $FACTOR_VALUE_STORE_DIR"
  pause_if_needed "${1:-}"
  exit 1
fi
echo "      OK: Parquet/DuckDB mode"

echo "[4/8] Applying database migrations..."
(
  cd "$BACKEND_DIR"
  "$PYTHON" -m alembic -c alembic.ini upgrade head
) || {
  echo "      ERROR: Database migration failed. Backend services were not started."
  pause_if_needed "${1:-}"
  exit 1
}
echo "      OK"

echo "[5/8] Starting sync service on $SYNC_HOST:$SYNC_PORT..."
start_service "sync" "$BACKEND_DIR" "$LOG_DIR/sync-service.log" "$PYTHON" -m uvicorn app.sync_main:app --host "$SYNC_HOST" --port "$SYNC_PORT"
if ! wait_service_http_ok "sync" "$SYNC_URL" "$LOG_DIR/sync-service.log" 60; then
  pause_if_needed "${1:-}"
  exit 1
fi
echo "      OK"

echo "[6/8] Starting backend on $BACKEND_HOST:$BACKEND_PORT..."
start_service "backend" "$BACKEND_DIR" "$LOG_DIR/backend.log" "$PYTHON" -m uvicorn app.main:app --host "$BACKEND_HOST" --port "$BACKEND_PORT"
if ! wait_service_http_ok "backend" "$BACKEND_URL" "$LOG_DIR/backend.log" 60; then
  pause_if_needed "${1:-}"
  exit 1
fi
echo "      OK"

echo "[7/8] Checking miniQMT live-trading bridge..."
if [[ -z "${QMT_ACCOUNT_ID:-}" ]]; then
  echo "      SKIP: miniQMT account is optional and QMT_ACCOUNT_ID is not configured."
elif [[ -z "${QMT_TRADER_PATH:-}" ]]; then
  echo "      SKIP: miniQMT account is optional and QMT_TRADER_PATH is not configured."
else
  echo "      OPTIONAL: miniQMT account config found. Open the miniQMT client before using /live."
  echo "      OPTIONAL: status can be checked at http://$BACKEND_HOST:$BACKEND_PORT/api/live-trading/status"
fi

echo "[8/8] Building and starting frontend on $FRONTEND_HOST:$FRONTEND_PORT..."
(
  cd "$FRONTEND_DIR"
  npm run build
  export VITE_API_PROXY_TARGET="http://$BACKEND_HOST:$BACKEND_PORT"
  nohup npm run preview -- --host "$FRONTEND_HOST" --port "$FRONTEND_PORT" --strictPort >"$LOG_DIR/frontend.log" 2>&1 &
  echo "      frontend PID: $!"
  echo "      log: $LOG_DIR/frontend.log"
)
if ! wait_http_ok "$FRONTEND_URL" 60; then
  echo "      ERROR: Frontend did not bind to $FRONTEND_URL"
  pause_if_needed "${1:-}"
  exit 1
fi
echo "      OK"

echo
echo "========================================"
echo "  Startup complete"
echo "========================================"
echo "Backend docs:  http://$BACKEND_HOST:$BACKEND_PORT/docs"
echo "Backend API:   http://$BACKEND_HOST:$BACKEND_PORT/api/system/status"
echo "Sync health:   http://$SYNC_HOST:$SYNC_PORT/health"
echo "Live trading:  $FRONTEND_URL/trade"
echo "Frontend:      $FRONTEND_URL"
echo
pause_if_needed "${1:-}"
