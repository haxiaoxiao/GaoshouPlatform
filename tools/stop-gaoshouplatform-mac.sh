#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${GAOSHOU_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
ENV_FILE="${GAOSHOU_ENV_FILE:-}"
NO_PAUSE="${GAOSHOU_SKIP_PAUSE:-0}"
STOP_REDIS=0

if [[ -z "$ENV_FILE" ]]; then
  if [[ -f "$ROOT/.env.local" ]]; then
    ENV_FILE="$ROOT/.env.local"
  else
    ENV_FILE="$ROOT/.env"
  fi
fi

for arg in "$@"; do
  case "$arg" in
    --no-pause) NO_PAUSE=1 ;;
    --stop-redis) STOP_REDIS=1 ;;
  esac
done

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

BACKEND_PORT="${GAOSHOU_BACKEND_PORT:-${BACKEND_PORT:-8800}}"
SYNC_PORT="${GAOSHOU_SYNC_PORT:-${SYNC_SERVICE_PORT:-${SYNC_PORT:-8810}}}"
FRONTEND_PORT="${GAOSHOU_FRONTEND_PORT:-${FRONTEND_PORT:-3511}}"

pause_if_needed() {
  if [[ "$NO_PAUSE" != "1" ]]; then
    read -r -p "Press Enter to continue..." _
  fi
}

pids_for_port() {
  lsof -nP -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null || true
}

echo "========================================"
echo "  GaoshouPlatform Shutdown (macOS)"
echo "========================================"
echo "Root:      $ROOT"
echo "Env file:  $ENV_FILE"
echo "Ports:     backend=$BACKEND_PORT sync=$SYNC_PORT frontend=$FRONTEND_PORT"
echo

echo "[1/3] Stopping backend/sync/frontend processes on configured ports..."
for port in "$BACKEND_PORT" "$SYNC_PORT" "$FRONTEND_PORT"; do
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    if [[ "$command" =~ uvicorn[[:space:]]+app\.(main|sync_main):app || "$command" =~ npm[[:space:]]+run[[:space:]]+dev || "$command" =~ node_modules/.bin/vite || "$command" =~ vite/bin/vite ]]; then
      echo "      Killing PID $pid"
      kill "$pid" 2>/dev/null || true
    fi
  done < <(pids_for_port "$port")
done
sleep 1
echo "      OK"

echo "[2/3] Docker service handling..."
if command -v docker >/dev/null 2>&1; then
  if [[ "$STOP_REDIS" == "1" ]]; then
    if docker stop redis-server >/dev/null 2>&1; then
      echo "      Redis stopped"
    else
      echo "      Redis was not running"
    fi
  else
    echo "      Redis left running. Use --stop-redis to stop it."
  fi
else
  echo "      Docker not found. Skip Docker containers."
fi

echo "[3/3] Verifying configured ports..."
busy=0
for port in "$BACKEND_PORT" "$SYNC_PORT" "$FRONTEND_PORT"; do
  if [[ -n "$(pids_for_port "$port")" ]]; then
    echo "      ERROR: port $port is still listening"
    busy=1
  fi
done
if [[ "$busy" != "0" ]]; then
  echo "      One or more configured ports are still occupied."
  pause_if_needed
  exit 1
fi
echo "      Done"
echo "      miniQMT client is external and was left running."

echo
echo "========================================"
echo "  Shutdown complete"
echo "========================================"
echo
pause_if_needed
