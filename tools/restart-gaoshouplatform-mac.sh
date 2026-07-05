#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${GAOSHOU_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
STOP="$ROOT/tools/stop-gaoshouplatform-mac.sh"
START="$ROOT/tools/start-gaoshouplatform-mac.sh"

echo "========================================"
echo "  GaoshouPlatform Restart (macOS)"
echo "========================================"
echo

if [[ ! -x "$STOP" ]]; then
  echo "[ERROR] $STOP not found or not executable"
  exit 1
fi
if [[ ! -x "$START" ]]; then
  echo "[ERROR] $START not found or not executable"
  exit 1
fi

echo "Stopping..."
"$STOP" --no-pause
echo
sleep 2
echo "Starting..."
"$START" --no-pause
