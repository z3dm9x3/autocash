#!/usr/bin/env bash
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
if [ ! -f "$HERE/add_account.py" ]; then
    echo "ERROR: add_account.py not found next to this file."
    exit 1
fi
if [ ! -x "$ROOT/.venv/bin/python" ]; then
    python3 -m venv "$ROOT/.venv" || { echo "Install: sudo apt install python3-venv"; exit 1; }
    "$ROOT/.venv/bin/python" -m pip install -r "$ROOT/requirements.txt"
fi
exec "$ROOT/.venv/bin/python" "$HERE/add_account.py"
