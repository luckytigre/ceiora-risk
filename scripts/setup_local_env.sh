#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv_local"
INSTALL_LSEG_RUNTIME="${INSTALL_LSEG_RUNTIME:-1}"

python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel

if [[ "$INSTALL_LSEG_RUNTIME" == "1" ]]; then
  if ! "$VENV_DIR/bin/python" -m pip install -e "$ROOT_DIR/backend[dev,lseg]"; then
    "$VENV_DIR/bin/python" -m pip install -e "$ROOT_DIR/backend[dev]"
    cat <<'EOF'
warning: failed to install backend.[lseg] into .venv_local
The local backend can still run serving-only workflows, but LSEG-backed ingest/repair
lanes will fail until `lseg-data` is installed successfully in .venv_local.
EOF
  fi
else
  "$VENV_DIR/bin/python" -m pip install -e "$ROOT_DIR/backend[dev]"
fi

(cd "$ROOT_DIR/frontend" && npm install)

cat <<EOF
local environment ready: $VENV_DIR
activate with: source "$VENV_DIR/bin/activate"
EOF
