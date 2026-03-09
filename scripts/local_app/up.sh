#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

log "Starting local app from clean state"
stop_all
: >"${BACKEND_LOG}"
: >"${FRONTEND_LOG}"
start_backend
start_frontend
log "Local app is up"
print_status
