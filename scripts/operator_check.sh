#!/usr/bin/env bash
set -euo pipefail

BACKEND_ORIGIN="${BACKEND_ORIGIN:-http://127.0.0.1:8000}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

fetch_json() {
  local url="$1"
  local out="$2"
  curl -fsS "$url" > "$out"
}

fetch_json "${BACKEND_ORIGIN}/api/health" "$TMP_DIR/health.json"
fetch_json "${BACKEND_ORIGIN}/api/operator/status" "$TMP_DIR/operator.json"

python3 - "$TMP_DIR/health.json" "$TMP_DIR/operator.json" <<'PY'
import json, sys
health = json.load(open(sys.argv[1], 'r', encoding='utf-8'))
operator = json.load(open(sys.argv[2], 'r', encoding='utf-8'))

errors = []
health_status = str(health.get('status') or '')
operator_status = str(operator.get('status') or '')
neon = operator.get('neon_sync_health') or {}
runtime = operator.get('runtime') or {}
core_due = operator.get('core_due') or {}
lanes = operator.get('lanes') or []
neon_enabled = bool(runtime.get('neon_auto_sync_enabled'))

if health_status != 'ok':
    errors.append(f"api health={health_status}")
if operator_status != 'ok':
    errors.append(f"operator status={operator_status}")
if neon_enabled and str(neon.get('status') or '') != 'ok':
    errors.append(f"neon_sync_health={neon.get('status')}")
if not lanes:
    errors.append('operator lanes missing')
if neon_enabled and not (operator.get('latest_parity_artifact') or neon.get('artifact_path')):
    errors.append('latest parity artifact missing')

print('Operator check summary')
print(f"- api health: {health_status}")
print(f"- operator status: {operator_status}")
print(f"- neon health: {neon.get('status')} (enabled={neon_enabled})")
print(f"- core due: {core_due.get('due')} ({core_due.get('reason')})")
print(f"- parity artifact: {operator.get('latest_parity_artifact') or neon.get('artifact_path')}")
for lane in lanes:
    latest = lane.get('latest_run') or {}
    print(f"- lane {lane.get('profile')}: {latest.get('status')} :: {latest.get('finished_at') or latest.get('updated_at')}")

if errors:
    print('\nFAIL')
    for err in errors:
        print(f"- {err}")
    raise SystemExit(1)
print('\nPASS')
PY
