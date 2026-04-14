# Rollback Procedure: Revert to Local Authority

This document defines the procedure to revert the cUSE/cPAR system from Cloud-Native (Neon) to Local Authority (SQLite) in the event of severe cloud-side corruption, schema mismatch, or availability failure.

## 1. Objective
Re-establish Shaun's machine (or a local-ingest host) as the single source of truth for both ingest and serving, bypassing Neon.

## 2. Procedure

### 2A. Environment Configuration
On the local-ingest host, update the `.env` or shell environment:

```bash
# 1. Revert authority to SQLite
export DATA_BACKEND=sqlite
export NEON_AUTHORITATIVE_REBUILDS=false

# 2. Enable local-ingest role
export APP_RUNTIME_ROLE=local-ingest
export ORCHESTRATOR_ENABLE_INGEST=true
```

### 2B. Data Reconciliation
Ensure the local `data.db` and `cache.db` are healthy. If the local state is behind, perform a full re-ingest from the last known-good LSEG anchor:

```bash
# Run a core rebuild locally to re-establish SQLite authority
python3 -m backend.orchestration.run_model_pipeline \
  --profile cold-core \
  --as-of-date <LAST_STABLE_DATE> \
  --force-core
```

### 2C. Frontend/API Pointing
If the frontend was configured to talk to Cloud Run, it must be pointed back to the local API:

```bash
# In frontend/.env.local
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

## 3. Post-Rollback Verification
1. Run `make operator-check` locally.
2. Verify holdings and risk views in the dashboard.
3. Once stable, investigate the Neon corruption/mismatch before attempting to re-enable Cloud Cutover.

## 4. Recovery (Return to Cloud)
Do not re-enable `DATA_BACKEND=neon` until:
1. The root cause of the rollback is identified and fixed.
2. `make neon-parity-audit` (if available) or `make cloud-topology-check` returns Green.
3. A new `cold-core` run is successfully pushed from local to Neon.
