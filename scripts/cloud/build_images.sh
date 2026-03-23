#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PROJECT_ID="${PROJECT_ID:-project-4e18de12-63a3-4206-aaa}"
REGION="${REGION:-us-east4}"
REPOSITORY="${REPOSITORY:-ceiora-images}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
BACKEND_API_ORIGIN="${BACKEND_API_ORIGIN:-https://api.ceiora.com}"

REGISTRY_BASE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}"
FRONTEND_IMAGE="${FRONTEND_IMAGE:-${REGISTRY_BASE}/frontend:${IMAGE_TAG}}"
SERVE_IMAGE="${SERVE_IMAGE:-${REGISTRY_BASE}/serve:${IMAGE_TAG}}"
CONTROL_IMAGE="${CONTROL_IMAGE:-${REGISTRY_BASE}/control:${IMAGE_TAG}}"

docker build \
  --build-arg "BACKEND_API_ORIGIN=${BACKEND_API_ORIGIN}" \
  -f frontend/Dockerfile \
  -t "${FRONTEND_IMAGE}" \
  .

docker build \
  -f backend/Dockerfile.serve \
  -t "${SERVE_IMAGE}" \
  .

docker build \
  -f backend/Dockerfile.control \
  -t "${CONTROL_IMAGE}" \
  .

printf 'Built images:\n- %s\n- %s\n- %s\n' "${FRONTEND_IMAGE}" "${SERVE_IMAGE}" "${CONTROL_IMAGE}"
