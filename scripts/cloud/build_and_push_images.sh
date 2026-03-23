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

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

PROJECT_ID="${PROJECT_ID}" \
REGION="${REGION}" \
REPOSITORY="${REPOSITORY}" \
IMAGE_TAG="${IMAGE_TAG}" \
BACKEND_API_ORIGIN="${BACKEND_API_ORIGIN}" \
FRONTEND_IMAGE="${FRONTEND_IMAGE}" \
SERVE_IMAGE="${SERVE_IMAGE}" \
CONTROL_IMAGE="${CONTROL_IMAGE}" \
./scripts/cloud/build_images.sh

docker push "${FRONTEND_IMAGE}"
docker push "${SERVE_IMAGE}"
docker push "${CONTROL_IMAGE}"

printf 'Pushed images:\n- %s\n- %s\n- %s\n' "${FRONTEND_IMAGE}" "${SERVE_IMAGE}" "${CONTROL_IMAGE}"
