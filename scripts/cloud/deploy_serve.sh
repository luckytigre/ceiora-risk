#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PROJECT_ID="${PROJECT_ID:-project-4e18de12-63a3-4206-aaa}"
REGION="${REGION:-us-east4}"
REPOSITORY="${REPOSITORY:-ceiora-images}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
SERVICE_NAME="${SERVICE_NAME:-ceiora-prod-serve}"
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM:-linux/amd64}"

REGISTRY_BASE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}"
SERVE_IMAGE="${SERVE_IMAGE:-${REGISTRY_BASE}/serve:${IMAGE_TAG}}"

PROJECT_ID="${PROJECT_ID}" \
REGION="${REGION}" \
REPOSITORY="${REPOSITORY}" \
IMAGE_TAG="${IMAGE_TAG}" \
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM}" \
BUILD_TARGETS="serve" \
BUILD_OUTPUT="push" \
SERVE_IMAGE="${SERVE_IMAGE}" \
./scripts/cloud/build_and_push_images.sh

gcloud run deploy "${SERVICE_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${SERVE_IMAGE}" \
  --cpu-throttling \
  --quiet

printf 'Deployed serve image %s to Cloud Run service %s (%s)\n' "${SERVE_IMAGE}" "${SERVICE_NAME}" "${REGION}"
