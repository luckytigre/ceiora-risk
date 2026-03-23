#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PROJECT_ID="${PROJECT_ID:-project-4e18de12-63a3-4206-aaa}"
REGION="${REGION:-us-east4}"
REPOSITORY="${REPOSITORY:-ceiora-images}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
BACKEND_API_ORIGIN="${BACKEND_API_ORIGIN:-https://api.ceiora.com}"
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM:-linux/amd64}"

REGISTRY_BASE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}"
FRONTEND_IMAGE="${FRONTEND_IMAGE:-${REGISTRY_BASE}/frontend:${IMAGE_TAG}}"
SERVE_IMAGE="${SERVE_IMAGE:-${REGISTRY_BASE}/serve:${IMAGE_TAG}}"
CONTROL_IMAGE="${CONTROL_IMAGE:-${REGISTRY_BASE}/control:${IMAGE_TAG}}"
BUILD_TARGETS="${BUILD_TARGETS:-frontend serve control}"
NORMALIZED_BUILD_TARGETS=" $(printf '%s' "${BUILD_TARGETS}" | tr ',' ' ') "

build_target() {
  local target="$1"
  case "${NORMALIZED_BUILD_TARGETS}" in
    *" ${target} "*) return 0 ;;
    *) return 1 ;;
  esac
}

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

PROJECT_ID="${PROJECT_ID}" \
REGION="${REGION}" \
REPOSITORY="${REPOSITORY}" \
IMAGE_TAG="${IMAGE_TAG}" \
BACKEND_API_ORIGIN="${BACKEND_API_ORIGIN}" \
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM}" \
BUILD_TARGETS="${BUILD_TARGETS}" \
BUILD_OUTPUT="push" \
FRONTEND_IMAGE="${FRONTEND_IMAGE}" \
SERVE_IMAGE="${SERVE_IMAGE}" \
CONTROL_IMAGE="${CONTROL_IMAGE}" \
./scripts/cloud/build_images.sh

PUSHED_IMAGES=()
if build_target frontend; then
  PUSHED_IMAGES+=("${FRONTEND_IMAGE}")
fi
if build_target serve; then
  PUSHED_IMAGES+=("${SERVE_IMAGE}")
fi
if build_target control; then
  PUSHED_IMAGES+=("${CONTROL_IMAGE}")
fi

printf 'Pushed images for %s:\n' "${CLOUD_RUN_PLATFORM}"
printf -- '- %s\n' "${PUSHED_IMAGES[@]}"
