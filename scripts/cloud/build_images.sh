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
BUILD_OUTPUT="${BUILD_OUTPUT:-load}"
BUILD_TARGETS="${BUILD_TARGETS:-frontend serve control}"
NORMALIZED_BUILD_TARGETS=" $(printf '%s' "${BUILD_TARGETS}" | tr ',' ' ') "

REGISTRY_BASE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}"
FRONTEND_IMAGE="${FRONTEND_IMAGE:-${REGISTRY_BASE}/frontend:${IMAGE_TAG}}"
SERVE_IMAGE="${SERVE_IMAGE:-${REGISTRY_BASE}/serve:${IMAGE_TAG}}"
CONTROL_IMAGE="${CONTROL_IMAGE:-${REGISTRY_BASE}/control:${IMAGE_TAG}}"

case "${BUILD_OUTPUT}" in
  load)
    OUTPUT_FLAG="--load"
    ;;
  push)
    OUTPUT_FLAG="--push"
    ;;
  *)
    echo "BUILD_OUTPUT must be 'load' or 'push'" >&2
    exit 1
    ;;
esac

build_target() {
  local target="$1"
  case "${NORMALIZED_BUILD_TARGETS}" in
    *" ${target} "*) return 0 ;;
    *) return 1 ;;
  esac
}

BUILT_IMAGES=()

if build_target frontend; then
  docker buildx build \
    --platform "${CLOUD_RUN_PLATFORM}" \
    ${OUTPUT_FLAG} \
    --build-arg "BACKEND_API_ORIGIN=${BACKEND_API_ORIGIN}" \
    -f frontend/Dockerfile \
    -t "${FRONTEND_IMAGE}" \
    .
  BUILT_IMAGES+=("${FRONTEND_IMAGE}")
fi

if build_target serve; then
  docker buildx build \
    --platform "${CLOUD_RUN_PLATFORM}" \
    ${OUTPUT_FLAG} \
    -f backend/Dockerfile.serve \
    -t "${SERVE_IMAGE}" \
    .
  BUILT_IMAGES+=("${SERVE_IMAGE}")
fi

if build_target control; then
  docker buildx build \
    --platform "${CLOUD_RUN_PLATFORM}" \
    ${OUTPUT_FLAG} \
    -f backend/Dockerfile.control \
    -t "${CONTROL_IMAGE}" \
    .
  BUILT_IMAGES+=("${CONTROL_IMAGE}")
fi

printf 'Built images for %s (%s):\n' "${CLOUD_RUN_PLATFORM}" "${BUILD_OUTPUT}"
printf -- '- %s\n' "${BUILT_IMAGES[@]}"
