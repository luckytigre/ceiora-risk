#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PROJECT_ID="${PROJECT_ID:-project-4e18de12-63a3-4206-aaa}"
REGION="${REGION:-us-east4}"
REPOSITORY="${REPOSITORY:-ceiora-images}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM:-linux/amd64}"
ENDPOINT_MODE="${ENDPOINT_MODE:-custom_domains}"

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

validate_build_targets() {
  local seen=0
  local target
  for target in ${BUILD_TARGETS//,/ }; do
    seen=1
    case "${target}" in
      frontend|serve|control)
        ;;
      *)
        echo "BUILD_TARGETS must contain only 'frontend', 'serve', or 'control'." >&2
        exit 1
        ;;
    esac
  done

  if [[ "${seen}" -eq 0 ]]; then
    echo "BUILD_TARGETS must include at least one of 'frontend', 'serve', or 'control'." >&2
    exit 1
  fi
}

require_origin_for_frontend_build() {
  if ! build_target frontend; then
    return 0
  fi

  case "${ENDPOINT_MODE}" in
    custom_domains)
      if [[ -n "${BACKEND_API_ORIGIN:-}" && "${BACKEND_API_ORIGIN}" != "https://api.ceiora.com" ]]; then
        echo "ENDPOINT_MODE=custom_domains requires BACKEND_API_ORIGIN=https://api.ceiora.com. Use ENDPOINT_MODE=run_app for explicit run.app frontend builds." >&2
        exit 1
      fi
      ;;
    run_app)
      if [[ -z "${BACKEND_API_ORIGIN:-}" ]]; then
        echo "ENDPOINT_MODE=run_app requires explicit BACKEND_API_ORIGIN for frontend builds." >&2
        exit 1
      fi
      if [[ ! "${BACKEND_API_ORIGIN}" =~ ^https://[^/]+\.run\.app$ ]]; then
        echo "ENDPOINT_MODE=run_app requires BACKEND_API_ORIGIN to be an absolute https://<service>.run.app origin with no path." >&2
        exit 1
      fi
      ;;
  esac
}

case "${ENDPOINT_MODE}" in
  custom_domains|run_app)
    ;;
  *)
    echo "ENDPOINT_MODE must be 'custom_domains' or 'run_app'" >&2
    exit 1
    ;;
esac

validate_build_targets
require_origin_for_frontend_build

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

PROJECT_ID="${PROJECT_ID}" \
REGION="${REGION}" \
REPOSITORY="${REPOSITORY}" \
IMAGE_TAG="${IMAGE_TAG}" \
BACKEND_API_ORIGIN="${BACKEND_API_ORIGIN:-}" \
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM}" \
ENDPOINT_MODE="${ENDPOINT_MODE}" \
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

printf 'Pushed images for %s (endpoint_mode=%s):\n' "${CLOUD_RUN_PLATFORM}" "${ENDPOINT_MODE}"
printf -- '- %s\n' "${PUSHED_IMAGES[@]}"
