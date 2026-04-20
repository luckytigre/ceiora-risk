#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PROJECT_ID="${PROJECT_ID:-project-4e18de12-63a3-4206-aaa}"
REGION="${REGION:-us-east4}"
REPOSITORY="${REPOSITORY:-ceiora-images}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM:-linux/amd64}"
BUILD_OUTPUT="${BUILD_OUTPUT:-load}"
BUILD_TARGETS="${BUILD_TARGETS:-frontend serve control}"
ENDPOINT_MODE="${ENDPOINT_MODE:-custom_domains}"
NORMALIZED_BUILD_TARGETS=" $(printf '%s' "${BUILD_TARGETS}" | tr ',' ' ') "
BACKEND_CONTEXT_PATHS=(
  "backend/pyproject.toml"
  "backend/__init__.py"
  "backend/app_factory.py"
  "backend/config.py"
  "backend/control_main.py"
  "backend/main.py"
  "backend/serve_main.py"
  "backend/trading_calendar.py"
  "backend/api"
  "backend/analytics"
  "backend/cpar"
  "backend/data"
  "backend/ops"
  "backend/orchestration"
  "backend/portfolio"
  "backend/risk_model"
  "backend/scripts"
  "backend/services"
  "backend/universe"
  "backend/vendor"
  "docs/reference/migrations/neon"
)

declare -a TEMP_CONTEXT_DIRS=()
PREPARED_CONTEXT_DIR=""
cleanup_temp_contexts() {
  [[ ${TEMP_CONTEXT_DIRS+x} != x ]] && return 0
  [[ ${#TEMP_CONTEXT_DIRS[@]} -eq 0 ]] && return 0
  local dir
  for dir in "${TEMP_CONTEXT_DIRS[@]}"; do
    [[ -n "${dir}" && -d "${dir}" ]] && rm -rf "${dir}"
  done
}
trap cleanup_temp_contexts EXIT

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

case "${ENDPOINT_MODE}" in
  custom_domains|run_app)
    ;;
  *)
    echo "ENDPOINT_MODE must be 'custom_domains' or 'run_app'" >&2
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
      BACKEND_API_ORIGIN="${BACKEND_API_ORIGIN:-https://api.ceiora.com}"
      if [[ "${BACKEND_API_ORIGIN}" != "https://api.ceiora.com" ]]; then
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

prepare_context_dir() {
  local context_kind="$1"
  local tmpdir
  tmpdir="$(mktemp -d)"
  TEMP_CONTEXT_DIRS+=("${tmpdir}")
  case "${context_kind}" in
    frontend)
      tar -C "${ROOT_DIR}" -cf - frontend | tar -C "${tmpdir}" -xf -
      ;;
    backend)
      tar -C "${ROOT_DIR}" -cf - "${BACKEND_CONTEXT_PATHS[@]}" | tar -C "${tmpdir}" -xf -
      ;;
    *)
      echo "Unsupported context kind: ${context_kind}" >&2
      exit 1
      ;;
  esac
  PREPARED_CONTEXT_DIR="${tmpdir}"
}

build_image() {
  local context_dir="$1"
  local dockerfile_path="$2"
  local image_ref="$3"
  shift 3
  docker buildx build \
    --platform "${CLOUD_RUN_PLATFORM}" \
    ${OUTPUT_FLAG} \
    "$@" \
    -f "${dockerfile_path}" \
    -t "${image_ref}" \
    "${context_dir}"
}

BUILT_IMAGES=()
FRONTEND_CONTEXT_DIR=""
BACKEND_CONTEXT_DIR=""

validate_build_targets
require_origin_for_frontend_build

if build_target frontend; then
  prepare_context_dir frontend
  FRONTEND_CONTEXT_DIR="${PREPARED_CONTEXT_DIR}"
  build_image \
    "${FRONTEND_CONTEXT_DIR}" \
    "frontend/Dockerfile" \
    "${FRONTEND_IMAGE}" \
    --build-arg "BACKEND_API_ORIGIN=${BACKEND_API_ORIGIN}"
  BUILT_IMAGES+=("${FRONTEND_IMAGE}")
fi

if build_target serve; then
  if [[ -z "${BACKEND_CONTEXT_DIR}" ]]; then
    prepare_context_dir backend
    BACKEND_CONTEXT_DIR="${PREPARED_CONTEXT_DIR}"
  fi
  build_image \
    "${BACKEND_CONTEXT_DIR}" \
    "backend/Dockerfile.serve" \
    "${SERVE_IMAGE}"
  BUILT_IMAGES+=("${SERVE_IMAGE}")
fi

if build_target control; then
  if [[ -z "${BACKEND_CONTEXT_DIR}" ]]; then
    prepare_context_dir backend
    BACKEND_CONTEXT_DIR="${PREPARED_CONTEXT_DIR}"
  fi
  build_image \
    "${BACKEND_CONTEXT_DIR}" \
    "backend/Dockerfile.control" \
    "${CONTROL_IMAGE}"
  BUILT_IMAGES+=("${CONTROL_IMAGE}")
fi

printf 'Built images for %s (%s, endpoint_mode=%s):\n' "${CLOUD_RUN_PLATFORM}" "${BUILD_OUTPUT}" "${ENDPOINT_MODE}"
printf -- '- %s\n' "${BUILT_IMAGES[@]}"
