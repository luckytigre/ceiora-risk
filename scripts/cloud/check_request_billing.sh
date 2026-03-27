#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-project-4e18de12-63a3-4206-aaa}"
REGION="${REGION:-us-east4}"

if ! command -v gcloud >/dev/null 2>&1; then
  printf 'gcloud is required\n' >&2
  exit 1
fi

if [[ "$#" -gt 0 ]]; then
  services=("$@")
else
  services=(
    "ceiora-prod-control"
    "ceiora-prod-serve"
    "ceiora-prod-frontend"
  )
fi

failed=0

for service in "${services[@]}"; do
  value="$(gcloud run services describe "${service}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --format='value(spec.template.metadata.annotations.[run.googleapis.com/cpu-throttling])')"

  if [[ "${value}" != "true" ]]; then
    printf 'FAIL  %-24s cpu-throttling=%s\n' "${service}" "${value:-<unset>}" >&2
    failed=1
    continue
  fi

  printf 'OK    %-24s cpu-throttling=%s\n' "${service}" "${value}"
done

exit "${failed}"
