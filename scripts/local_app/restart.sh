#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/down.sh"
"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/up.sh"
