#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: bash compare_distributed.sh BASELINE_ROOT CANDIDATE_ROOT REPORTS_DIR [benchmark options]" >&2
  exit 2
fi

harness="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
baseline="$(cd "$1" && pwd)"
candidate="$(cd "$2" && pwd)"
mkdir -p "$3" "${harness}/_build"
reports="$(cd "$3" && pwd)"
shift 3

# Reuse the harness, not compiled library artifacts. A unique build root also
# prevents a later comparison against different refs from inheriting either VM.
build_root="$(mktemp -d "${harness}/_build/comparison.XXXXXX")"

for revision in baseline candidate; do
  if [[ "${revision}" == "baseline" ]]; then
    library="${baseline}"
  else
    library="${candidate}"
  fi

  GROUP_BENCH_GROUP_PATH="${library}" \
    MIX_BUILD_PATH="${build_root}/${revision}" \
    timeout 20m bash "${harness}/run_distributed.sh" "$@" 2>&1 |
    tee "${reports}/distributed-${revision}.log"
done
