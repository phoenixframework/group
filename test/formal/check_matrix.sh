#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${TLA_JAR:?TLA_JAR must point to tla2tools.jar}"

run_check() {
  local spec="$1"
  local config="$2"

  TLA_SPEC="${script_dir}/${spec}.tla" \
    TLA_CONFIG="${script_dir}/${config}.cfg" \
    "${script_dir}/check.sh"
}

run_check GroupAntiEntropy GroupAntiEntropy
run_check SnapshotAssembly SnapshotAssembly
run_check PeerEviction PeerEviction
run_check AuthorityProjection AuthorityProjection
run_check AuthorityHint AuthorityHint

if [[ "${TLA_EXTENDED:-0}" == "1" ]]; then
  run_check GroupAntiEntropy GroupAntiEntropyExtended
fi
