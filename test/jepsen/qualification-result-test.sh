#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/qualification-result.sh"
work="$(mktemp -d "${script_dir}/qualification-test.XXXXXX")"
trap 'rm -rf "${work}"' EXIT

probe() {
  local want="$1" expectation="$2" mode="$3" status="$4" valid="$5" qualified="$6"
  local result="${work}/result"
  rm -f "${result}"
  # A bounded stand-in for the CLI: no Docker, JVM or network required.
  if [[ "${valid}" != absent ]]; then
    printf 'group-qualification-v1\t%s\t%s\t%s\n' "${mode}" "${valid}" "${qualified}" >"${result}"
  fi
  local actual=reject
  if qualification_result "${expectation}" "${mode}" "${status}" "${result}"; then
    actual=accept
  fi
  [[ "${actual}" == "${want}" ]] || { echo "unexpected classification"; exit 1; }
}

probe accept pass none 0 true true
for mode in unexpected-death internal-index cursor-marker registry-projection terminal-unavailable; do
  probe accept fail "${mode}" 1 false true
  probe reject fail "${mode}" 1 false false
done
probe reject pass none 1 false false
probe reject pass none 1 absent false
probe reject fail internal-index 0 true true
for status in 1 124 125 137 127; do
  probe reject fail internal-index "${status}" absent false
done
probe reject fail internal-index 124 false true
probe reject fail internal-index 125 false true
echo "qualification status regression passed"
