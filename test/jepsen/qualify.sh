#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/../.." && pwd)"
artifact_dir="$(mktemp -d "${script_dir}/.cache/qualification.XXXXXX")"
source "${script_dir}/qualification-result.sh"

cd "${repo_dir}"

mix run test/mutation/run.exs

export GROUP_JEPSEN_SKIP_CHECKER=1

run_jepsen() {
  local expectation="$1"
  local corruption="$2"
  local log="${artifact_dir}/${expectation}-${corruption}.log"
  local result="${artifact_dir}/${expectation}-${corruption}.result"
  local status=0

  set +e
  GROUP_JEPSEN_QUALIFICATION_RESULT="${result}" \
    timeout --signal=TERM --kill-after=30 180 "${script_dir}/run.sh" test \
    --no-ssh \
    --nodes n1,n2,n3 \
    --concurrency 2n \
    --time-limit 6 \
    --fault-interval 1 \
    --recovery-time 5 \
    --transport distribution \
    --scenario mixed \
    --corruption "${corruption}" >"${log}" 2>&1
  status=$?
  set -e

  if ! qualification_result "${expectation}" "${corruption}" "${status}" "${result}"; then
    echo "see ${log} and ${result}" >&2
    return 1
  fi

  echo "${expectation}: ${corruption} (${log})"
}

run_jepsen pass none
run_jepsen fail unexpected-death
run_jepsen fail internal-index
run_jepsen fail cursor-marker
run_jepsen fail registry-projection
run_jepsen fail terminal-unavailable

echo "mutation and live checker qualification passed"
