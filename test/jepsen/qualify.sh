#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/../.." && pwd)"
artifact_dir="$(mktemp -d "${script_dir}/.cache/qualification.XXXXXX")"

cd "${repo_dir}"

mix run test/mutation/run.exs

export GROUP_JEPSEN_SKIP_CHECKER=1

run_jepsen() {
  local expectation="$1"
  local corruption="$2"
  local log="${artifact_dir}/${expectation}-${corruption}.log"
  local status=0

  set +e
  "${script_dir}/run.sh" test \
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

  if [[ "${expectation}" == "pass" ]] && [[ "${status}" -ne 0 ]]; then
    echo "healthy Jepsen baseline failed; see ${log}" >&2
    return 1
  fi

  if [[ "${expectation}" == "fail" ]] && [[ "${status}" -eq 0 ]]; then
    echo "Jepsen checker accepted corruption ${corruption}; see ${log}" >&2
    return 1
  fi

  echo "${expectation}: ${corruption} (${log})"
}

run_jepsen pass none
run_jepsen fail unexpected-death
run_jepsen fail internal-index
run_jepsen fail cursor-marker

echo "mutation and live checker qualification passed"
