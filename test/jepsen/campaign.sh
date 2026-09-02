#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/../.." && pwd)"

time_limit="${GROUP_JEPSEN_CAMPAIGN_TIME:-300}"
test_count="${GROUP_JEPSEN_CAMPAIGN_COUNT:-20}"
concurrency="${GROUP_JEPSEN_CAMPAIGN_CONCURRENCY:-4n}"
owner_count="${GROUP_JEPSEN_CAMPAIGN_OWNERS:-128}"
key_count="${GROUP_JEPSEN_CAMPAIGN_KEYS:-32}"
recovery_time="${GROUP_JEPSEN_CAMPAIGN_RECOVERY:-15}"
profile_grace="${GROUP_JEPSEN_CAMPAIGN_PROFILE_GRACE:-90}"
artifact_dir="${GROUP_JEPSEN_CAMPAIGN_ARTIFACT_DIR:-}"

cd "${repo_dir}"

if [[ "${GROUP_JEPSEN_SKIP_CHECKER:-0}" != "1" ]]; then
  "${script_dir}/checker.sh"
fi

export GROUP_JEPSEN_SKIP_CHECKER=1
mkdir -p "${script_dir}/.cache"

if [[ -z "${artifact_dir}" ]]; then
  artifact_dir="$(mktemp -d "${script_dir}/.cache/campaign.XXXXXX")"
else
  mkdir -p "${artifact_dir}"
fi

echo "Jepsen campaign artifacts: ${artifact_dir}"

for transport in distribution tcp chaos; do
  for scenario in mixed permanent; do
    log="${artifact_dir}/${transport}-${scenario}.log"
    sender_buffer_size=1
    min_delta_run_records=1

    if [[ "${transport}/${scenario}" == "chaos/mixed" ]]; then
      sender_buffer_size=32
      min_delta_run_records=2
    fi

    echo "Starting ${transport}/${scenario}: ${test_count} histories x ${time_limit}s (sender buffer ${sender_buffer_size})"

    # Jepsen's active generator is time-limited, but setup, recovery, checker,
    # and bugs in a terminal generator live outside that limit. Keep the
    # nightly gate itself bounded as a final defense against hung campaigns.
    profile_timeout=$((test_count * (time_limit + recovery_time + profile_grace)))

    if GROUP_JEPSEN_SENDER_BUFFER_SIZE="${sender_buffer_size}" timeout \
        --signal=TERM --kill-after=30 "${profile_timeout}" \
        "${script_dir}/run.sh" test \
        --no-ssh \
        --nodes n1,n2,n3 \
        --concurrency "${concurrency}" \
        --time-limit "${time_limit}" \
        --test-count "${test_count}" \
        --key-count "${key_count}" \
        --owner-count "${owner_count}" \
        --fault-interval 2 \
        --recovery-time "${recovery_time}" \
        --min-delta-run-records "${min_delta_run_records}" \
        --transport "${transport}" \
        --scenario "${scenario}" >"${log}" 2>&1; then
      valid_count="$(rg -c "Everything looks good" "${log}" || true)"

      if [[ "${valid_count}" != "${test_count}" ]]; then
        echo "Failed ${transport}/${scenario}: expected ${test_count} valid histories, found ${valid_count:-0}" >&2
        tail -200 "${log}" >&2
        exit 1
      fi

      echo "Passed ${transport}/${scenario}: ${valid_count}/${test_count} valid histories"
    else
      status=$?
      echo "Failed ${transport}/${scenario}; tail of ${log}:" >&2
      tail -200 "${log}" >&2
      exit "${status}"
    fi
  done
done

echo "Jepsen campaign passed: ${artifact_dir}"
