#!/usr/bin/env bash

# Only a normal CLI exit and a fresh, completed checker decision qualify.
# In particular timeout (124/137), timeout invocation (125), and JVM/Docker
# failures are not negative checker evidence, even if an artifact exists.
qualification_result() {
  local expectation="$1" corruption="$2" status="$3" result="$4"
  local expected_status=1 valid=false
  if [[ "${expectation}" == pass ]]; then
    expected_status=0
    valid=true
  fi
  if [[ "${status}" -ne "${expected_status}" ]] ||
     [[ ! -f "${result}" ]] ||
     [[ "$(cat "${result}")" != "$(printf 'group-qualification-v1\t%s\t%s\ttrue' "${corruption}" "${valid}")" ]]; then
    echo "Jepsen qualification failed: ${expectation}/${corruption}, exit ${status}; missing, mismatched, or unqualified checker result" >&2
    return 1
  fi
}
