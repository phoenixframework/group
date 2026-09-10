#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose_file="${script_dir}/docker-compose.yml"

if [[ "${GROUP_JEPSEN_SKIP_CHECKER:-0}" != "1" ]]; then
  "${script_dir}/checker.sh"
fi

cleanup() {
  if [[ "${GROUP_JEPSEN_KEEP_CONTAINERS:-0}" != "1" ]]; then
    docker compose --file "${compose_file}" down --volumes >/dev/null
  fi
}

trap cleanup EXIT

transport="${GROUP_JEPSEN_TRANSPORT:-distribution}"
args=("$@")

for ((index = 0; index < ${#args[@]}; index++)); do
  if [[ "${args[index]}" == "--transport" ]] && ((index + 1 < ${#args[@]})); then
    transport="${args[index + 1]}"
  fi
done

export GROUP_JEPSEN_TRANSPORT="${transport}"

docker compose --file "${compose_file}" up --detach --build --force-recreate

if [[ "$#" -eq 0 ]]; then
  set -- test \
    --no-ssh \
    --nodes n1,n2,n3 \
    --concurrency 2n \
    --time-limit 60 \
    --transport "${transport}"
fi

"${script_dir}/lein.sh" run -- "$@"
