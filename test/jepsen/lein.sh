#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cache_dir="${script_dir}/.cache"
lein="${cache_dir}/lein"

mkdir -p "${cache_dir}"

if [[ ! -x "${lein}" ]]; then
  curl --fail --silent --show-error --location \
    https://raw.githubusercontent.com/technomancy/leiningen/2.12.0/bin/lein \
    --output "${lein}"
  chmod +x "${lein}"
fi

export LEIN_HOME="${cache_dir}/lein-home"
cd "${script_dir}"

exec "${lein}" "$@"
