#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${TLA_JAR:-}" ]]; then
  echo "TLA_JAR must point to tla2tools.jar" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
metadir="${repo_root}/tmp/tlc"
config="${TLA_CONFIG:-${repo_root}/test/formal/GroupAntiEntropy.cfg}"
mkdir -p "${metadir}"

exec java -XX:+UseParallelGC -cp "${TLA_JAR}" tlc2.TLC \
  -cleanup \
  -metadir "${metadir}" \
  -workers "${TLC_WORKERS:-4}" \
  -config "${config}" \
  "${repo_root}/test/formal/GroupAntiEntropy.tla"
