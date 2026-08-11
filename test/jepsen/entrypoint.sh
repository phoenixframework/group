#!/usr/bin/env bash
set -euo pipefail

: "${GROUP_JEPSEN_NODE:?GROUP_JEPSEN_NODE is required}"

exec elixir \
  --sname group \
  --cookie group_jepsen \
  --erl "-kernel net_ticktime 2" \
  -S mix run --no-compile --no-deps-check test/jepsen/node.exs
