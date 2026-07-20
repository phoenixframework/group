defmodule GroupBench do
  @moduledoc """
  Benchmark suite for Group — local and distributed.

  Usage:
    # Local benchmarks (no distribution needed)
    cd priv/group/priv/bench && mix run -e "GroupBench.main([\"local\"])"

    # Distributed benchmarks (bash script handles all 3 VMs)
    cd priv/group/priv/bench && ./run_distributed.sh
  """

  def main(args) do
    case args do
      ["local"] ->
        GroupBench.Local.run()

      ["distributed"] ->
        GroupBench.Distributed.run()

      ["pubsub_single_shard"] ->
        GroupBench.Distributed.run_pubsub_single_shard_only()

      _ ->
        IO.puts("""
        Usage: GroupBench.main(["local" | "distributed" | "pubsub_single_shard"])

          local                — Run single-node benchmarks
          distributed          — Coordinator: connects to replicas, drives benchmarks
          pubsub_single_shard  — Focused dispatch/broadcast fan-out benchmark
        """)
    end
  end
end
