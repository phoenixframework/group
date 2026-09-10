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

      ["member_counts"] ->
        GroupBench.Local.run_member_counts()

      ["distributed"] ->
        GroupBench.Distributed.run()

      _ ->
        IO.puts("""
        Usage: GroupBench.main(["local" | "member_counts" | "distributed"])

          local        — Run single-node benchmarks
          member_counts — Run 1K/100K/1M materialized count read benchmarks
          distributed  — Coordinator: connects to replicas, drives benchmarks
        """)
    end
  end
end
