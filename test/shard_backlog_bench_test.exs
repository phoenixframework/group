defmodule Group.ShardBacklogBenchTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 3_600_000

  alias Group.{ShardBacklog, TestCluster}

  # Prints how a replica shard's cost per replicated batch changes with the
  # number of batches queued behind it:
  #
  #     GROUP_BACKLOG_BENCH=1 mix test.exunit test/shard_backlog_bench_test.exs
  #
  # GROUP_BACKLOG_SIZES and GROUP_BACKLOG_SAMPLE override the backlog sizes and
  # the number of batches measured at the head of each backlog,
  # GROUP_BACKLOG_RECORDS the delta records per batch, and GROUP_BACKLOG_MODES
  # the modes: plain, interleaved (a local request every ten batches), live
  # (a caller joining and leaving throughout), and profile (call-time tracing).
  @tag skip: System.get_env("GROUP_BACKLOG_BENCH") != "1"
  test "cost per replicated batch against backlog depth" do
    peers = TestCluster.start_peers(2, schedulers: 4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)
    [{_, source}, {_, receiver}] = peers

    sizes =
      "GROUP_BACKLOG_SIZES"
      |> System.get_env("1000,10000,100000,200000")
      |> String.split(",")
      |> Enum.map(&String.to_integer/1)

    sample = String.to_integer(System.get_env("GROUP_BACKLOG_SAMPLE", "1000"))
    modes = System.get_env("GROUP_BACKLOG_MODES", "plain,interleaved,live,profile")

    for mode <- String.split(modes, ","), size <- sizes do
      measured = min(sample, size)
      row = run(source, receiver, mode, size, measured)
      per_batch = div(row.micros * 1_000, measured)
      reductions = div(row.reductions, measured)

      IO.puts(
        "mode=#{mode} backlog=#{size} measured=#{measured} " <>
          "ns_per_batch=#{per_batch} reductions_per_batch=#{reductions} " <>
          inspect(Map.drop(row, [:micros, :reductions, :call_time]))
      )

      for {micros, count, mfa} <- Map.get(row, :call_time, []) do
        IO.puts(
          "    #{String.pad_leading(to_string(micros), 10)} us  #{count} calls  #{inspect(mfa)}"
        )
      end
    end
  end

  defp run(source, receiver, mode, size, measured) do
    name = :"shard_backlog_#{System.unique_integer([:positive])}"

    groups =
      for node <- [source, receiver] do
        {:ok, pid} =
          TestCluster.start_group(node,
            name: name,
            shards: 1,
            replicated_anti_entropy_interval: 600_000,
            replicated_peer_lease_timeout: 1_200_000
          )

        {node, pid}
      end

    TestCluster.assert_eventually(
      fn ->
        receiver in TestCluster.rpc!(source, Group, :nodes, [name]) and
          source in TestCluster.rpc!(receiver, Group, :nodes, [name])
      end,
      timeout: 10_000
    )

    stream = TestCluster.rpc!(source, Group.Replica.Data, :local_stream_id, [name, 0, nil])
    member = TestCluster.rpc!(source, ShardBacklog, :spawn_member, [])
    records = String.to_integer(System.get_env("GROUP_BACKLOG_RECORDS", "1"))
    opts = if mode == "interleaved", do: [local_every: 10], else: []
    opts = [records_per_batch: records] ++ opts

    result =
      :erpc.call(
        receiver,
        ShardBacklog,
        :run,
        [mode, name, source, stream, member, size, measured * records, opts],
        3_600_000
      )

    for {node, pid} <- groups do
      TestCluster.rpc!(node, ShardBacklog, :stop_group, [pid])
    end

    result
  end
end
