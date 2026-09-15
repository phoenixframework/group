Code.require_file("jepsen/repair_coverage.exs", __DIR__)

defmodule Group.JepsenRepairCoverageTest do
  use ExUnit.Case, async: false

  alias Group.TestCluster, as: Cluster
  alias Group.JepsenRepairProbe, as: Probe
  alias Group.Jepsen.RepairCoverage, as: Coverage
  alias Group.Jepsen.Transport.Stats
  alias Group.Replica.Data

  @moduletag :capture_log
  @moduletag tmp_dir: "repair_coverage_#{System.pid()}"
  @moduletag timeout: 120_000

  test "restart truncates an interrupted append before accepting new evidence", %{tmp_dir: dir} do
    path = Path.join(dir, "interrupted")
    committed = "multi_chunk_snapshot_committed\t1\n"

    for suffix <- ["applied_delta_run_records_peak\t", "applied_delta_run_records_peak\t99"] do
      File.write!(path, committed <> suffix)
      start_supervised!({Coverage, path: path})
      assert Coverage.snapshot() == %{multi_chunk_snapshot_committed: 1}
      assert File.read!(path) == committed

      Coverage.observe(:delta, 0, 2)
      expected = %{multi_chunk_snapshot_committed: 1, applied_delta_run_records_peak: 2}
      assert Coverage.snapshot() == expected
      assert File.read!(path) == committed <> "applied_delta_run_records_peak\t2\n"

      stop_supervised!(Coverage)
      start_supervised!({Coverage, path: path})
      assert Coverage.snapshot() == expected
      stop_supervised!(Coverage)
    end
  end

  test "malformed committed records fail without truncating the log", %{tmp_dir: dir} do
    path = Path.join(dir, "malformed")
    contents = "applied_delta_run_records_peak\t2\tunexpected\npartial"
    File.write!(path, contents)
    assert {:error, {reason, stack}} = GenServer.start(Coverage, path: path)
    assert %MatchError{} = Exception.normalize(:error, reason, stack)
    assert File.read!(path) == contents
  end

  for transport <- [
        Group.Jepsen.Transport.Distribution,
        Group.Jepsen.Transport.TCP,
        Group.Jepsen.Transport.Chaos
      ] do
    test "#{transport}: only receiver commits certify repair", %{tmp_dir: dir} do
      transport = unquote(transport)
      peers = Cluster.start_peers(2, schedulers: 2)
      on_exit(fn -> Cluster.stop_peers(peers) end)
      [{_, source}, {_, receiver}] = peers

      for {node, suffix} <- [{source, "source"}, {receiver, "receiver"}] do
        :ok = rpc(node, Probe, :boot, [Path.join(dir, suffix)])
      end

      name = :coverage_probe

      for node <- [source, receiver] do
        {:ok, _} =
          Cluster.start_group(node,
            name: name,
            shards: 1,
            log: false,
            replica_transport: transport,
            replicated_anti_entropy_interval: 60_000,
            replicated_peer_lease_timeout: 120_000,
            replicated_sender_buffer_size: 1
          )
      end

      Cluster.assert_eventually(fn ->
        receiver in rpc(source, Group, :nodes, [name]) and
          source in rpc(receiver, Group, :nodes, [name])
      end)

      rpc(source, Stats, :block, [receiver])
      rpc(receiver, Stats, :block, [source])
      pid = Cluster.spawn_join(source, name, "one", %{})
      Cluster.flush_shards(source, name)
      stream = rpc(source, Data, :local_stream_id, [name, 0, nil])
      version = Group.Replica.WireProtocol.version()
      mutation = fn key -> {:join, nil, key, pid, %{}, 1, :normal, source} end
      records = [{1, [mutation.("one")]}, {2, [mutation.("two")]}]
      delta = {:delta_batch, version, [{stream, 1, records, 2}]}
      chunk1 = {:snapshot_chunk, version, stream, 4, 1, [], [{"one", pid, %{}, 1}]}
      chunk2 = {:snapshot_chunk, version, stream, 4, 2, [], [{"two", pid, %{}, 1}]}
      commit = {:snapshot_commit, version, stream, 4, 2, 0, 2}

      # Every live wrapper's logical gate reports attempts without delegating.
      for message <- [delta, chunk1, chunk2, commit] do
        assert :ok = rpc(source, transport, :outgoing, [name, receiver, 0, message, []])
      end

      assert events(receiver, name) == %{}
      assert rpc(source, Coverage, :snapshot, []) == %{}
      attempted = rpc(source, Stats, :snapshot, [])
      assert attempted.attempted_delta_run_records_peak == 2
      assert attempted.attempted_snapshot_chunks_peak == 2

      # Bypass the logical gate, but still use the actual selected data lane.
      rpc(source, Stats, :unblock, [receiver])
      stale_generation = Group.Replica.WireProtocol.new_generation()
      stale = stream |> put_elem(2, stale_generation) |> put_elem(5, stale_generation)
      assert Group.Replica.WireProtocol.valid_stream_id?(stale)
      rejected = {:delta_batch, version, [{stale, 1, records, 2}]}
      rejected_snapshot = Enum.map([chunk1, chunk2, commit], &put_elem(&1, 2, stale))
      send_frames(source, receiver, name, transport, [rejected | rejected_snapshot])
      assert events(receiver, name) == %{}

      # A gap and a rejected mutation cannot inflate the actual contiguous run.
      gap = {:delta_batch, version, [{stream, 2, [List.last(records)], 2}]}
      invalid = {:delta_batch, version, [{stream, 1, [{1, [:invalid]}], 1}]}
      send_frames(source, receiver, name, transport, [gap, invalid])
      assert events(receiver, name) == %{}

      # One accepted record followed by duplicate-only traffic is not a run of two.
      single = {:delta_batch, version, [{stream, 1, [hd(records)], 1}]}

      deliver_until(source, receiver, name, transport, [single], fn ->
        rpc(receiver, Data, :replica_cursor, [name, 0, stream]) == 1
      end)

      send_frames(source, receiver, name, transport, [single, single])
      assert events(receiver, name) == %{applied_delta_run_records_peak: 1}

      # Two new records in one run really advance two, excluding the duplicate prefix.
      run = {:delta_batch, version, [{stream, 1, records ++ [{3, [mutation.("three")]}], 3}]}

      deliver_until(source, receiver, name, transport, [run], fn ->
        rpc(receiver, Data, :replica_cursor, [name, 0, stream]) == 3
      end)

      assert events(receiver, name) == %{applied_delta_run_records_peak: 2}
      assert [{^pid, %{}}] = rpc(receiver, Group, :members, [name, "three"])

      # All chunks without a terminal manifest are still uncommitted.
      send_frames(source, receiver, name, transport, [chunk1, chunk2, chunk1])
      assert rpc(receiver, Data, :replica_cursor, [name, 0, stream]) == 3
      assert events(receiver, name) == %{applied_delta_run_records_peak: 2}

      # Supersede the provisional transfer. Commit alone, incomplete assembly,
      # duplicate chunks, and a stale terminal frame preserve the old slice.
      chunk1 = put_elem(chunk1, 3, 5)
      chunk2 = put_elem(chunk2, 3, 5)
      commit = put_elem(commit, 3, 5)
      send_frames(source, receiver, name, transport, [commit, chunk1, chunk1])
      assert rpc(receiver, Data, :replica_cursor, [name, 0, stream]) == 3
      assert events(receiver, name) == %{applied_delta_run_records_peak: 2}
      send_frames(source, receiver, name, transport, [put_elem(commit, 2, stale)])
      assert events(receiver, name) == %{applied_delta_run_records_peak: 2}

      deliver_until(source, receiver, name, transport, [chunk2, chunk1, commit], fn ->
        rpc(receiver, Data, :replica_cursor, [name, 0, stream]) == 5
      end)

      assert rpc(receiver, Group, :members, [name, "three"]) == []
      assert [{^pid, %{}}] = rpc(receiver, Group, :members, [name, "two"])

      assert rpc(receiver, :sys, :get_state, [Group.Replica.shard_name(name, 0)]).snapshot_transfers ==
               %{}

      evidence = events(receiver, name)

      emitted =
        rpc(receiver, Stats, :snapshot, [])
        |> Map.take([:applied_delta_run_records_peak, :multi_chunk_snapshot_committed])

      assert emitted == evidence
      encoded = rpc(receiver, Group.Jepsen.EDN, :encode, [emitted])
      assert encoded == String.trim(File.read!("test/jepsen/fixtures/applied-repair-events.edn"))
      send_frames(source, receiver, name, transport, [run, chunk1, chunk2, commit])
      assert events(receiver, name) == evidence

      # The durable emitted evidence, not transient sender stats, survives restart.
      rpc(receiver, GenServer, :stop, [Coverage])
      rpc(receiver, Probe, :start_coverage, [Path.join(dir, "receiver")])
      assert rpc(receiver, Coverage, :snapshot, []) == evidence

      # A genuinely new BEAM loads the same completed evidence. Keep at most two
      # peers alive, and never recompile an instrumented module under a live Group.
      Cluster.stop_peers(peers)
      replacement = Cluster.start_peers(1, schedulers: 2)
      on_exit(fn -> Cluster.stop_peers(replacement) end)
      [{_, restarted}] = replacement
      rpc(restarted, Probe, :boot, [Path.join(dir, "receiver")])
      assert rpc(restarted, Coverage, :snapshot, []) == evidence

      # A new history clears persisted evidence even if the VM is already up.
      File.rm!(Path.join(dir, "receiver"))
      assert rpc(restarted, Coverage, :snapshot, []) == %{}
    end
  end

  defp rpc(node, module, function, args), do: :erpc.call(node, module, function, args)

  defp events(node, name) do
    rpc(node, Probe, :barrier, [name])
    rpc(node, Coverage, :snapshot, [])
  end

  defp send_frames(source, receiver, name, transport, frames) do
    for frame <- frames do
      rpc(source, transport, :outgoing, [name, receiver, 0, frame, []])
    end

    # TCP/outbox and chaos forwarding is asynchronous. This bound is only for
    # negative observations; positive assertions below always await exact state.
    Process.sleep(150)
    rpc(receiver, Probe, :barrier, [name])
  end

  defp deliver_until(source, receiver, name, transport, frames, predicate) do
    Cluster.assert_eventually(fn ->
      send_frames(source, receiver, name, transport, frames)
      predicate.()
    end)
  end
end
