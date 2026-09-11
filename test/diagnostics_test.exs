defmodule Group.DiagnosticsTest do
  # These checks temporarily change a process-wide environment variable.
  use ExUnit.Case, async: false

  @moduletag :local
  @moduletag :tmp_dir
  @moduletag :capture_log

  setup %{tmp_dir: directory} do
    previous = System.get_env("GROUP_TEST_DIAGNOSTICS")
    System.put_env("GROUP_TEST_DIAGNOSTICS", directory)

    on_exit(fn ->
      if previous do
        System.put_env("GROUP_TEST_DIAGNOSTICS", previous)
      else
        System.delete_env("GROUP_TEST_DIAGNOSTICS")
      end
    end)

    :ok
  end

  test "retains suite seeds and failure details", %{tmp_dir: directory} do
    Group.TestDiagnostics.handle_cast({:suite_started, [seed: 12345]}, [])

    failed = %ExUnit.Test{
      name: :synthetic_failure,
      module: __MODULE__,
      state: {:failed, [{:error, %RuntimeError{message: "diagnostic fixture"}, []}]}
    }

    Group.TestDiagnostics.handle_cast({:test_finished, failed}, [])
    [suite] = Path.wildcard(Path.join(directory, "*-suite.txt"))
    [failure] = Path.wildcard(Path.join(directory, "*-failure.txt"))
    assert File.read!(suite) =~ "12345"
    assert File.read!(failure) =~ "diagnostic fixture"
  end

  test "snapshot includes topology, configuration, shard state and queue sizes" do
    name = :"diagnostics_#{System.unique_integer([:positive])}"
    start_supervised!({Group, name: name, shards: 1, log: false})
    snapshot = Group.TestDiagnostics.snapshot()
    assert snapshot.node == node()
    assert snapshot.connected_nodes == Node.list()
    group = Enum.find(snapshot.groups, &(&1.name == name))
    assert group.config.num_shards == 1
    [shard] = group.shards
    assert shard.process[:message_queue_len] >= 0
    assert shard.state =~ "pending_replicated_registry_len"
    assert shard.state =~ "remote_shards"
    assert snapshot.tables != []
  end

  test "unreachable peers produce diagnostics instead of hanging", %{tmp_dir: directory} do
    peer = :"unreachable_diagnostic_#{System.unique_integer([:positive])}@127.0.0.1"
    Group.TestDiagnostics.capture_peers([{nil, peer}])
    [snapshot] = Path.wildcard(Path.join(directory, "*-peer_snapshot.txt"))
    assert File.read!(snapshot) =~ "unavailable"
  end
end
