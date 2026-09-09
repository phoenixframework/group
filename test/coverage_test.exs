defmodule Group.CoverageTest do
  use ExUnit.Case

  alias Group.TestCluster

  @tag skip: is_nil(Process.whereis(:cover_server))
  test "peer-only execution is retained after the peer stops" do
    before_calls = log_level_calls()
    peers = TestCluster.start_peers(1)
    on_exit(fn -> TestCluster.stop_peers(peers) end)
    [{_, node} = peer] = peers
    name = :coverage_peer

    assert :cover_compiled = TestCluster.rpc!(node, :code, :which, [Group])
    TestCluster.start_group(node, name: name, shards: 1)
    TestCluster.rpc!(node, Group, :log_level, [name, false])
    TestCluster.stop_peer(peer)

    TestCluster.assert_eventually(fn -> node not in :cover.which_nodes() end)
    assert log_level_calls() == before_calls + 1
  end

  defp log_level_calls do
    {:ok, calls} = :cover.analyse(Group, :calls, :function)
    {{Group, :log_level, 2}, count} = List.keyfind(calls, {Group, :log_level, 2}, 0)
    count
  end
end
