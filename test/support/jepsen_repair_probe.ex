defmodule Group.JepsenRepairProbe do
  @moduledoc false

  def boot(path) do
    System.put_env("GROUP_JEPSEN_LIBRARY_ONLY", "1")
    Code.require_file("../jepsen/node.exs", __DIR__)
    :ok = apply(Group.Jepsen.RepairCoverage, :install!, [])
    start_coverage(path)
  end

  def start_coverage(path) do
    {:ok, pid} = apply(Group.Jepsen.RepairCoverage, :start_link, [[path: path]])
    Process.unlink(pid)
    :ok
  end

  def barrier(name) do
    :sys.get_state(Group.Replica.shard_name(name, 0))
    # Calls from the shard order its earlier coverage casts ahead of this reply.
    :sys.replace_state(Group.Replica.shard_name(name, 0), fn state ->
      apply(Group.Jepsen.RepairCoverage, :snapshot, [])
      state
    end)

    :ok
  end
end
