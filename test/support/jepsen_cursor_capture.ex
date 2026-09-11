defmodule Group.JepsenCursorCapture do
  @moduledoc false
  @compile {:no_warn_undefined,
            [
              Group.Jepsen.Driver,
              Group.Jepsen.Driver.Supervisor,
              Group.Jepsen.Transport.Stats,
              Group.Jepsen.Snapshot
            ]}
  alias Group.Replica.{Data, WireProtocol}

  def start(path) do
    System.put_env("GROUP_JEPSEN_LIBRARY", "1")
    Code.require_file(path)

    for starter <- [
          fn -> Group.Jepsen.Transport.Stats.start_link([]) end,
          &start_group/0,
          fn ->
            Group.Jepsen.Driver.Supervisor.start_link(
              node_id: Atom.to_string(node()),
              boot_id: "cursor-capture"
            )
          end
        ] do
      {:ok, pid} = starter.()
      Process.unlink(pid)
    end

    :ok = Group.connect(:jepsen_group, ["red"])
  end

  defp start_group do
    Group.start_link(
      name: :jepsen_group,
      shards: 2,
      log: false,
      replicated_anti_entropy_interval: 50,
      replicated_peer_lease_timeout: 60_000
    )
  end

  def stop_group do
    %{status: :ok} = Group.Jepsen.Driver.kill("owner")
    Supervisor.stop(:jepsen_group_group_sup)
  end

  def restart do
    :ok = stop_group()
    {:ok, pid} = start_group()
    Process.unlink(pid)
    :ok
  end

  def snapshot(retired \\ []) do
    Group.Jepsen.Snapshot.capture(Atom.to_string(node()), "cursor-capture", 1, ["red"], retired).snapshot
  end

  def write do
    %{status: :ok} = Group.Jepsen.Driver.mutate(:register, "owner", nil, 0, 1)
    :ok
  end

  def freeze do
    for shard <- 0..1, do: :sys.suspend(Group.Replica.shard_name(:jepsen_group, shard))
    :ok
  end

  def thaw do
    for shard <- 0..1, do: :sys.resume(Group.Replica.shard_name(:jepsen_group, shard))
    :ok
  end

  def stream(cluster, shard) do
    WireProtocol.stream_id(
      :jepsen_group,
      node(),
      Data.generation(:jepsen_group),
      shard,
      cluster,
      Data.local_cluster_epoch(:jepsen_group, cluster)
    )
  end

  def set_cursor(stream, position) do
    table = Data.replica_cursor_table(:jepsen_group, WireProtocol.stream_shard(stream))
    old = :ets.lookup(table, stream)

    if position == :missing,
      do: :ets.delete(table, stream),
      else: :ets.insert(table, {stream, position})

    old
  end

  def restore_cursor(stream, old) do
    set_cursor(stream, :missing)
    table = Data.replica_cursor_table(:jepsen_group, WireProtocol.stream_shard(stream))
    :ets.insert(table, old)
    :ok
  end
end
