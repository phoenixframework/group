defmodule Group.Replica.Protocol do
  @moduledoc false

  @version 2

  def version, do: @version

  def stream_id(name, origin_node, origin_generation, shard, cluster, cluster_epoch) do
    {name, origin_node, origin_generation, shard, cluster, cluster_epoch}
  end

  def stream_name({name, _origin_node, _generation, _shard, _cluster, _epoch}), do: name

  def stream_origin({_name, origin_node, _generation, _shard, _cluster, _epoch}),
    do: origin_node

  def stream_generation({_name, _origin_node, generation, _shard, _cluster, _epoch}),
    do: generation

  def stream_shard({_name, _origin_node, _generation, shard, _cluster, _epoch}), do: shard

  def stream_cluster({_name, _origin_node, _generation, _shard, cluster, _epoch}), do: cluster

  def stream_epoch({_name, _origin_node, _generation, _shard, _cluster, epoch}), do: epoch

  def op_cluster({:register, cluster, _key, _pid, _meta, _time, _node}), do: cluster
  def op_cluster({:unregister, cluster, _key, _pid, _meta, _reason}), do: cluster
  def op_cluster({:join, cluster, _key, _pid, _meta, _time, _reason, _node}), do: cluster
  def op_cluster({:leave, cluster, _key, _pid, _meta, _reason}), do: cluster
end
