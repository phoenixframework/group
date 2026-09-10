defmodule Group.Replica.WireProtocol do
  @moduledoc false

  @version 3

  def version, do: @version

  # The counter orders Group incarnations created within one BEAM. The ref
  # keeps the identity globally unique across BEAM restarts; Erlang
  # distribution supplies nodedown before a restarted VM can install a new
  # authority, so ordering is only required within one VM lifetime.
  def new_generation do
    {System.unique_integer([:monotonic, :positive]), make_ref()}
  end

  def valid_generation?({counter, identity}),
    do: is_integer(counter) and counter > 0 and is_reference(identity)

  def valid_generation?(_generation), do: false

  def generation_newer?({new_counter, _new_identity}, {old_counter, _old_identity})
      when is_integer(new_counter) and is_integer(old_counter),
      do: new_counter > old_counter

  def generation_newer?(_new_generation, _old_generation), do: false

  def stream_id(name, origin_node, origin_generation, shard, cluster, cluster_epoch) do
    {name, origin_node, origin_generation, shard, cluster, cluster_epoch}
  end

  def valid_stream_id?({name, origin_node, origin_generation, shard, cluster, cluster_epoch}) do
    is_atom(name) and is_atom(origin_node) and valid_generation?(origin_generation) and
      is_integer(shard) and shard >= 0 and
      ((is_nil(cluster) and cluster_epoch == origin_generation) or
         (is_binary(cluster) and is_reference(cluster_epoch)))
  end

  def valid_stream_id?(_stream_id), do: false

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
