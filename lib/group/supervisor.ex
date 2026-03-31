defmodule Group.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_group_sup")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.get(opts, :shards, 8)
    callbacks = Keyword.get(opts, :callbacks, %{})
    extract_meta = Keyword.get(opts, :extract_meta)
    resolve_registry_conflict = Keyword.get(opts, :resolve_registry_conflict)
    log = Keyword.get(opts, :log, :info)

    replicated_pg_receiver_buffer_size =
      positive_integer_opt(opts, :replicated_pg_receiver_buffer_size, 64)

    replicated_pg_receiver_flush_interval =
      non_negative_integer_opt(opts, :replicated_pg_receiver_flush_interval, 5)

    # persistent_term config — must be set before children start (Replica reads it)
    config = %{
      callbacks: callbacks,
      num_shards: num_shards,
      log: log,
      replicated_pg_receiver_buffer_size: replicated_pg_receiver_buffer_size,
      replicated_pg_receiver_flush_interval: replicated_pg_receiver_flush_interval
    }

    config = if extract_meta, do: Map.put(config, :extract_meta, extract_meta), else: config

    config =
      if resolve_registry_conflict,
        do: Map.put(config, :resolve_registry_conflict, resolve_registry_conflict),
        else: config

    :persistent_term.put({Group, name}, config)

    children = [
      {Group.Replica.Data, name: name, num_shards: num_shards},
      {Group.Replica.Supervisor, name: name, num_shards: num_shards},
      {Registry, keys: :duplicate, name: Group.registry_name(name)},
      {Group.ClusterLease, name: name, num_shards: num_shards}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp positive_integer_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "expected #{inspect(key)} to be a positive integer, got: #{inspect(value)}"
    end
  end

  defp non_negative_integer_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 ->
        value

      value ->
        raise ArgumentError,
              "expected #{inspect(key)} to be a non-negative integer, got: #{inspect(value)}"
    end
  end
end
