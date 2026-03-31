defmodule Group.ClusterLease do
  @moduledoc false
  use GenServer

  alias Group.Replica.Data

  _archdoc = """
  Local named-cluster TTL lease sweeper.

  `Group.connect(name, cluster, ttl: ms)` still uses the normal fast path first:
  it checks `cluster_nodes` and returns `:ok` immediately if the local node is
  already connected. Only a newly connected TTL cluster gets a row in
  `cluster_leases`.

  This process owns no authoritative cluster membership state. Instead, it
  periodically sweeps the lease rows and applies local policy:

  - if the lease is expired and the local node still has cluster-scoped monitors,
    local registry entries, or local PG memberships in that cluster, extend the
    lease by one TTL interval
  - otherwise run the normal disconnect path and delete the lease row

  There is only one timer per Group instance, scheduled to the nearest expiry.
  The lease rows themselves live in ETS under `Group.Replica.Data` so they
  survive lease-manager restarts.
  """

  @sweep_timer :cluster_lease_sweep
  @disconnect_timeout 60_000

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)
    GenServer.start_link(__MODULE__, {name, num_shards}, name: lease_name(name))
  end

  @doc false
  def lease_name(name), do: :"#{name}_cluster_lease"

  @doc false
  def reschedule(name) do
    GenServer.cast(lease_name(name), :reschedule)
  end

  @impl true
  def init({name, num_shards}) do
    {:ok, schedule_next_sweep(%{name: name, num_shards: num_shards, sweep_ref: nil})}
  end

  @impl true
  def handle_cast(:reschedule, state) do
    state =
      state
      |> maybe_sweep_due_leases()
      |> schedule_next_sweep()

    {:noreply, state}
  end

  @impl true
  def handle_info({@sweep_timer, sweep_ref}, %{sweep_ref: sweep_ref} = state) do
    state =
      %{state | sweep_ref: nil}
      |> maybe_sweep_due_leases()
      |> schedule_next_sweep()

    {:noreply, state}
  end

  def handle_info(:force_sweep, state) do
    state =
      %{state | sweep_ref: nil}
      |> maybe_sweep_due_leases()
      |> schedule_next_sweep()

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp maybe_sweep_due_leases(state) do
    now = now_ms()

    Enum.reduce(Data.cluster_leases(state.name), state, fn
      {cluster, ttl_ms, expires_at}, acc when expires_at <= now ->
        sweep_cluster_lease(acc, cluster, ttl_ms, now)

      _lease, acc ->
        acc
    end)
  end

  defp sweep_cluster_lease(state, cluster, ttl_ms, now) do
    cond do
      not Group.connected?(state.name, cluster) ->
        Data.delete_cluster_lease(state.name, cluster)
        state

      cluster_active?(state.name, state.num_shards, cluster) ->
        Data.put_cluster_lease(state.name, cluster, ttl_ms, now + ttl_ms)
        state

      true ->
        disconnect_expired_cluster(state.name, cluster, ttl_ms, now)
        state
    end
  end

  defp disconnect_expired_cluster(name, cluster, ttl_ms, now) do
    try do
      Group.disconnect_clusters(name, [cluster], @disconnect_timeout)
      Data.delete_cluster_lease(name, cluster)
    catch
      :exit, _reason ->
        if Group.connected?(name, cluster) do
          Data.put_cluster_lease(name, cluster, ttl_ms, now + ttl_ms)
        else
          Data.delete_cluster_lease(name, cluster)
        end
    end
  end

  defp cluster_active?(name, num_shards, cluster) do
    Data.local_registry_present?(name, num_shards, cluster) or
      Data.local_pg_present?(name, num_shards, cluster) or
      local_monitor_present?(name, cluster)
  end

  defp local_monitor_present?(name, cluster) do
    Group.registry_name(name)
    |> Registry.count_select([
      {{{name, cluster, :_}, :_, :_}, [], [true]}
    ]) > 0
  rescue
    ArgumentError -> false
  end

  defp schedule_next_sweep(state) do
    case next_expiration(Data.cluster_leases(state.name)) do
      nil ->
        %{state | sweep_ref: nil}

      expires_at ->
        sweep_ref = make_ref()
        delay = max(expires_at - now_ms(), 0)
        Process.send_after(self(), {@sweep_timer, sweep_ref}, delay)
        %{state | sweep_ref: sweep_ref}
    end
  end

  defp next_expiration([]), do: nil

  defp next_expiration(leases) do
    leases
    |> Enum.map(fn {_cluster, _ttl_ms, expires_at} -> expires_at end)
    |> Enum.min()
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
