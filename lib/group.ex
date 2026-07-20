defmodule Group do
  @moduledoc """
  Distributed process groups, registry, lifecycle monitoring, and isolated subclusters.

  This module provides:
  - **Distributed registry**: Unique key => process mapping across all nodes
  - **Process groups**: Allow processes to join/leave keys (many processes per key)
  - **Isolated subclusters**: Partition groups and registries into named subclusters for isolated messaging
  - **Lifecycle monitoring**: Monitor lifecycle events for registry and group changes

  ## Consistency Model

  All operations are **eventually consistent**. The built-in replication layer uses
  Erlang distribution to propagate state across nodes, which means:

  - Writes (register, join, etc.) return immediately after local update
  - Other nodes receive updates asynchronously via Erlang distribution
  - During network partitions, nodes may have divergent views
  - When partitions heal, conflicts are resolved. The losing process is killed
    with `{:group_registry_conflict, key, meta}`

  ## Dispatch and Broadcast Ordering

  `dispatch/4` and `broadcast/4` are asynchronous message-delivery APIs.
  They return after Group accepts the message for delivery, not after recipients
  handle it.

  ### What Group Guarantees

  Same cluster-and-key membership causality is preserved. The default cluster
  (`nil`) and each named cluster are separate routing scopes. If a process
  successfully `join`s or `register`s key `K` in cluster `C`, and then later
  sends `dispatch` or `broadcast` for the same cluster `C` and key `K`, any
  remote process that receives that message will see the earlier membership
  update in `members/3` or `lookup/3`.

      :ok = Group.join(MyGroup, "room/123", %{role: :player})
      :ok = Group.broadcast(MyGroup, "room/123", {:hello, self()})

  A remote receiver that handles `{:hello, pid}` can call:

      Group.members(MyGroup, "room/123")

  and expect the sender's joined process to be visible.

  Receiver-local joins are included at delivery time. On each contacted node,
  Group performs a fresh local lookup immediately before delivery, so a process
  that joined locally before the dispatcher fan-out runs can receive the message
  even if the sender had not learned about that member yet.

  `broadcast/4` contacts every known peer node in the selected cluster. Use it
  when you prefer an extra remote lookup over missing a recent subscriber.

  `dispatch/4` contacts only remote nodes that are already visible in this
  node's replicated member view for the selected cluster and key. Use it when
  you want the optimized path and can tolerate eventual membership visibility.

  Per-sender ordering is preserved for messages to the same cluster and key.
  Because messages for the same cluster and key route through the same source
  shard and same receiver shard, a single sender's consecutive `broadcast` or
  `dispatch` calls for cluster `C` and key `K` are delivered to each contacted
  node in send order.

  ### What Group Does Not Guarantee

  There is no synchronous delivery guarantee. A successful `dispatch/4` or
  `broadcast/4` return does not mean any recipient has received or handled the
  message.

  There is no global membership barrier. Same cluster-and-key causality does
  not extend across unrelated keys or clusters. A message for `"room/456"` does
  not act as a barrier for membership changes on `"room/123"`:

      :ok = Group.join(MyGroup, "room/123", %{})
      :ok = Group.broadcast(MyGroup, "room/456", :hello)

  A receiver of the `"room/456"` message should not assume that
  `Group.members(MyGroup, "room/123")` has already caught up.

  Cluster scope matters the same way: a join in `cluster: "blue"` does not act
  as a barrier for a dispatch or broadcast in the default cluster, or in any
  other named cluster.

  `dispatch/4` does not guarantee all-node delivery. If the sender has not yet
  learned that a remote node has members for the selected cluster and key,
  `dispatch/4` may not contact that node. Use `broadcast/4` when every known
  peer node in the selected cluster should be contacted.

  `broadcast/4` does not guarantee delivery to nodes outside the sender's
  current cluster view. If cluster membership itself is still converging, a
  newly connected node may be missed by a concurrent broadcast.

  `monitor/3` is not an application-message subscription. Monitor-only
  processes receive lifecycle events, not application messages. A process must
  `join/4` the key or be registered at that exact key to receive `dispatch/4`
  or `broadcast/4` messages.

  `leave/3` and `unregister/3` are not unsubscribe barriers for messages that
  were already selected, in flight, or already delivered to the process mailbox.
  After leaving, a process may still receive an older application message. If
  this matters, include an application-level epoch or session id in messages and
  ignore stale messages after leaving.

  Monitor events are not ordered before application messages. Lifecycle events
  from `monitor/3` and application messages from `dispatch/4` or `broadcast/4`
  are separate delivery paths.

  There is no cross-sender total ordering. If multiple processes concurrently
  broadcast to the same cluster and key, recipients may observe those messages
  in different interleavings.

  There is no duplicate suppression across registry and group membership. If
  the same process is both registered at key `K` and joined to key `K` in the
  same cluster, it may receive the same application message through both
  routes.

  During partitions, dispatch, broadcast, lookup, and members reflect each
  node's current local view. Group converges after connectivity returns, but it
  does not provide linearizable reads or exactly-once message delivery across
  partitions.

  ## Clusters

  By default, all operations use the default cluster (`nil`). You can
  optionally create isolated subclusters where only connected nodes receive events.

  Named clusters use string names (e.g., `"game_servers"`).

  `connect/3` supports `ttl: milliseconds` for named clusters. This is useful
  when a node only needs a cluster while it has local interest in that cluster.
  The initial connect still does the normal ETS-first membership check, so
  repeated `connect/3` calls while already connected stay a cheap noop and do
  not refresh the TTL. When a lease expires, Group only disconnects if the
  local node has no cluster-scoped monitors, no local registrations, and no
  local group memberships in that named cluster.

  ### Important: DurableServer Registration

  DurableServers always register in the **default cluster** to ensure global uniqueness
  via the distributed locking mechanism. Named clusters are purely for isolating your own
  registries, process groups, and subscriptions to isolated subclusters. If a DurableServer
  wants to participate in an isolated cluster, it can call `connect/2` and `join/4`
  inside its `init` callback.

  ## Core Concepts

  ### Monitoring vs Memberships

  - **Monitoring** (`monitor/2`, `demonitor/2`): Receive events in your mailbox
    when DurableServers or other processes register/join matching keys anywhere in the
    cluster. Supports pattern matching on keys.

  - **Memberships** (`join/3`, `leave/2`): Make your process discoverable cluster-wide
    via `members/2`. Triggers `:joined`/`:left` events to monitors.

  These are independent - joining a key does NOT automatically monitor events,
  and monitoring does NOT make you discoverable via `members/2`.

  ## Event Types

  Events are delivered as `{:group, events, info}` tuples:

      {:group, [
        %Group.Event{
          type: event_type,
          supervisor: name,
          cluster: cluster_name,  # nil for default cluster
          key: key,
          pid: pid,
          meta: meta,             # always user-provided meta (internal keys stripped)
          previous_meta: ...,     # nil for new, old meta for re-register/re-join
          reason: ...             # set on :unregistered/:left events
        }
      ], %{name: name}}

  | Event           | Trigger                           | Extra Fields     |
  |-----------------|-----------------------------------|------------------|
  | `:registered`   | `register/3` (new or re-register) | `:previous_meta` |
  | `:unregistered` | Process unregistered or died      | `:reason`        |
  | `:joined`       | `join/3` (new or re-join)         | `:previous_meta` |
  | `:left`         | Process left group or died        | `:reason`        |

  `:previous_meta` is `nil` for new registrations/joins, or the old
  metadata map when re-registering/re-joining.

  DurableServers automatically register/unregister during their lifecycle, so these
  events can be used to track DurableServer start/stop.

  ## Pattern Types

  Monitors support three pattern types:

  - `"user/123"` - Exact match, only events for this specific key
  - `"user/"` - Prefix match, all keys starting with "user/"
  - `:all` - All events for this supervisor

  ## Self-Events

  A process that monitors a pattern and then joins a matching key will receive
  its own `:joined` event. Similarly for `:left` when leaving. Filter these in your
  handler if needed:

      def handle_info({:group, events, _info}, state) do
        Enum.each(events, fn
          %Group.Event{type: :joined, pid: pid} when pid != self() ->
            # Handle other processes' join events
            :ok
          _ -> :ok
        end)
        {:noreply, state}
      end

  ## Examples

  ### Basic Monitoring

      # Monitor all events for a specific key
      :ok = Group.monitor(MySup, "user/123")

      # Monitor all keys under a prefix
      :ok = Group.monitor(MySup, "chat/")

      # Monitor all events
      :ok = Group.monitor(MySup, :all)

      # Handle events in a GenServer
      def handle_info({:group, events, _info}, state) do
        Enum.each(events, fn
          %Group.Event{type: :registered, key: key} ->
            IO.puts("DurableServer started: \#{key}")
          %Group.Event{type: :unregistered, key: key, reason: reason} ->
            IO.puts("DurableServer stopped: \#{key}, reason: \#{inspect(reason)}")
          _ -> :ok
        end)
        {:noreply, state}
      end

  ### Joining as a Member

      # Join a key to be discoverable by other processes
      :ok = Group.join(MySup, "game/room/42", %{role: :spectator})

      # Query all members of a key (joined processes only)
      members = Group.members(MySup, "game/room/42")
      # => [{#PID<0.200.0>, %{role: :spectator}}]

      # Leave when done
      :ok = Group.leave(MySup, "game/room/42")

  ### Using Named Clusters

      # Connect this node to a named cluster
      :ok = Group.connect(MySup, "game_servers")

      # Or lease it while this node still has local interest
      :ok = Group.connect(MySup, "game_servers", ttl: 30_000)

      # Join a group in the named cluster
      :ok = Group.join(MySup, "room/123", %{role: :member}, cluster: "game_servers")

      # Monitor events in the named cluster
      :ok = Group.monitor(MySup, :all, cluster: "game_servers")

      # Members, dispatch, and broadcast also support cluster option
      Group.members(MySup, "room/123", cluster: "game_servers")
      Group.dispatch(MySup, "room/123", {:msg, "hi"}, cluster: "game_servers")
      Group.broadcast(MySup, "room/123", {:msg, "hi"}, cluster: "game_servers")

  ## Architecture Notes

  - **Events are cluster-wide**: Replication callbacks fire on ALL nodes in the cluster.
    This means a monitor on Node A receives events when a DurableServer registers on Node B.

  - **Monitors** are stored per-node in an Elixir `Registry`, enabling pattern matching
    and automatic cleanup when monitoring processes die.

  - **Memberships** use built-in process groups for cluster-wide distribution and automatic
    cleanup when member processes die.
  """

  alias Group.Replica
  alias Group.Replica.Data

  @default_call_timeout 5_000
  @default_cluster_call_timeout 60_000

  # ===========================================================================
  # Startup
  # ===========================================================================

  @doc """
  Returns a child spec for starting Group under a supervisor.

  ## Options

  - `:name` (required) — atom identifying this Group instance
  - `:shards` — number of GenServer shards (default: 8). Must match across all nodes
  - `:log` — logging level. `:info` (default) logs peer discovery, node events,
    and cluster membership changes. `:verbose` additionally logs per-shard
    replication messages. `false` disables all Group log output.
  - `:resolve_registry_conflict` — `{module, function, extra_args}` callback invoked when
    two nodes hold the same registry key (partition heal or concurrent registration).
    Called as `apply(module, function, [name, key, {pid1, meta1, time1}, {pid2, meta2, time2} | extra_args])`.
    Must return the winner pid. The loser is killed with `{:group_registry_conflict, key, meta}`.
    **Important:** This callback runs synchronously inside the shard GenServer — it must
    return quickly and never block. Any information needed for the decision should be
    carried in the registration metadata, not fetched at resolution time.
  - `:extract_meta` — `{module, function, args}` to transform metadata on reads
  - `:replicated_pg_receiver_buffer_size` — max buffered replicated PG join/leave ops
    per shard before the receiver flushes immediately (default: `64`)
  - `:replicated_pg_receiver_flush_interval` — max time in milliseconds a shard will
    buffer replicated PG join/leave ops before flushing (default: `5`)
  - `:replicated_registry_receiver_buffer_size` — max buffered replicated registry
    register/unregister ops per shard before the receiver flushes immediately
    (default: `64`)
  - `:replicated_registry_receiver_flush_interval` — max time in milliseconds a shard
    will buffer replicated register/unregister ops before flushing (default: `5`)
  - `:replicated_sender_buffer_size` — max buffered replicated outbound ops per shard
    before the sender flushes immediately (default: `64`)
  - `:replicated_sender_flush_interval` — max time in milliseconds a shard will
    buffer replicated outbound ops before flushing during idle periods. Sender
    buffers also flush on size, overdue enqueue, and control/routing barriers
    (default: `5`)
  - `:busy_dist_retry_attempts` — max reconnect attempts after a shard hits
    `send_nosuspend == false` to a remote node and forces a disconnect
    (default: `300`)
  - `:busy_dist_retry_interval` — interval in milliseconds between reconnect
    attempts after a busy-dist disconnect (default: `1_000`)
  - `:replicated_pg_receiver_local_request_quota` — max queued local PG shard requests
    drained after each replicated PG flush before yielding (default: `8`)
  """
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)
    %{id: {__MODULE__, name}, start: {Group.Supervisor, :start_link, [opts]}, type: :supervisor}
  end

  def start_link(opts), do: Group.Supervisor.start_link(opts)

  # ===========================================================================
  # Cluster Management (Node <-> Cluster)
  # ===========================================================================

  @doc """
  Connect the local node to a named cluster.

  This adds the current node to the cluster, allowing it to send and receive
  process group events within that cluster.

  ## Parameters

  - `name` - The Group name
  - `cluster_name` - The name of the cluster to connect to (binary string)
  - `opts` - Keyword list of options

  ## Options

  - `:timeout` - timeout passed to the shard request (default: `60_000`)
  - `:ttl` - for named clusters, keep the local connection leased for this many
    milliseconds. The lease is only created on a new connect; repeated
    `connect/3` calls while already connected stay a cheap noop and do not
    refresh the TTL. Expired leases only disconnect if the local node has no
    cluster-scoped monitors, no local registrations, and no local group
    memberships in that cluster.

  ## Returns

  - `:ok` on success
  """
  def connect(name, cluster_or_clusters, opts \\ [])
      when is_atom(name) and is_list(opts) do
    clusters = List.wrap(cluster_or_clusters)
    local = node()
    ttl_ms = cluster_ttl(opts)
    new_clusters = Enum.reject(clusters, fn c -> local in Data.cluster_nodes(name, c) end)

    if new_clusters != [] do
      connect_clusters(name, new_clusters, cluster_call_timeout(opts))

      if ttl_ms do
        expires_at = System.monotonic_time(:millisecond) + ttl_ms

        for cluster <- new_clusters do
          Data.put_cluster_lease(name, cluster, ttl_ms, expires_at)
        end

        Group.ClusterLease.reschedule(name)
      end
    end

    :ok
  end

  @doc """
  Disconnect the local node from one or more named clusters.

  Accepts a single cluster name (binary) or a list of cluster names.

  ## Parameters

  - `name` - The Group name
  - `cluster_name` - The cluster name or list of cluster names to disconnect from
  - `opts` - Keyword list of options

  ## Options

  - `:timeout` - timeout passed to the shard request (default: `60_000`)
  """
  def disconnect(name, cluster_or_clusters, opts \\ [])
      when is_atom(name) and is_list(opts) do
    clusters = List.wrap(cluster_or_clusters)
    timeout = cluster_call_timeout(opts)

    if clusters != [] do
      for cluster <- clusters do
        Data.delete_cluster_lease(name, cluster)
      end

      Group.ClusterLease.reschedule(name)
      disconnect_clusters(name, clusters, timeout)
    end

    :ok
  end

  @doc """
  Check if the local node is connected to a named cluster.

  ## Parameters

  - `name` - The Group name
  - `cluster_name` - The name of the cluster to check (binary string)

  ## Returns

  - `true` if connected
  - `false` if not connected
  """
  def connected?(name, cluster_name)
      when is_atom(name) and is_binary(cluster_name) do
    node() in Data.cluster_nodes(name, cluster_name)
  end

  @doc """
  List all nodes running this Group instance.

  Returns nodes that have completed the peer discovery handshake, excluding
  the local node. Unlike `Node.list()`, this only includes nodes actually
  running this Group instance, not all connected Erlang nodes.

  ## Parameters

  - `name` - The Group name

  ## Returns

  - List of node atoms
  """
  def nodes(name) when is_atom(name) do
    try do
      Data.cluster_nodes(name, nil) -- [node()]
    rescue
      ArgumentError -> []
    end
  end

  @doc """
  List all nodes in a named cluster.

  ## Parameters

  - `name` - The Group name
  - `cluster_name` - The cluster name (binary string)

  ## Returns

  - List of node atoms
  """
  def nodes(name, cluster_name)
      when is_atom(name) and is_binary(cluster_name) do
    try do
      Data.cluster_nodes(name, cluster_name)
    rescue
      ArgumentError -> []
    end
  end

  # ===========================================================================
  # Registry (Process Registration)
  # ===========================================================================

  @doc """
  Register the calling process in the cluster registry.

  This registers a process with a unique key in the cluster. Only one process
  can be registered with a given key at a time (cluster-wide uniqueness).

  Use `register/4` when you need exactly one process per key. Use `join/4`
  when multiple processes should be able to share the same key.

  See the [Dispatch and Broadcast Ordering](#module-dispatch-and-broadcast-ordering)
  section for the same cluster-and-key causality guarantee between registration
  and later app messages.

  ## Parameters

  - `name` - The Group name
  - `key` - The unique key to register under
  - `meta` - Metadata map to associate with the registration
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Register in a named cluster instead of the default cluster
  - `:timeout` - timeout passed to the shard request (default: `5_000`)

  ## Returns

  - `:ok` on success
  - `{:error, :taken}` if another process is already registered with this key
  """
  def register(name, key, meta, opts \\ [])

  def register(name, key, meta, opts)
      when is_atom(name) and is_binary(key) and is_map(meta) and is_list(opts) do
    validate_key!(key)
    cluster = Keyword.get(opts, :cluster)
    validate_cluster_connected!(name, cluster)
    shard = Replica.shard_for(name, cluster, key)

    Replica.local_request(shard, {:register, cluster, key, self(), meta}, call_timeout(opts))
  end

  @doc """
  Unregister a process from the cluster registry.

  This removes a process registration. Typically not needed as registrations
  are automatically cleaned up when the process dies.

  ## Parameters

  - `name` - The Group name
  - `key` - The key to unregister
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Unregister from a named cluster instead of the default cluster
  - `:timeout` - timeout passed to the shard request (default: `5_000`)

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def unregister(name, key, opts \\ [])

  def unregister(name, key, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    validate_key!(key)
    cluster = Keyword.get(opts, :cluster)
    validate_cluster_connected!(name, cluster)
    shard = Replica.shard_for(name, cluster, key)

    Replica.local_request(shard, {:unregister, cluster, key}, call_timeout(opts))
  end

  @doc """
  Look up a registered process by key.

  Returns the pid and metadata for the process registered at the given key,
  or `nil` if no process is registered.

  ## Parameters

  - `name` - The Group name
  - `key` - The key to look up
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Look up in a named cluster instead of the default cluster
  - `:extract_meta` - Override the configured extract_meta callback for this call

  ## Returns

  - `{pid, meta}` if a process is registered at the key
  - `nil` if no process is registered
  """
  def lookup(name, key, opts \\ [])

  def lookup(name, key, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    case get_config(name) do
      nil ->
        nil

      config ->
        cluster = Keyword.get(opts, :cluster)
        extract_meta_fn = resolve_extract_meta(name, opts)
        num_shards = config.num_shards
        shard = Replica.shard_index_for(cluster, key, num_shards)

        case Data.registry_lookup(name, shard, cluster, key) do
          {pid, meta, _time, _node} ->
            {pid, extract_meta_fn.(meta)}

          nil ->
            nil
        end
    end
  rescue
    ArgumentError -> nil
  end

  # ===========================================================================
  # Lifecycle Monitoring
  # ===========================================================================

  @doc """
  Monitor lifecycle events matching the given pattern.

  The calling process will receive `{:group, events, info}` tuples containing matching events:

      {:group, [%Group.Event{type: :registered, ...}], %{name: name}}

  ## Patterns

  - `"exact/key"` - exact key match
  - `"prefix/"` - all keys starting with "prefix/"
  - `:all` - all keys

  ## Options

  - `:cluster` - Monitor events from a named cluster (default: nil for default cluster)

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def monitor(name, pattern_string, opts \\ [])
      when is_atom(name) and (is_binary(pattern_string) or pattern_string == :all) do
    cluster = Keyword.get(opts, :cluster)
    pattern = parse_pattern(pattern_string)
    key = {name, cluster, pattern}

    case Registry.register(registry_name(name), key, nil) do
      {:ok, _} -> :ok
      {:error, {:already_registered, _}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Stop monitoring lifecycle events for the given pattern.

  ## Options

  - `:cluster` - The cluster to demonitor from (default: nil for default cluster)

  ## Returns

  - `:ok` always (demonitoring a non-existent monitor is a no-op)
  """
  def demonitor(name, pattern_string, opts \\ []) when is_atom(name) do
    cluster = Keyword.get(opts, :cluster)
    pattern = parse_pattern(pattern_string)
    key = {name, cluster, pattern}
    Registry.unregister(registry_name(name), key)
    :ok
  end

  # ===========================================================================
  # Process Groups (Process <-> Group)
  # ===========================================================================

  @doc """
  Join a group as a member.

  The process will:
  - Be discoverable via `members/2`
  - Be automatically removed when it dies

  Re-joining an already-joined group updates the metadata in place.

  Note: Joining does NOT automatically monitor events.
  Call `monitor/2` separately if you want to receive events.

  See the [Dispatch and Broadcast Ordering](#module-dispatch-and-broadcast-ordering)
  section for the same cluster-and-key causality guarantee between membership
  and later app messages.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to join (string)
  - `meta` - Metadata map (default: `%{}`)
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Join a named cluster instead of the default cluster
  - `:timeout` - timeout passed to the shard request (default: `5_000`)

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def join(name, group, meta \\ %{}, opts \\ [])

  def join(name, group, meta, opts)
      when is_atom(name) and is_binary(group) and is_map(meta) and
             is_list(opts) do
    validate_key!(group)
    cluster = Keyword.get(opts, :cluster)
    validate_cluster_connected!(name, cluster)
    shard = Replica.shard_for(name, cluster, group)

    Replica.local_request(shard, {:join, cluster, group, self(), meta}, call_timeout(opts))
  end

  @doc """
  Leave a group that was previously joined.

  Leaving removes the process from future membership lookups once the leave is
  observed, but it is not an unsubscribe barrier for messages that were already
  selected, in flight, or already delivered to the process mailbox. If stale
  messages matter, include an application-level epoch or session id in messages
  and ignore old messages after leaving.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to leave (string)
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Leave from a named cluster instead of the default cluster
  - `:timeout` - timeout passed to the shard request (default: `5_000`)

  ## Returns

  - `:ok` on success
  - `{:error, :not_in_group}` if not a member
  """
  def leave(name, group, opts \\ [])

  def leave(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    validate_key!(group)
    cluster = Keyword.get(opts, :cluster)
    validate_cluster_connected!(name, cluster)
    shard = Replica.shard_for(name, cluster, group)

    Replica.local_request(shard, {:leave, cluster, group, self()}, call_timeout(opts))
  end

  # ===========================================================================
  # Queries
  # ===========================================================================

  @doc """
  List all members of a group (process group entries only).

  Returns processes that have joined via `join/3`. Registry entries
  (via `register/3`) are not included — use `lookup/3` for those.

  Supports prefix matching: if `group` ends with `"/"`, returns all
  members whose group key starts with that prefix. Prefix queries scan
  all shards. Exact queries hit a single shard.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to query (string). Append `"/"` for prefix matching.
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Query a named cluster instead of the default cluster
  - `:extract_meta` - Override the configured extract_meta callback for this call
  - `:limit` - Return at most this many members. Must be a non-negative integer.

  ## Returns

  - List of `{pid, meta}` tuples
  """
  def members(name, group, opts \\ [])

  def members(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    extract_meta_fn = resolve_extract_meta(name, opts)
    num_shards = get_config(name).num_shards
    limit = member_limit!(opts)

    if String.ends_with?(group, "/") do
      members_by_prefix(name, num_shards, cluster, group, extract_meta_fn, limit)
    else
      members_exact(name, num_shards, cluster, group, extract_meta_fn, limit)
    end
  end

  defp members_exact(name, num_shards, cluster, group, extract_meta_fn, limit) do
    shard = Replica.shard_index_for(cluster, group, num_shards)

    Data.pg_members(name, shard, cluster, group, limit)
    |> Enum.map(fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
  end

  defp members_by_prefix(name, num_shards, cluster, prefix, extract_meta_fn, limit) do
    num_shards
    |> members_across_shards(limit, fn shard, shard_limit ->
      Data.pg_members_by_prefix(name, shard, cluster, prefix, shard_limit)
    end)
    |> Enum.map(fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
  end

  @doc """
  List process-group members owned by the local node.

  Unlike `members/3`, this excludes replicated memberships owned by remote
  nodes. Registry entries are not included.

  Supports prefix matching: if `group` ends with `"/"`, returns local members
  whose group key starts with that prefix. Prefix queries scan all shards. Exact
  queries hit a single shard.

  ## Options

  - `:cluster` - Query a named cluster instead of the default cluster
  - `:extract_meta` - Override the configured extract_meta callback for this call
  - `:limit` - Return at most this many members. Must be a non-negative integer.

  ## Returns

  - List of `{pid, meta}` tuples
  """
  def local_members(name, group, opts \\ [])

  def local_members(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    extract_meta_fn = resolve_extract_meta(name, opts)
    num_shards = get_config(name).num_shards
    limit = member_limit!(opts)

    members =
      if String.ends_with?(group, "/") do
        members_across_shards(num_shards, limit, fn shard, shard_limit ->
          Data.pg_members_local_by_prefix(name, shard, cluster, group, shard_limit)
        end)
      else
        shard = Replica.shard_index_for(cluster, group, num_shards)
        Data.pg_members_local_with_meta(name, shard, cluster, group, limit)
      end

    Enum.map(members, fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
  end

  @doc """
  Count processes registered in this node's local replicated ETS view.

  This returns the caller node's current view of the cluster registry.
  It is eventually consistent across nodes and may differ briefly during
  propagation, partitions, or healing.

  ## Parameters

  - `name` - The Group name
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def registry_count(name, opts \\ [])

  def registry_count(name, opts)
      when is_atom(name) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    Data.registry_count(name, num_shards, cluster)
  end

  @doc """
  Count processes registered in the local node's registry.

  This counts only processes registered via `register/5` on the local node.

  ## Parameters

  - `name` - The Group name
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def local_registry_count(name, opts \\ [])

  def local_registry_count(name, opts)
      when is_atom(name) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    Data.local_registry_count(name, num_shards, cluster)
  end

  @doc """
  Count group members in this node's local replicated ETS view.

  This returns the caller node's current view of process group membership.
  It is eventually consistent across nodes and may differ briefly during
  propagation, partitions, or healing.

  Supports prefix matching: if `group` ends with `"/"`, counts all
  members whose group key starts with that prefix.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to count (string). Append `"/"` for prefix matching.
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def member_count(name, group, opts \\ [])

  def member_count(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards

    if String.ends_with?(group, "/") do
      Data.pg_count_by_prefix(name, num_shards, cluster, group)
    else
      Data.pg_count(name, num_shards, cluster, group)
    end
  end

  @doc """
  Count processes in a group on the local node.

  Supports prefix matching: if `group` ends with `"/"`, counts all local
  members whose group key starts with that prefix.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to count (string). Append `"/"` for prefix matching.
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def local_member_count(name, group, opts \\ [])

  def local_member_count(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards

    if String.ends_with?(group, "/") do
      Data.local_pg_count_by_prefix(name, num_shards, cluster, group)
    else
      Data.local_pg_count(name, num_shards, cluster, group)
    end
  end

  @doc """
  List all entries owned by the local node across all shards.

  Returns a flat tagged list containing both registry and process-group entries:

      {:registry, cluster, key, pid, meta}
      {:pg, cluster, key, pid, meta}

  This includes only entries whose owner node is the local node, not the full
  replicated cluster view. Metadata is passed through the configured
  `extract_meta` callback, if any.

  ## Parameters

  - `name` - The Group name

  ## Returns

  - List of `{:registry | :pg, cluster, key, pid, meta}` tuples
  """
  def local_entries(name) when is_atom(name) do
    extract_meta_fn = resolve_extract_meta(name, [])
    num_shards = get_config(name).num_shards

    Enum.flat_map(0..(num_shards - 1), fn shard ->
      Data.local_entries(name, shard)
      |> Enum.map(fn {type, cluster, key, pid, meta} ->
        {type, cluster, key, pid, extract_meta_fn.(meta)}
      end)
    end)
  end

  # ===========================================================================
  # Dispatch
  # ===========================================================================

  @doc """
  Dispatch a message to all members of a key visible from this node.

  Sends `message` to all processes that have joined the key via `join/3`, as well as
  any DurableServer registered at that key. This is useful for application-level
  messaging between a DurableServer and connected clients (e.g., Phoenix Channels).

  ## Dispatch vs Monitor

  There are two ways to receive messages in this module:

  - **`monitor/2`** - Receive *lifecycle events* (`:registered`, `:unregistered`, etc.)
    when DurableServers or processes join/leave keys matching a pattern. These are
    system-generated events.

  - **`dispatch/3`** - Receive *application messages* sent explicitly by your code.
    Only members of the exact key receive the message.

  Use `monitor` to react to lifecycle changes. Use `dispatch` to send your own
  messages to members.

  ## Filtering by Metadata

  `dispatch/3` sends to all members in this node's current replicated view.
  Remote nodes perform a fresh local lookup before delivering the message, but
  `dispatch/3` only contacts remote nodes that this node can already see for
  the selected cluster and key. Use `broadcast/3` when you want to contact all
  peer nodes in the selected cluster.

  Delivery is asynchronous: the call returns after enqueueing work on the
  local owning shard, not after recipients process the message.

  See the [Dispatch and Broadcast Ordering](#module-dispatch-and-broadcast-ordering)
  section for the same cluster-and-key causality guarantee.

  If you need to filter by metadata (e.g., only send to members with
  `%{type: :channel}`), use `members/2` directly:

      for {pid, %{type: :channel}} <- Group.members(sup, key) do
        send(pid, message)
      end

  ## Options

  - `:cluster` - Dispatch to a named cluster instead of the default cluster

  ## Returns

  - `:ok` always
  """
  def dispatch(name, key, message, opts \\ [])

  def dispatch(name, key, message, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    shard = Replica.shard_index_for(cluster, key, num_shards)

    send(Replica.shard_name(name, shard), {:group_dispatch_request, cluster, key, message})

    :ok
  end

  @doc """
  Broadcast a message to all members of a key on every peer node in the selected
  cluster.

  Like `dispatch/4`, this sends to the local registry entry and local PG
  members for the key. Unlike `dispatch/4`, it does not use this node's
  replicated member view to decide which remote nodes may have recipients.
  It routes through the local owning shard, which flushes pending replication
  before sending one request to the owning shard on every remote peer in the
  cluster. Each receiver shard flushes pending replication before forwarding
  to its local dispatcher for a fresh local lookup and delivery.

  This is useful when missing a recent remote join is worse than sending an
  extra remote shard message.

  The `:cluster` option selects the cluster whose peer nodes are contacted and
  is part of the causality scope. The default cluster and named clusters do not
  order each other.

  Delivery is asynchronous: the call returns after enqueueing work on the
  local owning shard, not after recipients process the message.

  See the [Dispatch and Broadcast Ordering](#module-dispatch-and-broadcast-ordering)
  section for the same cluster-and-key causality guarantee.

  ## Options

  - `:cluster` - Broadcast within a named cluster instead of the default cluster

  ## Returns

  - `:ok` always
  """
  def broadcast(name, key, message, opts \\ [])

  def broadcast(name, key, message, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    shard = Replica.shard_index_for(cluster, key, num_shards)

    send(Replica.shard_name(name, shard), {:group_broadcast_request, cluster, key, message})

    :ok
  end

  @doc """
  Like `dispatch/4`, but only sends to members on the local node.

  Skips all cross-node messaging. Useful when you know remote nodes will
  handle their own local dispatch (e.g., a broadcast originating on each node).

  ## Options

  - `:cluster` - Dispatch to a named cluster instead of the default cluster

  ## Returns

  - `:ok` always
  """
  def dispatch_local(name, key, message, opts \\ [])

  def dispatch_local(name, key, message, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    shard = Replica.shard_index_for(cluster, key, num_shards)

    dispatch_local_members(name, shard, cluster, key, message)

    :ok
  end

  defp dispatch_local_members(name, shard, cluster, key, message) do
    local = node()

    case Data.registry_lookup(name, shard, cluster, key) do
      {pid, _meta, _time, ^local} -> send(pid, message)
      _ -> :ok
    end

    for pid <- Data.pg_members_local(name, shard, cluster, key) do
      send(pid, message)
    end

    :ok
  end

  # ===========================================================================
  # Public Helpers
  # ===========================================================================

  @doc """
  Set the log level at runtime. Accepts `:info`, `:verbose`, or `false`.

  Updates the persistent_term config so the change takes effect immediately
  on all shards without restart.

  This calls `:persistent_term.put/2`, so treat it as an admin operation,
  not something to invoke in a hot loop.
  """
  def log_level(name, level) when level in [:info, :verbose, false] do
    config = get_config(name)
    :persistent_term.put({__MODULE__, name}, %{config | log: level})
    :ok
  end

  @doc false
  def get_config(name) when is_atom(name) do
    :persistent_term.get({__MODULE__, name}, nil)
  end

  @doc false
  def connect_clusters(name, clusters, timeout)
      when is_atom(name) and is_list(clusters) and is_integer(timeout) do
    local = node()

    for cluster <- clusters do
      Data.add_cluster_node(name, cluster, local)
    end

    notify_shard = :rand.uniform(get_config(name).num_shards) - 1

    Replica.local_request(
      Replica.shard_name(name, notify_shard),
      {:cluster_connect, clusters},
      timeout
    )
  end

  @doc false
  def disconnect_clusters(name, clusters, timeout)
      when is_atom(name) and is_list(clusters) and is_integer(timeout) do
    for cluster <- clusters do
      Data.remove_cluster_node(name, cluster, node())
    end

    num_shards = get_config(name).num_shards

    for i <- 0..(num_shards - 1) do
      Replica.local_request(
        Replica.shard_name(name, i),
        {:cluster_disconnect, clusters},
        timeout
      )
    end

    :ok
  end

  # ===========================================================================
  # Internal
  # ===========================================================================

  defp validate_key!(key) do
    if String.ends_with?(key, "/") do
      raise ArgumentError,
            "key #{inspect(key)} must not end with \"/\" — trailing slash is reserved for prefix queries"
    end
  end

  defp call_timeout(opts), do: Keyword.get(opts, :timeout, @default_call_timeout)
  defp cluster_call_timeout(opts), do: Keyword.get(opts, :timeout, @default_cluster_call_timeout)
  defp cluster_ttl(opts), do: Keyword.get(opts, :ttl) |> validate_cluster_ttl!()

  defp member_limit!(opts) do
    case Keyword.fetch(opts, :limit) do
      :error ->
        :infinity

      {:ok, limit} when is_integer(limit) and limit >= 0 ->
        limit

      {:ok, limit} ->
        raise ArgumentError,
              "expected :limit to be a non-negative integer, got: #{inspect(limit)}"
    end
  end

  defp members_across_shards(num_shards, :infinity, query) do
    Enum.flat_map(0..(num_shards - 1), fn shard -> query.(shard, :infinity) end)
  end

  defp members_across_shards(_num_shards, 0, _query), do: []

  defp members_across_shards(num_shards, limit, query) do
    {chunks, _remaining} =
      Enum.reduce_while(0..(num_shards - 1), {[], limit}, fn shard, {chunks, remaining} ->
        members = query.(shard, remaining)
        remaining = remaining - length(members)
        chunks = [members | chunks]

        if remaining == 0 do
          {:halt, {chunks, remaining}}
        else
          {:cont, {chunks, remaining}}
        end
      end)

    chunks
    |> Enum.reverse()
    |> List.flatten()
  end

  defp validate_cluster_connected!(_name, nil), do: :ok

  defp validate_cluster_connected!(name, cluster) do
    unless node() in Data.cluster_nodes(name, cluster) do
      raise ArgumentError,
            "not connected to cluster #{inspect(cluster)}. Call Group.connect(#{inspect(name)}, #{inspect(cluster)}) first"
    end
  end

  @doc false
  def registry_name(name) when is_atom(name), do: :"#{name}_group_registry"

  defp validate_cluster_ttl!(nil), do: nil
  defp validate_cluster_ttl!(ttl_ms) when is_integer(ttl_ms) and ttl_ms > 0, do: ttl_ms

  defp validate_cluster_ttl!(ttl_ms) do
    raise ArgumentError, "expected :ttl to be a positive integer, got: #{inspect(ttl_ms)}"
  end

  defp parse_pattern(:all), do: :all

  defp parse_pattern(pattern) when is_binary(pattern) do
    if String.ends_with?(pattern, "/") do
      {:prefix, pattern}
    else
      {:exact, pattern}
    end
  end

  # Resolve the extract_meta function: per-call override > persistent_term config > identity
  defp resolve_extract_meta(name, opts) do
    case Keyword.get(opts, :extract_meta) do
      nil ->
        case :persistent_term.get({__MODULE__, name}, nil) do
          %{extract_meta: {mod, func, args}} -> fn meta -> apply(mod, func, [meta | args]) end
          _ -> & &1
        end

      {mod, func, args} ->
        fn meta -> apply(mod, func, [meta | args]) end

      func when is_function(func, 1) ->
        func
    end
  end
end
