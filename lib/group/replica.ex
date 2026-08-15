defmodule Group.Replica do
  @moduledoc false

  use GenServer

  @process_down_batch_size 32
  @replicated_pg_receiver_flush_timer :flush_replicated_pg_receiver_buffer
  @replicated_registry_receiver_flush_timer :flush_replicated_registry_receiver_buffer
  @replica_broadcast_flush_timer :flush_replica_broadcast_buffer
  @anti_entropy_timer :group_replica_anti_entropy
  @local_request_tag :group_local_request
  @local_reply_tag :group_local_reply
  @protocol_version Group.Replica.WireProtocol.version()
  @priority_control_quota 64
  @incoming_batch_quota 64
  @snapshot_event_batch_size 512

  _archdoc = ~S"""
  Sharded control process for local writes, replica transport, anti-entropy,
  process monitoring, and registry conflict projection.

  There is one process per shard, registered as
  :"#{name}_replica_#{shard_index}". Reads bypass it and use the materialized
  ETS indexes owned by Group.Replica.Data.

  ## Authority and identity

  Each locally owned mutation belongs to one stream:

      {group, origin_node, origin_generation, shard, cluster, cluster_epoch}

  The shard assigns a strictly increasing sequence number and appends the record
  to its write-ahead oplog before changing materialized ETS. Data owns the
  journal, so a shard crash replays any appended-but-unapplied record before
  rebuilding local process monitors.

  A registry's authoritative claims are stored per origin separately from its
  single visible winner. Conflict selection computes one deterministic rank per
  claim and chooses the maximum `{rank, pid}`, so selection is associative,
  commutative, and independent of delivery order.
  When a local claim loses, only its owner node appends the authoritative
  unregister and terminates the local process. Immediately before that
  irreversible step, Data serially revalidates the remote winner against the
  node-wide authority and this lane's installed view; an authority change
  restarts selection without killing either owner. Retaining hidden remote
  claims until their origin deletes them prevents a later winner change from
  orphaning or permanently forgetting a live claim. Local unregister and
  process-DOWN paths always re-project every affected key after deleting their
  claims. On a shard restart, the complete claim/key union is projected again,
  closing the crash window between journal application and the materialized
  winner write.

  PG memberships need no winner projection: the origin stream owns exactly the
  rows whose member processes live on that origin node.

  ## Wire protocol

  Dist Erlang remains the control plane:

  - peer_connect / peer_connect_ack discover matching shards and clusters.
  - shard 0 exchanges replica_hello authority containing the origin generation
    and complete active named-cluster epoch set exactly once per node.
  - matching nonzero shards exchange constant-size replica_lane_hello messages
    containing their transport descriptor and the authority revision they use.
  - replica_cluster_open / replica_cluster_close fence named-cluster lifetimes.
  - constant-size periodic heartbeats provide a bounded peer lease without
    creating remote process monitors. A generation/epoch-revision mismatch
    requests a fresh authoritative hello.

  Data installs an exact remote authority and its shared-cluster dual-index
  projection in one serialized operation. A concurrent local cluster connect
  therefore observes either the old authority and is covered by the install,
  or the new authority and projects the peer itself; neither ordering can leave
  exact authority permanently disconnected from replica routing.
  Local activation uses the same Data serialization point to install its epoch,
  self route, and every already-exact remote route. Local deactivation removes
  admission and enqueues idempotent cleanup to every shard before Data replies.
  The public API still waits for the shard barrier, but caller survival is not a
  correctness dependency; shard restart repair consumes the same close marker.

  One persisted `{generation, revision}` authority hint is the cross-lane
  fence. A heartbeat or lane hello may advance it only for a peer that already
  has exact authority; Data invalidates every installed lane view in the same
  turn. A delayed hint after complete retirement is discovery only: it cannot
  recreate authority, a transport route, or an unbounded lease. Only the
  dist-Erlang exact hello reintroduces that peer. Contiguous incremental cluster
  controls compare their expected generation/revision against the applied
  authority, observed revision, and hint inside one Data callback. A raced
  heartbeat therefore rejects the complete incremental write instead of
  installing a partial epoch set.

  Replica state uses the configured Group.Transport:

  - heads advertises {stream, retained_floor, head}.
  - delta_batch carries one or more contiguous stream runs.
  - need requests the receiver's next missing sequence.
  - snapshot_chunk carries a byte-bounded part of one exact origin slice when
    the requested prefix has already been pruned. snapshot_commit carries the
    independently retryable terminal row/chunk counts. Receivers expose nothing
    until one valid manifest and every provisional chunk are present.

  Every stream field is validated against the source node and
  current generation/epoch. An old generation, a closed epoch, a wrong shard,
  or a transitive claim for another node's pid is rejected. Control/data
  reordering is safe: early messages are ignored and repeated heads repair them;
  late messages fail their generation or epoch fence. Snapshot chunks may be
  lost, duplicated, reordered, or mixed across retransmissions at the same
  stream head; exact row counts, set insertion, and conflicting-retransmission
  checks prevent partial or mixed commits. Rejected chunks/manifests clear their
  staging immediately. Node loss,
  generation replacement, and retired cluster streams discard matching partial
  assemblies immediately rather than waiting for their inactivity deadline.
  Unsequenced legacy state messages and malformed replica frames are rejected
  before they can mutate a cursor or materialized row.

  ## Bounded recovery

  The oplog is bounded per shard, not by peer acknowledgements. A dropped tail
  is found by periodic heads. A gap inside the retained range is repaired with
  bounded delta batches. A gap below the retained floor receives the existing
  full-sync primitive, narrowed to an exact origin/shard/cluster snapshot.
  Absence from that snapshot is deletion, so no tombstones are required.

  There is no leader, quorum, retention ACK, or requirement to know all members.
  A slow or disconnected peer cannot pin memory. When it returns it repairs from
  deltas when possible and a snapshot otherwise.

  ## Nonblocking transport and ordering

  All cross-node control messages use :erlang.send_nosuspend/3 with :noconnect.
  The default replica adapter does the same. Transport callbacks return :ok,
  :busy, or :disconnected; failure drops the message and anti-entropy repairs it.
  Replica shards never remotely monitor or exit member processes.

  Optional peer lifecycle callbacks are per shard lane. A sideband adapter that
  shares one node connection across shards keeps that route until its final
  live lane goes down; one flapping shard cannot disconnect healthy lanes. A
  lane hello calls `peer_up` and enters `remote_shards` only after exact/current
  authority admits that lane, so delayed post-retirement hellos cannot strand
  an unleased transport route. If a lane hello arrives before exact authority,
  authority fanout immediately repeats shard-local discovery instead of waiting
  for the periodic anti-entropy probe.

  The transport need not order messages for correctness. The local shard
  serializes writes, sequence numbers establish per-stream order, and receivers
  reject duplicates and gaps. TCP shard-to-shard ordering remains the efficient
  fast path. No semantic operation spans clusters, so cross-stream ordering is
  unnecessary; generation and cluster-epoch fences cover lifecycle races.

  ## Batching and fairness

  Local writes share one outbound sender buffer so registry and PG mutations
  retain mailbox order. Flushes group records by target and stream. Size, age,
  control/routing barriers, and idle timers bound the delay.

  Incremental cluster controls are generation fenced, receiver batched, and
  installed only by shard 0 into the node-wide authority table; controls that
  arrive on another lane are forwarded locally to that single owner. Revisions
  must be contiguous. A gap records the highest observation, fences every lane,
  and requests an exact hello instead of applying a partial authority set. The
  last exact revision, complete applied revision, and highest observed revision
  are distinct. The Data owner compare-and-installs an incremental batch only
  when its expected revision still matches the applied revision and persisted
  hint. Shard-local lane readiness is separate from shared authority, so no
  epoch map is copied per shard. On shard restart, constant-size retained
  authority/view metadata seeds leases for origins whose rows survived in Data;
  a disappeared Group is still retired after the normal bounded timeout. Each
  lane deletes its own view only after purging its rows/cursors, so shard 0
  cannot erase a sibling's restart breadcrumb. Shard 0 also reconstructs an
  exact-hello obligation whenever the persisted observed authority revision is
  newer than the last exact revision. If a registry conflict is reconciled
  while one retained claim is temporarily fenced by such an authority gap, the
  lane remembers only that key and reprojects it when the exact view is
  installed. This avoids a shard-wide claim scan while ensuring a current
  cursor can never strand an old visible winner.

  Exact-snapshot scans run in at most one off-shard worker per shard. The worker
  validates the local stream identity and fully-applied head before and after a
  single pass, sends completed provisional chunks immediately, and retains only
  one byte-targeted chunk. Overlapping writes suppress the terminal commit and
  periodic anti-entropy retries the newer head. A busy chunk resumes at its
  index; a busy commit retains only its small manifest. Receivers retain the
  complete candidate and exact-install events in private ETS because exact
  absence-as-delete replacement must start from a complete set. Cleared tables are
  pooled for reuse; shard death deletes active and pooled tables. This keeps
  sender memory bounded at million-to-tens-of-millions scale without putting
  whole-snapshot heaps on the control process.

  Incoming PG mutations retain the bulk receiver lane. Contiguous registry
  records in one stream run are projected together and emit one monitor event
  batch. A mixed process-down record applies maximal same-domain segments in
  wire order and emits one combined batch.

  After replicated work, the shard takes a bounded local-request turn before
  yielding. Priority-control recursion is also quota-bounded. FIFO is preserved
  within the local request lane, while protocol and cluster barriers flush
  earlier buffered state first.

  Snapshot staging is owned by the receiving shard, expires after a peer lease
  without progress, and disappears automatically if the shard crashes. Commit
  first replaces the numeric receive cursor with a durable
  `{:snapshot_installing, sequence}` marker. Startup repair sees that marker,
  removes any partially replaced registry/PG slice, and clears the cursor so
  anti-entropy requests the exact state again.
  """

  require Logger

  alias Group.Replica.{Data, Snapshot, WireProtocol}

  defstruct [
    :name,
    :shard_index,
    :num_shards,
    :replicated_pg_receiver_buffer_size,
    :replicated_pg_receiver_flush_interval,
    :replicated_registry_receiver_buffer_size,
    :replicated_registry_receiver_flush_interval,
    :replicated_sender_buffer_size,
    :replicated_sender_flush_interval,
    :replicated_pg_receiver_local_request_quota,
    :replicated_oplog_max_entries,
    :replicated_snapshot_chunk_target_bytes,
    :replicated_anti_entropy_interval,
    :replicated_peer_lease_timeout,
    :replica_transport,
    :replica_transport_opts,
    :anti_entropy_ref,
    :pending_replicated_pg_started_at,
    :pending_replicated_pg_flush_ref,
    :pending_replicated_registry_started_at,
    :pending_replicated_registry_flush_ref,
    :pending_replica_broadcast_started_at,
    :pending_replica_broadcast_flush_ref,
    pending_replicated_pg_len: 0,
    pending_replicated_pg_ops: [],
    pending_replicated_registry_len: 0,
    pending_replicated_registry_ops: [],
    pending_replica_broadcast_len: 0,
    pending_replica_broadcast_ops: [],
    remote_shards: %{},
    peer_last_seen: %{},
    cluster_control_dirty: %{},
    authority_dirty_notified: MapSet.new(),
    pending_registry_reprojections: %{},
    monitors: %{},
    snapshot_transfers: %{},
    snapshot_staging_pool: [],
    snapshot_send: nil,
    snapshot_send_offsets: %{}
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: shard_name(name, shard_index))
  end

  def shard_name(name, shard_index), do: :"#{name}_replica_#{shard_index}"

  def shard_for(name, cluster, key) do
    num_shards = Group.get_config(name).num_shards
    index = :erlang.phash2({cluster, key}, num_shards)
    shard_name(name, index)
  end

  def shard_index_for(cluster, key, num_shards) do
    :erlang.phash2({cluster, key}, num_shards)
  end

  @doc false
  def local_request(shard_name, request, timeout)
      when (is_atom(shard_name) or is_pid(shard_name)) and
             (is_integer(timeout) or timeout == :infinity) do
    case GenServer.whereis(shard_name) do
      nil ->
        exit({:noproc, {GenServer, :call, [shard_name, request, timeout]}})

      pid ->
        do_local_request(pid, shard_name, request, timeout)
    end
  end

  @doc false
  def local_request_all(shard_names, request, timeout)
      when is_list(shard_names) and (is_integer(timeout) or timeout == :infinity) do
    requests = Enum.map(shard_names, &start_local_request(&1, request, timeout))

    try do
      Enum.each(requests, fn pending ->
        :ok = await_local_request(pending, timeout)
      end)
    after
      Enum.each(requests, &cancel_local_request/1)
    end

    :ok
  end

  @doc false
  def local_cast(shard_name, request) when is_atom(shard_name) or is_pid(shard_name) do
    case GenServer.whereis(shard_name) do
      nil -> :ok
      pid -> send(pid, {@local_request_tag, :noreply, request})
    end

    :ok
  end

  # =====================================================================
  # GenServer callbacks
  # =====================================================================

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    num_shards = Keyword.fetch!(opts, :num_shards)
    config = Group.get_config(name)

    Process.flag(:trap_exit, true)
    :net_kernel.monitor_nodes(true)

    # Register self as nil cluster member
    Data.add_cluster_node(name, [nil], node())

    state = %__MODULE__{
      name: name,
      shard_index: shard_index,
      num_shards: num_shards,
      replicated_pg_receiver_buffer_size: config.replicated_pg_receiver_buffer_size,
      replicated_pg_receiver_flush_interval: config.replicated_pg_receiver_flush_interval,
      replicated_registry_receiver_buffer_size: config.replicated_registry_receiver_buffer_size,
      replicated_registry_receiver_flush_interval:
        config.replicated_registry_receiver_flush_interval,
      replicated_sender_buffer_size: config.replicated_sender_buffer_size,
      replicated_sender_flush_interval: config.replicated_sender_flush_interval,
      replicated_pg_receiver_local_request_quota:
        config.replicated_pg_receiver_local_request_quota,
      replicated_oplog_max_entries: config.replicated_oplog_max_entries,
      replicated_snapshot_chunk_target_bytes: config.replicated_snapshot_chunk_target_bytes,
      replicated_anti_entropy_interval: config.replicated_anti_entropy_interval,
      replicated_peer_lease_timeout: config.replicated_peer_lease_timeout,
      replica_transport: elem(config.replica_transport, 0),
      replica_transport_opts: elem(config.replica_transport, 1)
    }

    state = schedule_anti_entropy(state)

    # Repair any interrupted multi-table journal/index mutation, complete
    # write-ahead records left unapplied by a shard crash, then rebuild local
    # process monitors from the surviving materialized tables.
    :ok = Data.repair_local_replica_journal(name, shard_index)
    state = replay_local_journal(state)
    :ok = Data.repair_shard_indexes(name, shard_index)
    {state, _events} = rebuild_registry_projections(state)

    _completed_clusters =
      Data.mark_closed_cluster_shard(
        name,
        Data.closed_local_cluster_epochs(name),
        shard_index
      )

    # Rebuild monitors from any surviving ETS data (after shard crash/restart)
    state = rebuild_monitors(state)

    # A shard can die after replica rows are materialized but before it handles
    # the peer's retirement. The ETS owner survives a shard restart, while the
    # in-memory lease map does not. Reconstruct every retained origin as a lease
    # candidate: live Groups answer the normal discovery probe below and refresh
    # the lease; permanently disappeared Groups are purged when it expires.
    restarted_at = monotonic_millis()
    retained_origins = Data.retained_replica_origins(name, shard_index)

    cluster_control_dirty =
      if shard_index == 0 do
        Enum.reduce(retained_origins, %{}, fn origin, dirty ->
          exact = Data.remote_cluster_epoch_exact_revision(name, origin)
          observed = Data.remote_cluster_epoch_observed_revision(name, origin)
          known_generation = Data.remote_generation(name, origin)
          hint = Data.remote_replica_authority_hint(name, origin)

          if (not is_nil(observed) and observed != exact) or
               (not is_nil(hint) and elem(hint, 0) != known_generation) do
            Map.put(dirty, origin, restarted_at)
          else
            dirty
          end
        end)
      else
        %{}
      end

    state = %{
      state
      | peer_last_seen: Map.new(retained_origins, &{&1, restarted_at}),
        cluster_control_dirty: cluster_control_dirty
    }

    log_once(state, fn -> "#{log_prefix(state)} started (shards=#{num_shards})" end)

    # Discover peers on all known nodes
    for remote_node <- Node.list() do
      send_remote_shard_message(
        state,
        remote_node,
        {:peer_connect, self(), shard_index, num_shards, Data.my_clusters(name)}
      )
    end

    {:ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    _state = flush_pending_replicated_message_barrier(state)
    :ok
  end

  # =====================================================================
  # Registration calls
  # =====================================================================

  @impl true
  def handle_call({:register, _, _, _, _, _} = request, _from, state) do
    {reply, state} = process_local_request(state, request)
    {:reply, reply, state}
  end

  def handle_call({:unregister, _, _, _} = request, _from, state) do
    {reply, state} = process_local_request(state, request)
    {:reply, reply, state}
  end

  # =====================================================================
  # Process group calls
  # =====================================================================

  def handle_call({:join, _, _, _, _, _} = request, _from, state) do
    {reply, state} = process_local_request(state, request)
    {:reply, reply, state}
  end

  def handle_call({:leave, _, _, _, _} = request, _from, state) do
    {reply, state} = process_local_request(state, request)
    {:reply, reply, state}
  end

  # =====================================================================
  # Cluster connect/disconnect (broadcast to all shards, rare operation)
  # =====================================================================

  def handle_call({:cluster_connect, _, _} = request, _from, state) do
    {reply, state} = process_local_request(state, request)
    {:reply, reply, state}
  end

  def handle_call({:cluster_connect, _} = request, _from, state) do
    {reply, state} = process_local_request(state, request)
    {:reply, reply, state}
  end

  def handle_call({:cluster_disconnect, _, _} = request, _from, state) do
    {reply, state} = process_local_request(state, request)
    {:reply, reply, state}
  end

  def handle_call({:cluster_disconnect, _} = request, _from, state) do
    {reply, state} = process_local_request(state, request)
    {:reply, reply, state}
  end

  # =====================================================================
  # Replication receive (handle_info)
  # =====================================================================

  @impl true
  def handle_info(
        {:replica_hello, remote_pid, version, generation, epoch_revision, cluster_epochs,
         transport_id, transport_descriptor},
        %{shard_index: 0} = state
      ) do
    state = flush_pending_replicated_message_barrier(state)
    remote_node = node(remote_pid)

    known_generation = Data.remote_generation(state.name, remote_node)

    hinted_generation =
      case Data.remote_replica_authority_hint(state.name, remote_node) do
        {hinted_generation, _revision} -> hinted_generation
        nil -> known_generation
      end

    observed_revision = Data.remote_cluster_epoch_observed_revision(state.name, remote_node)
    authoritative_revision = Data.remote_cluster_epoch_revision(state.name, remote_node)

    exact_revision = Data.remote_cluster_epoch_exact_revision(state.name, remote_node)

    stale_generation? =
      not is_nil(hinted_generation) and hinted_generation != generation and
        not WireProtocol.generation_newer?(generation, hinted_generation)

    stale_revision? =
      known_generation == generation and
        Enum.any?([observed_revision, authoritative_revision], fn
          revision when is_integer(revision) -> epoch_revision < revision
          _ -> false
        end)

    cond do
      version != WireProtocol.version() or transport_id != state.replica_transport.id() or
          not WireProtocol.valid_generation?(generation) ->
        Logger.error(
          "#{log_prefix_shard(state)} incompatible replica protocol/transport from #{inspect(remote_node)}"
        )

        {:noreply, state}

      stale_generation? or stale_revision? ->
        {:noreply, state}

      known_generation == generation and exact_revision == epoch_revision ->
        # Requests and heartbeats may race while one large authority snapshot
        # is being installed. Once this exact revision is present, another
        # identical hello is only a lease/descriptor refresh; reinstalling its
        # full epoch set would serialize every shard behind redundant ETS work.
        state =
          if replica_view_current?(state, remote_node) do
            state
          else
            install_current_replica_lane(state, remote_node, generation)
          end

        if replica_authority_current?(state, remote_node, generation, epoch_revision) and
             replica_view_current?(state, remote_node) do
          state = notify_replica_transport_peer_up(state, remote_node, transport_descriptor)

          {:noreply,
           %{
             state
             | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid),
               peer_last_seen: Map.put(state.peer_last_seen, remote_node, monotonic_millis()),
               cluster_control_dirty: Map.delete(state.cluster_control_dirty, remote_node)
           }}
        else
          {:noreply,
           state
           |> mark_cluster_control_dirty(remote_node)
           |> request_replica_authority(remote_node)}
        end

      true ->
        {:noreply,
         install_replica_authority(
           state,
           remote_pid,
           generation,
           epoch_revision,
           cluster_epochs,
           transport_id,
           transport_descriptor
         )}
    end
  end

  def handle_info(
        {:replica_hello, remote_pid, _version, _generation, _epoch_revision, _cluster_epochs,
         _transport_id, _transport_descriptor},
        state
      ) do
    # Full authority is installed only by shard 0. A full hello delivered to a
    # data lane cannot be used as its lane identity because remote_pid belongs
    # to the remote control shard, not this matching shard.
    {:noreply, request_replica_authority(state, node(remote_pid))}
  end

  def handle_info(
        {:replica_lane_hello, remote_pid, version, generation, epoch_revision, transport_id,
         transport_descriptor},
        state
      ) do
    state = flush_pending_replicated_message_barrier(state)
    remote_node = node(remote_pid)

    if version == WireProtocol.version() and transport_id == state.replica_transport.id() do
      state = observe_replica_authority_hint(state, remote_node, generation, epoch_revision)

      cond do
        replica_authority_current?(state, remote_node, generation, epoch_revision) and
            replica_view_current?(state, remote_node) ->
          state =
            state
            |> notify_replica_transport_peer_up(remote_node, transport_descriptor)
            |> put_remote_shard(remote_node, remote_pid)
            |> purge_remote_streams_outside_authority(remote_node)
            |> touch_replica_peer(remote_node)
            |> Map.update!(:cluster_control_dirty, &Map.delete(&1, remote_node))
            |> send_replica_heads(remote_node)

          {:noreply, state}

        replica_exact_authority_current?(state, remote_node, generation, epoch_revision) ->
          # The shared authority can arrive before this sibling is registered,
          # so shard-zero fanout is intentionally lossy at startup. Rebuild
          # this lane directly from the exact shared authority.
          {:noreply,
           state
           |> notify_replica_transport_peer_up(remote_node, transport_descriptor)
           |> put_remote_shard(remote_node, remote_pid)
           |> install_current_replica_lane(remote_node, generation)}

        true ->
          # A lane hello is only a hint until node-wide exact authority exists.
          # In particular, a delayed hello after retirement must not recreate
          # an unleased route that can live forever and suppress rediscovery.
          {:noreply, request_replica_authority(state, remote_node)}
      end
    else
      Logger.error(
        "#{log_prefix_shard(state)} incompatible replica protocol/transport from #{inspect(remote_node)}"
      )

      {:noreply, state}
    end
  end

  def handle_info(
        {:replica_authority_installed_local, remote_node, generation, epoch_revision,
         old_generation, stale_epochs},
        state
      ) do
    state = flush_pending_replicated_message_barrier(state)

    if replica_authority_current?(state, remote_node, generation, epoch_revision) do
      state = maybe_purge_remote_generation(state, remote_node, old_generation, generation)

      state =
        if old_generation == generation do
          state
          |> purge_closed_remote_epochs(remote_node, stale_epochs)
          |> purge_remote_streams_outside_authority(remote_node)
        else
          state
        end

      state = install_replica_view(state, remote_node, generation)

      state = %{
        state
        | cluster_control_dirty: Map.delete(state.cluster_control_dirty, remote_node),
          authority_dirty_notified: MapSet.delete(state.authority_dirty_notified, remote_node)
      }

      state =
        if Map.has_key?(state.remote_shards, remote_node) do
          state
          |> touch_replica_peer(remote_node)
          |> send_replica_heads(remote_node)
        else
          # A lane hello can legitimately outrun shard zero's exact authority.
          # The hello is not retained as a route, because a delayed hello after
          # retirement must not recreate an unleased peer. Once exact authority
          # reaches this lane, repeat shard-local discovery immediately instead
          # of waiting for the next anti-entropy probe.
          send_remote_shard_message(
            state,
            remote_node,
            {:peer_connect, self(), state.shard_index, state.num_shards,
             Data.my_clusters(state.name)}
          )

          state
        end

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_info(
        {:replica_cluster_open, remote_pid, generation, revision, epochs} = control,
        state
      ) do
    state = flush_pending_replicated_message_barrier(state)

    if state.shard_index == 0 do
      remote_node = node(remote_pid)

      controls =
        collect_replica_cluster_controls(
          :replica_cluster_open,
          remote_pid,
          generation,
          [{revision, epochs}],
          state.replicated_sender_buffer_size - 1
        )

      case accepted_replica_cluster_epochs(state, remote_node, generation, controls) do
        {:accept, expected_revision, observed_revision, epochs} ->
          case Data.put_remote_cluster_epochs(
                 state.name,
                 state.shard_index,
                 remote_node,
                 generation,
                 expected_revision,
                 observed_revision,
                 epochs
               ) do
            {:ok, stale} ->
              shared =
                Enum.filter(epochs, fn {cluster, _epoch} ->
                  node() in Data.cluster_nodes(state.name, cluster)
                end)

              Data.add_cluster_node(state.name, Enum.map(shared, &elem(&1, 0)), remote_node)

              fan_out_to_siblings(
                state,
                {:replica_cluster_open_control_local, remote_node, generation, observed_revision,
                 epochs, stale, Enum.map(shared, &elem(&1, 0))}
              )

              state =
                state
                |> mark_authority_dirty(remote_node)
                |> purge_closed_remote_epochs(remote_node, stale)
                |> purge_superseded_remote_streams(remote_node, epochs)
                |> purge_remote_streams_outside_authority(remote_node)

              state = install_replica_view(state, remote_node, generation)
              state = send_replica_heads(state, remote_node, Enum.map(shared, &elem(&1, 0)))
              {:noreply, take_one_local_request_turn(state)}

            :stale ->
              {:noreply,
               state
               |> mark_authority_dirty(remote_node)
               |> request_replica_authority(remote_node)
               |> take_one_local_request_turn()}
          end

        :stale ->
          {:noreply, take_one_local_request_turn(state)}

        :refresh ->
          {:noreply,
           state
           |> request_replica_authority(remote_node)
           |> take_one_local_request_turn()}

        {:gap, observed_revision} ->
          :ok =
            Data.observe_remote_cluster_epoch_revision(
              state.name,
              remote_node,
              observed_revision
            )

          {:noreply,
           state
           |> mark_authority_dirty(remote_node)
           |> request_replica_authority(remote_node)
           |> take_one_local_request_turn()}
      end
    else
      # Incremental authority is node-wide and therefore has one local owner.
      # Forward a control that arrived on another lane instead of racing its
      # check/update against shard 0.
      _ = send_local_control_message(state, control)
      {:noreply, take_one_local_request_turn(state)}
    end
  end

  def handle_info({:replica_authority_dirty_local, remote_node}, %{shard_index: 0} = state) do
    {:noreply, mark_cluster_control_dirty(state, remote_node)}
  end

  def handle_info({:replica_authority_dirty_local, remote_node}, state) do
    _ =
      send_local_control_message(state, {:replica_authority_dirty_local, remote_node})

    {:noreply, state}
  end

  def handle_info(
        {:replica_cluster_open_control_local, remote_node, generation, revision, epochs, stale,
         shared},
        state
      ) do
    state = flush_pending_replicated_message_barrier(state)

    state =
      if replica_authority_current?(state, remote_node, generation, revision) do
        state =
          state
          |> purge_closed_remote_epochs(remote_node, stale)
          |> purge_superseded_remote_streams(remote_node, epochs)
          |> purge_remote_streams_outside_authority(remote_node)

        state
        |> install_replica_view(remote_node, generation)
        |> send_replica_heads(remote_node, shared)
      else
        state
      end

    {:noreply, take_one_local_request_turn(state)}
  end

  def handle_info(
        {:replica_cluster_close, remote_pid, generation, revision, epochs},
        %{shard_index: 0} = state
      ) do
    state = flush_pending_replicated_message_barrier(state)
    remote_node = node(remote_pid)

    controls =
      collect_replica_cluster_controls(
        :replica_cluster_close,
        remote_pid,
        generation,
        [{revision, epochs}],
        state.replicated_sender_buffer_size - 1
      )

    case accepted_replica_cluster_epochs(state, remote_node, generation, controls) do
      {:accept, expected_revision, observed_revision, epochs} ->
        case Data.close_remote_cluster_epochs(
               state.name,
               0,
               remote_node,
               generation,
               expected_revision,
               observed_revision,
               epochs
             ) do
          {:ok, closed} ->
            if state.shard_index == 0 do
              Data.remove_cluster_node(state.name, Enum.map(closed, &elem(&1, 0)), remote_node)
            end

            fan_out_to_siblings(
              state,
              {:replica_cluster_close_control_local, remote_node, generation, observed_revision,
               closed}
            )

            state =
              state
              |> mark_cluster_control_dirty(remote_node)
              |> purge_closed_remote_epochs(remote_node, closed)
              |> purge_remote_streams_outside_authority(remote_node)

            state = install_replica_view(state, remote_node, generation)
            {:noreply, take_one_local_request_turn(state)}

          :stale ->
            {:noreply,
             state
             |> mark_authority_dirty(remote_node)
             |> request_replica_authority(remote_node)
             |> take_one_local_request_turn()}
        end

      :stale ->
        {:noreply, take_one_local_request_turn(state)}

      :refresh ->
        {:noreply,
         state
         |> request_replica_authority(remote_node)
         |> take_one_local_request_turn()}

      {:gap, observed_revision} ->
        :ok =
          Data.observe_remote_cluster_epoch_revision(
            state.name,
            remote_node,
            observed_revision
          )

        {:noreply,
         state
         |> mark_cluster_control_dirty(remote_node)
         |> request_replica_authority(remote_node)
         |> take_one_local_request_turn()}
    end
  end

  def handle_info({:replica_cluster_close, remote_pid, _generation, _revision, _epochs}, state) do
    {:noreply, request_replica_authority(state, node(remote_pid))}
  end

  def handle_info(
        {:replica_cluster_close_control_local, remote_node, generation, revision, closed},
        state
      ) do
    state = flush_pending_replicated_message_barrier(state)

    state =
      if replica_authority_current?(state, remote_node, generation, revision) do
        state =
          state
          |> purge_closed_remote_epochs(remote_node, closed)
          |> purge_remote_streams_outside_authority(remote_node)

        install_replica_view(state, remote_node, generation)
      else
        state
      end

    {:noreply, take_one_local_request_turn(state)}
  end

  def handle_info(
        {:replica_heartbeat, remote_pid, version, generation, epoch_revision, transport_id,
         transport_descriptor},
        state
      ) do
    remote_node = node(remote_pid)

    compatible? =
      version == WireProtocol.version() and transport_id == state.replica_transport.id()

    state =
      if compatible? do
        observe_replica_authority_hint(state, remote_node, generation, epoch_revision)
      else
        state
      end

    state =
      cond do
        compatible? and
          replica_authority_current?(state, remote_node, generation, epoch_revision) and
            replica_view_current?(state, remote_node) ->
          state
          |> notify_replica_transport_peer_up(remote_node, transport_descriptor)
          |> put_remote_shard(remote_node, remote_pid)
          |> touch_replica_peer(remote_node)

        compatible? and
            replica_authority_current?(state, remote_node, generation, epoch_revision) ->
          state

        true ->
          request_replica_authority(state, remote_node)
      end

    {:noreply, state}
  end

  def handle_info({:replica_hello_request, remote_pid}, state) do
    if state.shard_index == 0 do
      {:noreply, send_replica_hello(state, node(remote_pid))}
    else
      _ = send_local_control_message(state, {:replica_hello_request, remote_pid})
      {:noreply, state}
    end
  end

  def handle_info({:group_replica_frame, remote_pid, message}, state) when is_pid(remote_pid) do
    remote_node = node(remote_pid)
    state = handle_replica_message(state, remote_node, message)
    {:noreply, take_priority_turn(state)}
  end

  def handle_info({:group_replica_frame, remote_node, message}, state)
      when is_atom(remote_node) do
    state = handle_replica_message(state, remote_node, message)
    {:noreply, take_priority_turn(state)}
  end

  def handle_info({:group_replica_batch, remote_node, messages}, state)
      when is_atom(remote_node) and is_list(messages) do
    {turn, remaining} = Enum.split(messages, @incoming_batch_quota)
    state = Enum.reduce(turn, state, &handle_replica_message(&2, remote_node, &1))

    if remaining != [] do
      send(self(), {:group_replica_batch, remote_node, remaining})
    end

    {:noreply, take_priority_turn(state)}
  end

  def handle_info(
        {:replica_snapshot_send_complete, token, worker, snapshot_key, result},
        %{snapshot_send: {worker, token, snapshot_key}} = state
      ) do
    offsets =
      case result do
        :complete ->
          Map.delete(state.snapshot_send_offsets, snapshot_key)

        {:resume, chunk_index} ->
          if current_snapshot_send?(state, snapshot_key) do
            Map.put(state.snapshot_send_offsets, snapshot_key, {:chunk, chunk_index})
          else
            Map.delete(state.snapshot_send_offsets, snapshot_key)
          end

        {:resume_commit, manifest} ->
          if current_snapshot_send?(state, snapshot_key) do
            Map.put(state.snapshot_send_offsets, snapshot_key, {:commit, manifest})
          else
            Map.delete(state.snapshot_send_offsets, snapshot_key)
          end

        :retry ->
          if current_snapshot_send?(state, snapshot_key) do
            state.snapshot_send_offsets
          else
            Map.delete(state.snapshot_send_offsets, snapshot_key)
          end
      end

    {:noreply, %{state | snapshot_send: nil, snapshot_send_offsets: offsets}}
  end

  def handle_info(
        {:replica_snapshot_send_complete, _token, _worker, _snapshot_key, _result},
        state
      ) do
    {:noreply, state}
  end

  def handle_info({@anti_entropy_timer, ref}, state) do
    state =
      if state.anti_entropy_ref == ref do
        state
        |> expire_stale_snapshot_transfers()
        |> expire_stale_replica_peers()
        |> probe_replica_peers()
        |> request_quiet_cluster_hellos()
        |> broadcast_replica_heartbeats()
        |> broadcast_replica_heads()
        |> schedule_anti_entropy()
      else
        state
      end

    {:noreply, state}
  end

  def handle_info({@local_request_tag, caller_pid, ref, request}, state)
      when is_pid(caller_pid) and is_reference(ref) do
    {:noreply, process_local_request_turn(state, [{{:send, caller_pid, ref}, request}])}
  end

  def handle_info({@local_request_tag, alias_ref, request}, state) when is_reference(alias_ref) do
    {:noreply, process_local_request_turn(state, [{{:alias, alias_ref}, request}])}
  end

  def handle_info({@local_request_tag, :noreply, request}, state) do
    {:noreply, process_local_request_turn(state, [{:noreply, request}])}
  end

  # =====================================================================
  # Peer discovery protocol
  # =====================================================================

  def handle_info(
        {:peer_connect, remote_pid, remote_shard_index, remote_num_shards, remote_clusters},
        state
      )
      when remote_shard_index == state.shard_index do
    state = flush_pending_replicated_message_barrier(state)

    if remote_num_shards != state.num_shards do
      raise "Group shard count mismatch: local=#{state.num_shards} remote=#{remote_num_shards} from #{node(remote_pid)}"
    end

    %{name: name, shard_index: shard} = state
    remote_node = node(remote_pid)

    # Compute shared clusters for diagnostics only. The generation-fenced hello
    # is the sole authority that mutates peer and cluster membership. Keeping
    # discovery hints side-effect free prevents a delayed pre-restart
    # peer_connect from permanently re-adding stale cluster rows.
    my_clusters = Data.my_clusters(name)
    shared = compute_shared_clusters(my_clusters, remote_clusters)

    # Replica peers are addressed by registered `{name, node}` and established
    # only by replica_hello. Do not remotely monitor the shard PID: creating a
    # remote monitor itself emits a distribution signal and may suspend on a
    # busy dist connection.

    # Send ack with our cluster list
    send_to_peer(
      state,
      remote_node,
      {:peer_connect_ack, self(), shard, state.num_shards, my_clusters}
    )

    send_replica_hello(state, remote_node)

    log_once(state, fn ->
      "#{log_prefix(state)} peer_connect from #{remote_node} (#{length(shared)} shared clusters)"
    end)

    {:noreply, state}
  end

  def handle_info({:peer_connect, _remote_pid, _other_shard, _num_shards, _clusters}, state) do
    state = flush_pending_replicated_message_barrier(state)
    # Wrong shard index, ignore
    {:noreply, state}
  end

  def handle_info(
        {:peer_connect_ack, remote_pid, remote_shard_index, remote_num_shards, remote_clusters},
        state
      )
      when remote_shard_index == state.shard_index do
    state = flush_pending_replicated_message_barrier(state)

    if remote_num_shards != state.num_shards do
      raise "Group shard count mismatch: local=#{state.num_shards} remote=#{remote_num_shards} from #{node(remote_pid)}"
    end

    %{name: name} = state
    remote_node = node(remote_pid)

    # Discovery acknowledgements are hints only; replica_hello is the sole
    # generation-fenced authority for peer and cluster membership.
    my_clusters = Data.my_clusters(name)
    shared = compute_shared_clusters(my_clusters, remote_clusters)

    log_once(state, fn ->
      "#{log_prefix(state)} peer_connect_ack from #{remote_node} (#{length(shared)} shared clusters)"
    end)

    send_replica_hello(state, remote_node)

    {:noreply, state}
  end

  def handle_info({:peer_connect_ack, _remote_pid, _other_shard, _num_shards, _clusters}, state) do
    state = flush_pending_replicated_message_barrier(state)
    {:noreply, state}
  end

  # =====================================================================
  # Node up/down
  # =====================================================================

  def handle_info({:nodeup, remote_node}, state) do
    state = flush_pending_replicated_message_barrier(state)
    %{shard_index: shard, name: name} = state

    send_remote_shard_message(
      state,
      remote_node,
      {:peer_connect, self(), shard, state.num_shards, Data.my_clusters(name)}
    )

    {:noreply, state}
  end

  def handle_info({:nodedown, dead_node}, state) do
    state = flush_pending_replicated_message_barrier(state)
    state = discard_snapshot_transfers_for_source(state, dead_node)
    state = discard_snapshot_send_offsets_for_target(state, dead_node)
    state = discard_pending_registry_reprojections(state, dead_node)
    %{name: name, shard_index: shard} = state

    # Cursor absence is the durable restart marker for an incomplete or
    # retiring remote stream. Clear it before touching materialized rows so a
    # shard crash at any later purge step finishes that retirement on restart.
    Data.delete_replica_cursors_for_origin(name, shard, dead_node)

    # Remove cluster memberships from shared tables. Every shard calls this
    # unconditionally (not just shard 0) to handle the race where a non-zero
    # shard processes a late peer_connect from the dead node (re-adding it to
    # cluster_nodes) AFTER shard 0's nodedown already cleaned it. Since :bag
    # delete_object is idempotent, redundant calls from multiple shards are safe.
    Data.purge_cluster_node(name, dead_node)

    # Purge all data from the dead node
    {purged_reg, purged_pg} = Data.purge_node(name, shard, dead_node)
    affected_claims = Data.purge_registry_claims_for_origin(name, shard, dead_node)

    log_once(state, fn ->
      "#{log_prefix(state)} nodedown #{dead_node} (purged #{length(purged_reg)} reg, #{length(purged_pg)} pg entries)"
    end)

    events = build_purged_events(name, purged_reg, purged_pg, :nodedown)

    {state, events} =
      Enum.reduce(affected_claims, {state, events}, fn {cluster, key}, {acc, inner_events} ->
        reconcile_registry_projection(acc, cluster, key, :nodedown, inner_events)
      end)

    notify_monitors(name, events)

    state = %{
      state
      | remote_shards: Map.delete(state.remote_shards, dead_node),
        peer_last_seen: Map.delete(state.peer_last_seen, dead_node),
        cluster_control_dirty: Map.delete(state.cluster_control_dirty, dead_node),
        authority_dirty_notified: MapSet.delete(state.authority_dirty_notified, dead_node)
    }

    Data.delete_remote_replica_info(name, shard, dead_node)

    if function_exported?(state.replica_transport, :peer_down, 4) do
      :ok =
        state.replica_transport.peer_down(
          name,
          dead_node,
          shard,
          state.replica_transport_opts
        )
    end

    {:noreply, state}
  end

  # =====================================================================
  # Process DOWN
  # =====================================================================

  def handle_info({:DOWN, _mref, :process, pid, reason}, state) do
    state = flush_pending_replicated_message_barrier(state)
    %{name: name, shard_index: shard} = state

    # Replica shards intentionally never create remote process monitors: their
    # liveness is generation/lease fenced and remote monitoring can itself
    # suspend on a busy distribution connection. Consequently every genuine
    # DOWN handled here belongs to a locally owned registry/PG process.
    if Map.has_key?(state.monitors, pid) do
      {downs, monitors} =
        collect_local_process_downs(
          [{pid, reason}],
          state.monitors,
          @process_down_batch_size - 1
        )

      pids = Enum.map(downs, &elem(&1, 0))
      reason_by_pid = Map.new(downs)
      {visible_reg, pending_pg} = Data.entries_for_pids(name, shard, pids)

      claimed_reg =
        Data.local_registry_claims_by_pids(name, shard, pids)
        |> Enum.map(fn {pid, cluster, key, meta, _generation, _epoch} ->
          {pid, cluster, key, meta}
        end)

      pending_reg = Enum.uniq(visible_reg ++ claimed_reg)

      sequenced_downs =
        append_process_down_records(state, reason_by_pid, pending_reg, pending_pg)

      {purged_reg, purged_pg} = Data.delete_all_for_pids(name, shard, pids)

      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} process_down_batch pids=#{length(downs)} (#{length(purged_reg) + length(purged_pg)} entries cleaned)"
      end)

      state = finish_process_down_records(state, sequenced_downs)

      affected_registry_keys =
        pending_reg
        |> Enum.map(fn {_pid, cluster, key, _meta} -> {cluster, key} end)
        |> Enum.uniq()

      {state, projection_events} =
        reconcile_registry_keys(state, affected_registry_keys, reason, [])

      events =
        projection_events ++
          build_process_down_events(name, purged_reg, purged_pg, reason_by_pid)

      notify_monitors(name, events)
      state = %{state | monitors: Map.drop(monitors, pids)}
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_info({:group_dispatch, pids, message}, state) do
    state = flush_pending_replicated_message_barrier(state)
    for pid <- pids, do: send(pid, message)
    {:noreply, state}
  end

  def handle_info({@replicated_pg_receiver_flush_timer, flush_ref}, state) do
    state =
      if state.pending_replicated_pg_flush_ref == flush_ref do
        state
        |> flush_pending_replicated_pg()
        |> take_priority_turn()
      else
        state
      end

    {:noreply, state}
  end

  def handle_info({@replicated_registry_receiver_flush_timer, flush_ref}, state) do
    state =
      if state.pending_replicated_registry_flush_ref == flush_ref do
        state
        |> flush_pending_replicated_registry()
        |> take_priority_turn()
      else
        state
      end

    {:noreply, state}
  end

  def handle_info({@replica_broadcast_flush_timer, flush_ref}, state) do
    state =
      if state.pending_replica_broadcast_flush_ref == flush_ref do
        flush_pending_replica_broadcast(state)
      else
        state
      end

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    state = flush_pending_replicated_message_barrier(state)
    {:noreply, state}
  end

  # =====================================================================
  # Internal helpers
  # =====================================================================

  defp start_local_request(shard_name, request, timeout) do
    case GenServer.whereis(shard_name) do
      nil ->
        {:noproc, shard_name, request, timeout}

      pid ->
        alias_ref = Process.alias()
        mref = Process.monitor(pid)
        send(pid, {@local_request_tag, alias_ref, request})
        {:pending, pid, shard_name, request, alias_ref, mref}
    end
  end

  defp await_local_request({:noproc, shard_name, request, timeout}, _wait_timeout) do
    exit({:noproc, {GenServer, :call, [shard_name, request, timeout]}})
  end

  defp await_local_request(
         {:pending, pid, shard_name, request, alias_ref, mref},
         :infinity
       ) do
    receive do
      {@local_reply_tag, ^alias_ref, reply} ->
        cancel_local_request({:pending, pid, shard_name, request, alias_ref, mref})
        reply

      {:DOWN, ^mref, :process, ^pid, reason} ->
        Process.unalias(alias_ref)
        exit({reason, {GenServer, :call, [shard_name, request, :infinity]}})
    end
  end

  defp await_local_request(
         {:pending, pid, shard_name, request, alias_ref, mref} = pending,
         timeout
       )
       when is_integer(timeout) do
    receive do
      {@local_reply_tag, ^alias_ref, reply} ->
        cancel_local_request(pending)
        reply

      {:DOWN, ^mref, :process, ^pid, reason} ->
        Process.unalias(alias_ref)
        exit({reason, {GenServer, :call, [shard_name, request, timeout]}})
    after
      timeout ->
        cancel_local_request(pending)
        exit({:timeout, {GenServer, :call, [shard_name, request, timeout]}})
    end
  end

  defp cancel_local_request({:noproc, _shard_name, _request, _timeout}), do: :ok

  defp cancel_local_request({:pending, _pid, _shard_name, _request, alias_ref, mref}) do
    Process.demonitor(mref, [:flush])
    Process.unalias(alias_ref)

    receive do
      {@local_reply_tag, ^alias_ref, _reply} -> :ok
    after
      0 -> :ok
    end
  end

  defp do_local_request(pid, shard_name, request, :infinity) when is_pid(pid) do
    alias_ref = Process.alias()
    mref = Process.monitor(pid)
    send(pid, {@local_request_tag, alias_ref, request})

    receive do
      {@local_reply_tag, ^alias_ref, reply} ->
        Process.demonitor(mref, [:flush])
        Process.unalias(alias_ref)
        reply

      {:DOWN, ^mref, :process, ^pid, reason} ->
        Process.unalias(alias_ref)
        exit({reason, {GenServer, :call, [shard_name, request, :infinity]}})
    end
  end

  defp do_local_request(pid, shard_name, request, timeout)
       when is_pid(pid) and is_integer(timeout) do
    alias_ref = Process.alias()
    mref = Process.monitor(pid)
    send(pid, {@local_request_tag, alias_ref, request})

    receive do
      {@local_reply_tag, ^alias_ref, reply} ->
        Process.demonitor(mref, [:flush])
        Process.unalias(alias_ref)
        reply

      {:DOWN, ^mref, :process, ^pid, reason} ->
        Process.unalias(alias_ref)
        exit({reason, {GenServer, :call, [shard_name, request, timeout]}})
    after
      timeout ->
        Process.demonitor(mref, [:flush])
        Process.unalias(alias_ref)

        receive do
          {@local_reply_tag, ^alias_ref, _reply} -> :ok
        after
          0 -> :ok
        end

        exit({:timeout, {GenServer, :call, [shard_name, request, timeout]}})
    end
  end

  defp reply_local_request({:send, caller_pid, ref}, reply)
       when is_pid(caller_pid) and is_reference(ref) do
    send(caller_pid, {@local_reply_tag, ref, reply})
    :ok
  end

  defp reply_local_request({:alias, alias_ref}, reply) when is_reference(alias_ref) do
    send(alias_ref, {@local_reply_tag, alias_ref, reply})
    :ok
  end

  defp reply_local_request(:noreply, _reply), do: :ok

  defp process_local_request_turn(
         state,
         initial_messages
       ) do
    remaining =
      max(state.replicated_pg_receiver_local_request_quota - length(initial_messages), 0)

    messages = collect_local_request_messages(initial_messages, remaining)
    process_local_request_messages(state, messages)
  end

  defp collect_local_request_messages(acc, 0), do: Enum.reverse(acc)

  defp collect_local_request_messages(acc, remaining) do
    receive do
      {@local_request_tag, caller_pid, ref, request}
      when is_pid(caller_pid) and is_reference(ref) ->
        collect_local_request_messages([{{:send, caller_pid, ref}, request} | acc], remaining - 1)

      {@local_request_tag, alias_ref, request} when is_reference(alias_ref) ->
        collect_local_request_messages([{{:alias, alias_ref}, request} | acc], remaining - 1)

      {@local_request_tag, :noreply, request} ->
        collect_local_request_messages([{:noreply, request} | acc], remaining - 1)
    after
      0 ->
        Enum.reverse(acc)
    end
  end

  defp process_local_request_messages(state, []), do: flush_pending_replicated_barrier(state)

  defp process_local_request_messages(state, messages) do
    state = flush_pending_replicated_barrier(state)

    Enum.reduce(split_local_request_segments(messages), state, fn segment, acc_state ->
      process_local_request_segment(acc_state, segment)
    end)
  end

  defp split_local_request_segments(messages), do: do_split_local_request_segments(messages, [])

  defp do_split_local_request_segments([], acc), do: Enum.reverse(acc)

  defp do_split_local_request_segments([message | rest], acc) do
    {segment, rest} = take_local_request_segment(message, rest)
    do_split_local_request_segments(rest, [segment | acc])
  end

  defp take_local_request_segment({_reply_to, request} = message, rest) do
    if local_request_domain(request) == :pg do
      do_take_local_request_segment(rest, [message])
    else
      {[message], rest}
    end
  end

  defp do_take_local_request_segment([], acc), do: {Enum.reverse(acc), []}

  defp do_take_local_request_segment([{_reply_to, request} = message | rest], acc) do
    if local_request_domain(request) == :pg do
      do_take_local_request_segment(rest, [message | acc])
    else
      {Enum.reverse(acc), [message | rest]}
    end
  end

  defp process_local_request_segment(state, [{_reply_to, request} | _] = messages) do
    case local_request_domain(request) do
      :pg -> process_pg_local_request_batch(state, messages)
      :other -> process_sequential_local_request_messages(state, messages)
    end
  end

  defp process_sequential_local_request_messages(state, messages) do
    Enum.reduce(messages, state, fn {reply_to, request}, acc_state ->
      {reply, acc_state} = process_local_request_without_barrier(acc_state, request)
      :ok = reply_local_request(reply_to, reply)
      acc_state
    end)
  end

  defp process_local_request(state, request) do
    state = flush_pending_replicated_barrier(state)
    process_local_request_without_barrier(state, request)
  end

  defp process_local_request_without_barrier(state, request) do
    with :ok <- validate_local_mutation_epoch(state, request) do
      case request do
        {:register, cluster, _epoch, key, pid, meta} ->
          do_register(state, cluster, key, pid, meta)

        {:unregister, cluster, _epoch, key} ->
          do_unregister(state, cluster, key)

        {:join, cluster, _epoch, key, pid, meta} ->
          do_join(state, cluster, key, pid, meta)

        {:leave, cluster, _epoch, key, pid} ->
          do_leave(state, cluster, key, pid)

        {:cluster_connect, clusters} ->
          do_cluster_connect(state, clusters)

        {:cluster_connect, clusters, epochs} ->
          do_cluster_connect(state, clusters, epochs)

        {:cluster_disconnect, clusters} ->
          do_cluster_disconnect(state, clusters)

        {:cluster_disconnect, clusters, epochs} ->
          do_cluster_disconnect(state, clusters, epochs)

        _ ->
          {{:error, :invalid_local_request}, state}
      end
    else
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp validate_local_mutation_epoch(state, request) do
    case request do
      {op, cluster, epoch, _key, _pid, _meta} when op in [:register, :join] ->
        validate_local_mutation_epoch(state, cluster, epoch)

      {op, cluster, epoch, _key} when op == :unregister ->
        validate_local_mutation_epoch(state, cluster, epoch)

      {op, cluster, epoch, _key, _pid} when op == :leave ->
        validate_local_mutation_epoch(state, cluster, epoch)

      {op, _cluster, _key, _pid, _meta} when op in [:register, :join] ->
        {:error, :stale_cluster_epoch}

      {op, _cluster, _key} when op == :unregister ->
        {:error, :stale_cluster_epoch}

      {op, _cluster, _key, _pid} when op == :leave ->
        {:error, :stale_cluster_epoch}

      _ ->
        :ok
    end
  end

  defp validate_local_mutation_epoch(state, cluster, epoch) do
    if Data.local_cluster_epoch(state.name, cluster) == epoch do
      :ok
    else
      {:error, :stale_cluster_epoch}
    end
  end

  defp flush_pending_replicated_barrier(
         %{pending_replicated_pg_len: 0, pending_replicated_registry_len: 0} = state
       ),
       do: state

  defp flush_pending_replicated_barrier(
         %{pending_replicated_pg_len: 0, pending_replicated_registry_len: len} = state
       )
       when len > 0,
       do: flush_pending_replicated_registry(state)

  defp flush_pending_replicated_barrier(
         %{pending_replicated_pg_len: len, pending_replicated_registry_len: 0} = state
       )
       when len > 0,
       do: flush_pending_replicated_pg(state)

  defp flush_pending_replicated_barrier(state) do
    if state.pending_replicated_pg_started_at <= state.pending_replicated_registry_started_at do
      state
      |> flush_pending_replicated_pg()
      |> flush_pending_replicated_registry()
    else
      state
      |> flush_pending_replicated_registry()
      |> flush_pending_replicated_pg()
    end
  end

  defp flush_pending_replicated_pg_barrier(%{pending_replicated_pg_len: 0} = state), do: state
  defp flush_pending_replicated_pg_barrier(state), do: flush_pending_replicated_pg(state)

  defp flush_pending_replicated_registry_barrier(%{pending_replicated_registry_len: 0} = state),
    do: state

  defp flush_pending_replicated_registry_barrier(state),
    do: flush_pending_replicated_registry(state)

  defp flush_pending_replicated_message_barrier(state) do
    state
    |> flush_pending_replicated_sender_barrier()
    |> flush_pending_replicated_barrier()
  end

  defp flush_pending_replicated_sender_barrier(%{pending_replica_broadcast_len: 0} = state),
    do: state

  defp flush_pending_replicated_sender_barrier(state), do: flush_pending_replica_broadcast(state)

  defp process_pg_local_request_batch(state, messages) do
    %{name: name, shard_index: shard} = state
    local_node = node()

    {entries, replies, events, broadcasts, new_monitors, maybe_demonitor_pids} =
      Enum.reduce(
        messages,
        {%{}, [], [], [], %{}, MapSet.new()},
        fn {reply_to, request},
           {entries, replies, events, broadcasts, new_monitors, maybe_demonitor_pids} ->
          if validate_local_mutation_epoch(state, request) != :ok do
            {entries, [{reply_to, {:error, :stale_cluster_epoch}} | replies], events, broadcasts,
             new_monitors, maybe_demonitor_pids}
          else
            case request do
              {:join, cluster, _epoch, key, pid, meta} ->
                member = {cluster, key, pid}
                {initial, current} = local_pg_batch_entry(entries, name, shard, member)

                case current do
                  nil ->
                    time = System.system_time()
                    new_monitors = ensure_local_batch_monitor(state, new_monitors, pid)

                    {
                      Map.put(entries, member, {initial, {meta, time, local_node}}),
                      [{reply_to, :ok} | replies],
                      [
                        build_event(name, :joined, key, pid, meta, %{
                          previous_meta: nil,
                          cluster: cluster
                        })
                        | events
                      ],
                      [
                        {:join, cluster, key, pid, meta, time, :join, node(pid)} | broadcasts
                      ],
                      new_monitors,
                      maybe_demonitor_pids
                    }

                  {old_meta, _time, _node} when old_meta == meta ->
                    {entries, [{reply_to, :ok} | replies], events, broadcasts, new_monitors,
                     maybe_demonitor_pids}

                  {old_meta, _time, _node} ->
                    time = System.system_time()

                    {
                      Map.put(entries, member, {initial, {meta, time, local_node}}),
                      [{reply_to, :ok} | replies],
                      [
                        build_event(name, :joined, key, pid, meta, %{
                          previous_meta: old_meta,
                          cluster: cluster
                        })
                        | events
                      ],
                      [
                        {:join, cluster, key, pid, meta, time, :update, node(pid)} | broadcasts
                      ],
                      new_monitors,
                      maybe_demonitor_pids
                    }
                end

              {:leave, cluster, _epoch, key, pid} ->
                member = {cluster, key, pid}
                {initial, current} = local_pg_batch_entry(entries, name, shard, member)

                case current do
                  nil ->
                    {entries, [{reply_to, {:error, :not_in_group}} | replies], events, broadcasts,
                     new_monitors, maybe_demonitor_pids}

                  {meta, _time, _node} ->
                    {
                      Map.put(entries, member, {initial, nil}),
                      [{reply_to, :ok} | replies],
                      [
                        build_event(name, :left, key, pid, meta, %{
                          reason: :leave,
                          cluster: cluster
                        })
                        | events
                      ],
                      [
                        {:leave, cluster, key, pid, meta, :leave} | broadcasts
                      ],
                      new_monitors,
                      MapSet.put(maybe_demonitor_pids, pid)
                    }
                end
            end
          end
        end
      )

    sequenced_broadcasts =
      broadcasts
      |> Enum.reverse()
      |> Enum.map(&append_local_replica_record(state, &1))

    {insert_entries, delete_entries} = pg_batch_diff(entries)
    Data.pg_delete_many(name, shard, delete_entries)
    Data.pg_insert_many(name, shard, insert_entries)
    state = finalize_local_batch_monitors(state, new_monitors, maybe_demonitor_pids)

    state =
      Enum.reduce(sequenced_broadcasts, state, fn record, acc ->
        finish_local_replica_record(acc, record, :pg)
      end)

    notify_monitors(name, events)
    reply_local_requests(replies)
    state
  end

  defp registry_batch_entry(entries, name, shard, entry) do
    case Map.fetch(entries, entry) do
      {:ok, {initial, current}} ->
        {initial, current}

      :error ->
        {cluster, key} = entry

        current =
          case Data.registry_lookup(name, shard, cluster, key) do
            nil -> nil
            {pid, meta, time, entry_node} -> {pid, meta, time, entry_node}
          end

        {current, current}
    end
  end

  defp local_pg_batch_entry(entries, name, shard, member) do
    case Map.fetch(entries, member) do
      {:ok, {initial, current}} ->
        {initial, current}

      :error ->
        {cluster, key, pid} = member

        current =
          case Data.pg_lookup(name, shard, cluster, key, pid) do
            nil -> nil
            {meta, time, entry_node} -> {meta, time, entry_node}
          end

        {current, current}
    end
  end

  defp registry_batch_diff(entries) do
    Enum.reduce(entries, {[], []}, fn
      {{_cluster, _key}, {initial, current}}, {insert_entries, delete_entries}
      when current == initial ->
        {insert_entries, delete_entries}

      {{cluster, key}, {{pid, _meta, _time, _node}, nil}}, {insert_entries, delete_entries} ->
        {insert_entries, [{cluster, key, pid} | delete_entries]}

      {{cluster, key}, {nil, {pid, meta, time, entry_node}}}, {insert_entries, delete_entries} ->
        {[{cluster, key, pid, meta, time, entry_node} | insert_entries], delete_entries}

      {{cluster, key},
       {{old_pid, _old_meta, _old_time, _old_node}, {pid, meta, time, entry_node}}},
      {insert_entries, delete_entries} ->
        delete_entries =
          if old_pid == pid, do: delete_entries, else: [{cluster, key, old_pid} | delete_entries]

        {[{cluster, key, pid, meta, time, entry_node} | insert_entries], delete_entries}
    end)
    |> then(fn {insert_entries, delete_entries} ->
      {Enum.reverse(insert_entries), Enum.reverse(delete_entries)}
    end)
  end

  defp pg_batch_diff(entries) do
    Enum.reduce(entries, {[], []}, fn
      {{_cluster, _key, _pid}, {initial, current}}, {insert_entries, delete_entries}
      when current == initial ->
        {insert_entries, delete_entries}

      {{cluster, key, pid}, {_initial, nil}}, {insert_entries, delete_entries} ->
        {insert_entries, [{cluster, key, pid} | delete_entries]}

      {{cluster, key, pid}, {_initial, {meta, time, entry_node}}},
      {insert_entries, delete_entries} ->
        {[{cluster, key, pid, meta, time, entry_node} | insert_entries], delete_entries}
    end)
    |> then(fn {insert_entries, delete_entries} ->
      {Enum.reverse(insert_entries), Enum.reverse(delete_entries)}
    end)
  end

  defp ensure_local_batch_monitor(state, new_monitors, pid) do
    cond do
      Map.has_key?(state.monitors, pid) -> new_monitors
      Map.has_key?(new_monitors, pid) -> new_monitors
      true -> Map.put(new_monitors, pid, Process.monitor(pid))
    end
  end

  defp finalize_local_batch_monitors(state, new_monitors, maybe_demonitor_pids) do
    %{name: name, shard_index: shard} = state
    monitors = Map.merge(state.monitors, new_monitors)

    monitors =
      Enum.reduce(maybe_demonitor_pids, monitors, fn pid, acc ->
        case Data.maybe_demonitor(name, shard, pid) do
          :still_monitored ->
            acc

          :ok ->
            case Map.pop(acc, pid) do
              {nil, acc} ->
                acc

              {mref, acc} ->
                Process.demonitor(mref, [:flush])
                acc
            end
        end
      end)

    %{state | monitors: monitors}
  end

  defp send_local_batch_broadcasts(state, broadcasts) do
    Enum.reduce(Enum.reverse(broadcasts), state, fn op, acc_state ->
      enqueue_broadcast_op(acc_state, op)
    end)
  end

  defp enqueue_broadcast_op(state, {:register, _cluster, _key, _pid, _meta, _time, _node} = op),
    do: sequence_and_enqueue_broadcast(state, op, :registry)

  defp enqueue_broadcast_op(state, {:unregister, _cluster, _key, _pid, _meta, _reason} = op),
    do: sequence_and_enqueue_broadcast(state, op, :registry)

  defp enqueue_broadcast_op(
         state,
         {:join, _cluster, _key, _pid, _meta, _time, _reason, _node} = op
       ),
       do: sequence_and_enqueue_broadcast(state, op, :pg)

  defp enqueue_broadcast_op(state, {:leave, _cluster, _key, _pid, _meta, _reason} = op),
    do: sequence_and_enqueue_broadcast(state, op, :pg)

  defp sequence_and_enqueue_broadcast(state, op, domain) do
    record = append_local_replica_record(state, op)
    finish_local_replica_record(state, record, domain)
  end

  defp append_local_replica_record(state, op) do
    cluster = WireProtocol.op_cluster(op)

    case Data.local_stream_id(state.name, state.shard_index, cluster) do
      nil ->
        nil

      stream_id ->
        {seq, mutations} =
          Data.append_replica_record(state.name, state.shard_index, stream_id, [op])

        {:sequenced, stream_id, seq, mutations}
    end
  end

  defp finish_local_replica_record(state, nil, _domain), do: state

  defp finish_local_replica_record(
         state,
         {:sequenced, stream_id, seq, mutations} = sequenced,
         domain
       ) do
    apply_registry_claim_mutations(state, stream_id, seq, mutations)

    :ok = Data.mark_local_replica_applied(state.name, state.shard_index, stream_id, seq)

    :ok =
      Data.prune_replica_oplog(
        state.name,
        state.shard_index,
        state.replicated_oplog_max_entries
      )

    case domain do
      :registry -> enqueue_replicated_registry_broadcast(state, sequenced)
      :pg -> enqueue_replicated_pg_broadcast(state, sequenced)
    end
  end

  defp reply_local_requests(replies) do
    Enum.each(Enum.reverse(replies), fn {reply_to, reply} ->
      reply_local_request(reply_to, reply)
    end)
  end

  defp local_request_domain({:join, _cluster, _epoch, _key, _pid, _meta}), do: :pg
  defp local_request_domain({:leave, _cluster, _epoch, _key, _pid}), do: :pg
  defp local_request_domain(_request), do: :other

  defp do_register(state, cluster, key, pid, meta) do
    %{name: name, shard_index: shard} = state

    case Data.registry_lookup(name, shard, cluster, key) do
      nil ->
        time = System.system_time()
        op = {:register, cluster, key, pid, meta, time, node(pid)}
        record = append_local_replica_record(state, op)
        mref = monitor_pid(state, pid)
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} register key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        state = finish_local_replica_record(state, record, :registry)

        state = put_monitor(state, pid, mref)

        event =
          build_event(name, :registered, key, pid, meta, %{previous_meta: nil, cluster: cluster})

        notify_monitors(name, [event])
        {:ok, state}

      {^pid, old_meta, _time, _node} when old_meta == meta ->
        {:ok, state}

      {^pid, old_meta, _time, _node} ->
        time = System.system_time()
        op = {:register, cluster, key, pid, meta, time, node(pid)}
        record = append_local_replica_record(state, op)
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} re-register key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        state = finish_local_replica_record(state, record, :registry)

        event =
          build_event(name, :registered, key, pid, meta, %{
            previous_meta: old_meta,
            cluster: cluster
          })

        notify_monitors(name, [event])
        {:ok, state}

      _other ->
        {{:error, :taken}, state}
    end
  end

  defp do_unregister(state, cluster, key) do
    %{name: name, shard_index: shard} = state

    case Data.registry_lookup(name, shard, cluster, key) do
      {pid, meta, _time, entry_node} when entry_node == node() ->
        op = {:unregister, cluster, key, pid, meta, :unregister}
        record = append_local_replica_record(state, op)
        Data.registry_delete(name, shard, cluster, key, pid)
        state = maybe_demonitor_pid(state, name, shard, pid)

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} unregister key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        state = finish_local_replica_record(state, record, :registry)

        event =
          build_event(name, :unregistered, key, pid, meta, %{
            reason: :unregister,
            cluster: cluster
          })

        {state, projection_events} =
          reconcile_registry_keys(state, [{cluster, key}], :unregister, [])

        notify_monitors(name, projection_events ++ [event])
        {:ok, state}

      nil ->
        {{:error, :undefined}, state}

      {_pid, _meta, _time, _other_node} ->
        {{:error, :not_owner}, state}
    end
  end

  defp do_join(state, cluster, key, pid, meta) do
    %{name: name, shard_index: shard} = state

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        time = System.system_time()
        op = {:join, cluster, key, pid, meta, time, :join, node(pid)}
        record = append_local_replica_record(state, op)
        mref = monitor_pid(state, pid)
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} join key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        state = finish_local_replica_record(state, record, :pg)

        state = put_monitor(state, pid, mref)

        event =
          build_event(name, :joined, key, pid, meta, %{previous_meta: nil, cluster: cluster})

        notify_monitors(name, [event])
        {:ok, state}

      {old_meta, _time, _node} when old_meta == meta ->
        {:ok, state}

      {old_meta, _time, _node} ->
        time = System.system_time()
        op = {:join, cluster, key, pid, meta, time, :update, node(pid)}
        record = append_local_replica_record(state, op)
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} re-join key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        state = finish_local_replica_record(state, record, :pg)

        event =
          build_event(name, :joined, key, pid, meta, %{previous_meta: old_meta, cluster: cluster})

        notify_monitors(name, [event])
        {:ok, state}
    end
  end

  defp do_leave(state, cluster, key, pid) do
    %{name: name, shard_index: shard} = state

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        {{:error, :not_in_group}, state}

      {meta, _time, _node} ->
        op = {:leave, cluster, key, pid, meta, :leave}
        record = append_local_replica_record(state, op)
        Data.pg_delete(name, shard, cluster, key, pid)
        state = maybe_demonitor_pid(state, name, shard, pid)

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} leave key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        state = finish_local_replica_record(state, record, :pg)

        event = build_event(name, :left, key, pid, meta, %{reason: :leave, cluster: cluster})
        notify_monitors(name, [event])
        {:ok, state}
    end
  end

  defp do_cluster_connect(state, clusters),
    do:
      do_cluster_connect(
        state,
        clusters,
        Enum.map(clusters, &{&1, Data.local_cluster_epoch(state.name, &1)})
      )

  defp do_cluster_connect(state, clusters, epochs) do
    state = flush_pending_replicated_sender_barrier(state)
    %{name: name} = state

    log(state, fn ->
      "#{log_prefix(state)} cluster_connect #{inspect(clusters)}"
    end)

    peers = Data.cluster_nodes(name, nil) -- [node()]

    for target_node <- peers do
      send_remote_shard_message(
        state,
        target_node,
        {:replica_cluster_open, self(), Data.generation(name),
         Data.local_cluster_epoch_revision(name), epochs}
      )
    end

    {:ok, state}
  end

  defp do_cluster_disconnect(state, clusters),
    do:
      do_cluster_disconnect(
        state,
        clusters,
        Enum.map(clusters, &{&1, Data.closed_local_cluster_epoch(state.name, &1)})
      )

  defp do_cluster_disconnect(state, _clusters, epochs) do
    epochs =
      Enum.filter(epochs, fn
        {cluster, epoch} when not is_nil(epoch) ->
          Data.closed_local_cluster_pending?(
            state.name,
            cluster,
            epoch,
            state.shard_index
          )

        _ ->
          false
      end)

    case epochs do
      [] -> {:ok, state}
      epochs -> do_cluster_disconnect_epochs(state, epochs)
    end
  end

  defp do_cluster_disconnect_epochs(state, epochs) do
    state = flush_pending_replicated_sender_barrier(state)
    %{name: name, shard_index: shard} = state
    clusters = Enum.map(epochs, &elem(&1, 0))

    closed_streams =
      Enum.flat_map(epochs, fn
        {cluster, epoch} when not is_nil(epoch) ->
          [
            WireProtocol.stream_id(
              name,
              node(),
              Data.generation(name),
              shard,
              cluster,
              epoch
            )
          ]

        _ ->
          []
      end)

    state = discard_snapshot_send_offsets_for_streams(state, closed_streams)

    # See the nodedown ordering note: restart repair rejects remote rows whose
    # receive cursor was cleared before this cluster-wide purge.
    :ok = Data.delete_replica_cursors_for_clusters(name, shard, clusters)

    log_once(state, fn ->
      "#{log_prefix(state)} cluster_disconnect #{inspect(clusters)}"
    end)

    {events, local_pids} =
      Enum.reduce(clusters, {[], MapSet.new()}, fn cluster, {events, local_pids} ->
        _affected_keys = Data.purge_registry_claims_for_cluster(name, shard, cluster)
        {purged_reg, purged_pg} = purge_cluster_entries(name, shard, cluster, :all)

        local_pids =
          Enum.reduce(purged_reg ++ purged_pg, local_pids, fn
            {_cluster, _key, pid, _meta, _time}, acc when node(pid) == node() ->
              MapSet.put(acc, pid)

            _entry, acc ->
              acc
          end)

        {build_purged_events(name, purged_reg, purged_pg, :cluster_disconnect, events),
         local_pids}
      end)

    state =
      Enum.reduce(local_pids, state, fn pid, acc ->
        maybe_demonitor_pid(acc, name, shard, pid)
      end)

    # Disconnect purges every origin's materialized rows for these clusters.
    # Forget their receive cursors as well: if this node later reconnects while
    # a remote origin kept the same epoch, its advertised head must rebuild the
    # rows instead of being mistaken for data we still retain.
    if shard == 0 do
      broadcast_to_peers(
        state,
        {:replica_cluster_close, self(), Data.generation(name),
         Data.local_cluster_epoch_revision(name), epochs}
      )
    end

    Enum.each(epochs, fn
      {cluster, epoch} when not is_nil(epoch) ->
        Data.drop_local_stream(name, shard, cluster, epoch)

      _ ->
        :ok
    end)

    _completed_clusters = Data.mark_closed_cluster_shard(name, epochs, shard)
    notify_monitors(name, events)
    {:ok, state}
  end

  defp enqueue_replicated_pg_ops(state, ops) do
    state = flush_pending_replicated_registry_barrier(state)
    Enum.each(ops, &log_replicated_pg_op(state, &1))

    now = System.monotonic_time(:millisecond)
    ops_len = length(ops)

    state =
      case state.pending_replicated_pg_len do
        0 ->
          %{state | pending_replicated_pg_started_at: now}
          |> schedule_replicated_pg_flush()
          |> Map.put(:pending_replicated_pg_ops, Enum.reverse(ops))
          |> Map.put(:pending_replicated_pg_len, ops_len)

        len ->
          %{
            state
            | pending_replicated_pg_ops: Enum.reverse(ops, state.pending_replicated_pg_ops),
              pending_replicated_pg_len: len + ops_len
          }
      end

    if state.pending_replicated_pg_len >= state.replicated_pg_receiver_buffer_size or
         pending_replicated_pg_due?(state, now) do
      {flush_pending_replicated_pg(state), true}
    else
      {state, false}
    end
  end

  defp enqueue_replicated_registry_ops(state, ops) do
    state = flush_pending_replicated_pg_barrier(state)
    Enum.each(ops, &log_replicated_registry_op(state, &1))

    now = System.monotonic_time(:millisecond)
    ops_len = length(ops)

    state =
      case state.pending_replicated_registry_len do
        0 ->
          %{state | pending_replicated_registry_started_at: now}
          |> schedule_replicated_registry_flush()
          |> Map.put(:pending_replicated_registry_ops, Enum.reverse(ops))
          |> Map.put(:pending_replicated_registry_len, ops_len)

        len ->
          %{
            state
            | pending_replicated_registry_ops:
                Enum.reverse(ops, state.pending_replicated_registry_ops),
              pending_replicated_registry_len: len + ops_len
          }
      end

    if state.pending_replicated_registry_len >= state.replicated_registry_receiver_buffer_size or
         pending_replicated_registry_due?(state, now) do
      {flush_pending_replicated_registry(state), true}
    else
      {state, false}
    end
  end

  defp enqueue_replicated_pg_broadcast(state, op),
    do: enqueue_replica_broadcasts(state, [op])

  defp enqueue_replicated_registry_broadcast(state, op),
    do: enqueue_replica_broadcasts(state, [op])

  defp enqueue_replica_broadcasts(state, ops) do
    now = System.monotonic_time(:millisecond)
    ops_len = length(ops)

    state =
      case state.pending_replica_broadcast_len do
        0 ->
          %{state | pending_replica_broadcast_started_at: now}
          |> schedule_replica_broadcast_flush()
          |> Map.put(:pending_replica_broadcast_ops, Enum.reverse(ops))
          |> Map.put(:pending_replica_broadcast_len, ops_len)

        len ->
          %{
            state
            | pending_replica_broadcast_ops:
                Enum.reverse(ops, state.pending_replica_broadcast_ops),
              pending_replica_broadcast_len: len + ops_len
          }
      end

    if state.pending_replica_broadcast_len >= state.replicated_sender_buffer_size or
         pending_replica_broadcast_due?(state, now) do
      flush_pending_replica_broadcast(state)
    else
      state
    end
  end

  defp process_inline_priority_message(state, msg) do
    case handle_info(msg, state) do
      {:noreply, next_state} ->
        next_state
    end
  end

  defp take_priority_turn(state) do
    state
    |> take_priority_control_turn()
    |> take_one_local_request_turn()
  end

  defp take_priority_control_turn(state),
    do: take_priority_control_turn(state, @priority_control_quota)

  defp take_priority_control_turn(state, 0), do: state

  defp take_priority_control_turn(state, remaining) do
    receive do
      {:peer_connect, _remote_pid, _remote_shard_index, _remote_num_shards, _remote_clusters} =
          msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:peer_connect_ack, _remote_pid, _remote_shard_index, _remote_num_shards, _remote_clusters} =
          msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_hello, _remote_pid, _version, _generation, _epoch_revision, _cluster_epochs,
       _transport_id, _descriptor} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_lane_hello, _remote_pid, _version, _generation, _epoch_revision, _transport_id,
       _descriptor} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_authority_installed_local, _remote_node, _generation, _epoch_revision,
       _old_generation, _stale_epochs} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_authority_dirty_local, _remote_node} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_cluster_open_control_local, _remote_node, _generation, _revision, _epochs, _stale,
       _shared} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_cluster_close_control_local, _remote_node, _generation, _revision, _closed} =
          msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_heartbeat, _remote_pid, _version, _generation, _epoch_revision, _transport_id,
       _transport_descriptor} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_hello_request, _remote_pid} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_cluster_open, _remote_pid, _generation, _revision, _epochs} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:replica_cluster_close, _remote_pid, _generation, _revision, _epochs} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:DOWN, _mref, :process, _pid, _reason} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:nodeup, _remote_node} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)

      {:nodedown, _remote_node} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining - 1)
    after
      0 ->
        state
    end
  end

  defp take_one_local_request_turn(state) do
    receive do
      {@local_request_tag, caller_pid, ref, request}
      when is_pid(caller_pid) and is_reference(ref) ->
        process_local_request_turn(state, [{{:send, caller_pid, ref}, request}])

      {@local_request_tag, alias_ref, request} when is_reference(alias_ref) ->
        process_local_request_turn(state, [{{:alias, alias_ref}, request}])

      {@local_request_tag, :noreply, request} ->
        process_local_request_turn(state, [{:noreply, request}])
    after
      0 ->
        state
    end
  end

  defp schedule_replicated_pg_flush(%{replicated_pg_receiver_flush_interval: 0} = state) do
    %{state | pending_replicated_pg_flush_ref: nil}
  end

  defp schedule_replicated_pg_flush(state) do
    flush_ref = make_ref()

    Process.send_after(
      self(),
      {@replicated_pg_receiver_flush_timer, flush_ref},
      state.replicated_pg_receiver_flush_interval
    )

    %{state | pending_replicated_pg_flush_ref: flush_ref}
  end

  defp schedule_replicated_registry_flush(
         %{replicated_registry_receiver_flush_interval: 0} = state
       ) do
    %{state | pending_replicated_registry_flush_ref: nil}
  end

  defp schedule_replicated_registry_flush(state) do
    flush_ref = make_ref()

    Process.send_after(
      self(),
      {@replicated_registry_receiver_flush_timer, flush_ref},
      state.replicated_registry_receiver_flush_interval
    )

    %{state | pending_replicated_registry_flush_ref: flush_ref}
  end

  defp schedule_replica_broadcast_flush(%{replicated_sender_flush_interval: 0} = state) do
    %{state | pending_replica_broadcast_flush_ref: nil}
  end

  defp schedule_replica_broadcast_flush(state) do
    flush_ref = make_ref()

    Process.send_after(
      self(),
      {@replica_broadcast_flush_timer, flush_ref},
      state.replicated_sender_flush_interval
    )

    %{state | pending_replica_broadcast_flush_ref: flush_ref}
  end

  defp pending_replicated_pg_due?(%{pending_replicated_pg_len: 0}, _now), do: false

  defp pending_replicated_pg_due?(state, now) do
    state.replicated_pg_receiver_flush_interval == 0 or
      now - state.pending_replicated_pg_started_at >= state.replicated_pg_receiver_flush_interval
  end

  defp pending_replicated_registry_due?(%{pending_replicated_registry_len: 0}, _now), do: false

  defp pending_replicated_registry_due?(state, now) do
    state.replicated_registry_receiver_flush_interval == 0 or
      now - state.pending_replicated_registry_started_at >=
        state.replicated_registry_receiver_flush_interval
  end

  defp pending_replica_broadcast_due?(%{pending_replica_broadcast_len: 0}, _now),
    do: false

  defp pending_replica_broadcast_due?(state, now) do
    state.replicated_sender_flush_interval == 0 or
      now - state.pending_replica_broadcast_started_at >=
        state.replicated_sender_flush_interval
  end

  defp flush_pending_replicated_pg(%{pending_replicated_pg_len: 0} = state), do: state

  defp flush_pending_replicated_pg(state) do
    %{name: name, shard_index: shard} = state
    ops = Enum.reverse(state.pending_replicated_pg_ops)

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} flush_replicated_pg_receiver_buffer ops=#{length(ops)}"
    end)

    {insert_entries, delete_entries, events} = apply_replicated_pg_ops(name, shard, ops)

    Data.pg_delete_many(name, shard, delete_entries)
    Data.pg_insert_many(name, shard, insert_entries)
    notify_monitors(name, events)

    %{
      state
      | pending_replicated_pg_ops: [],
        pending_replicated_pg_len: 0,
        pending_replicated_pg_started_at: nil,
        pending_replicated_pg_flush_ref: nil
    }
  end

  defp flush_pending_replicated_registry(%{pending_replicated_registry_len: 0} = state), do: state

  defp flush_pending_replicated_registry(state) do
    %{name: name, shard_index: shard} = state
    ops = Enum.reverse(state.pending_replicated_registry_ops)

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} flush_replicated_registry_receiver_buffer ops=#{length(ops)}"
    end)

    {entries, events, broadcasts, maybe_demonitor_pids} =
      apply_replicated_registry_ops(state, ops)

    {insert_entries, delete_entries} = registry_batch_diff(entries)
    Data.registry_delete_many(name, shard, delete_entries)
    Data.registry_insert_many(name, shard, insert_entries)
    state = finalize_local_batch_monitors(state, %{}, maybe_demonitor_pids)
    state = send_local_batch_broadcasts(state, broadcasts)
    notify_monitors(name, events)

    %{
      state
      | pending_replicated_registry_ops: [],
        pending_replicated_registry_len: 0,
        pending_replicated_registry_started_at: nil,
        pending_replicated_registry_flush_ref: nil
    }
  end

  defp flush_pending_replica_broadcast(%{pending_replica_broadcast_len: 0} = state),
    do: state

  defp flush_pending_replica_broadcast(state) do
    ops = Enum.reverse(state.pending_replica_broadcast_ops)

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} flush_replica_broadcast_buffer ops=#{length(ops)}"
    end)

    send_replicated_batches(state, ops)

    %{
      state
      | pending_replica_broadcast_ops: [],
        pending_replica_broadcast_len: 0,
        pending_replica_broadcast_started_at: nil,
        pending_replica_broadcast_flush_ref: nil
    }
  end

  defp apply_replicated_registry_ops(state, ops) do
    %{name: name, shard_index: shard} = state
    local_node = node()

    ops =
      Enum.filter(ops, fn op ->
        replicated_op_for_active_cluster?(name, op, &registry_op_cluster/1)
      end)

    Enum.reduce(ops, {%{}, [], [], MapSet.new()}, fn
      {:register, cluster, key, pid, meta, time, entry_node},
      {entries, events, broadcasts, maybe_demonitor_pids} ->
        entry = {cluster, key}
        {initial, current} = registry_batch_entry(entries, name, shard, entry)

        case current do
          nil ->
            event =
              build_event(name, :registered, key, pid, meta, %{
                previous_meta: nil,
                cluster: cluster
              })

            {
              Map.put(entries, entry, {initial, {pid, meta, time, entry_node}}),
              [event | events],
              broadcasts,
              maybe_demonitor_pids
            }

          {^pid, old_meta, _old_time, _old_node} ->
            event =
              build_event(name, :registered, key, pid, meta, %{
                previous_meta: old_meta,
                cluster: cluster
              })

            {
              Map.put(entries, entry, {initial, {pid, meta, time, entry_node}}),
              [event | events],
              broadcasts,
              maybe_demonitor_pids
            }

          {existing_pid, existing_meta, existing_time, ^local_node} ->
            resolve_replicated_registry_conflict(
              state,
              cluster,
              key,
              {existing_pid, existing_meta, existing_time},
              {pid, meta, time},
              {initial, current},
              entries,
              events,
              broadcasts,
              maybe_demonitor_pids
            )

          {_existing_pid, _existing_meta, existing_time, _existing_node} ->
            if time > existing_time do
              event =
                build_event(name, :registered, key, pid, meta, %{
                  previous_meta: nil,
                  cluster: cluster
                })

              {
                Map.put(entries, entry, {initial, {pid, meta, time, entry_node}}),
                [event | events],
                broadcasts,
                maybe_demonitor_pids
              }
            else
              {entries, events, broadcasts, maybe_demonitor_pids}
            end
        end

      {:unregister, cluster, key, pid, meta, reason},
      {entries, events, broadcasts, maybe_demonitor_pids} ->
        entry = {cluster, key}
        {initial, current} = registry_batch_entry(entries, name, shard, entry)

        case current do
          {^pid, _current_meta, _current_time, _current_node} ->
            event =
              build_event(name, :unregistered, key, pid, meta, %{
                reason: reason,
                cluster: cluster
              })

            {
              Map.put(entries, entry, {initial, nil}),
              [event | events],
              broadcasts,
              maybe_demonitor_pids
            }

          _ ->
            {entries, events, broadcasts, maybe_demonitor_pids}
        end
    end)
  end

  defp apply_replicated_pg_ops(name, shard, ops) do
    ops =
      Enum.filter(ops, fn op ->
        replicated_op_for_active_cluster?(name, op, &pg_op_cluster/1)
      end)

    {entries, events} =
      Enum.reduce(ops, {%{}, []}, fn
        {:join, cluster, key, pid, meta, time, reason, entry_node}, {entries, events} ->
          member = {cluster, key, pid}
          {initial, current} = replicated_pg_entry(entries, name, shard, member)

          case current do
            {^meta, ^time, ^entry_node} ->
              {entries, events}

            _ ->
              previous_meta =
                case {current, reason} do
                  {{old_meta, _old_time, _old_node}, :update} -> old_meta
                  _ -> nil
                end

              event =
                build_event(name, :joined, key, pid, meta, %{
                  previous_meta: previous_meta,
                  cluster: cluster
                })

              updated_entries =
                Map.put(entries, member, {initial, {meta, time, entry_node}})

              {updated_entries, [event | events]}
          end

        {:leave, cluster, key, pid, meta, reason}, {entries, events} ->
          member = {cluster, key, pid}
          {initial, current} = replicated_pg_entry(entries, name, shard, member)

          case current do
            nil ->
              {entries, events}

            {_current_meta, _current_time, _current_node} ->
              event =
                build_event(name, :left, key, pid, meta, %{reason: reason, cluster: cluster})

              updated_entries = Map.put(entries, member, {initial, nil})
              {updated_entries, [event | events]}
          end
      end)

    {insert_entries, delete_entries} =
      Enum.reduce(entries, {[], []}, fn
        {{_cluster, _key, _pid}, {initial, current}}, {insert_entries, delete_entries}
        when current == initial ->
          {insert_entries, delete_entries}

        {{cluster, key, pid}, {_initial, nil}}, {insert_entries, delete_entries} ->
          {insert_entries, [{cluster, key, pid} | delete_entries]}

        {{cluster, key, pid}, {_initial, {meta, time, entry_node}}},
        {insert_entries, delete_entries} ->
          {[{cluster, key, pid, meta, time, entry_node} | insert_entries], delete_entries}
      end)

    {Enum.reverse(insert_entries), Enum.reverse(delete_entries), events}
  end

  defp replicated_pg_entry(entries, name, shard, member) do
    case Map.fetch(entries, member) do
      {:ok, {initial, current}} ->
        {initial, current}

      :error ->
        {cluster, key, pid} = member

        current =
          case Data.pg_lookup(name, shard, cluster, key, pid) do
            nil -> nil
            {meta, time, entry_node} -> {meta, time, entry_node}
          end

        {current, current}
    end
  end

  defp log_replicated_pg_op(state, {:join, cluster, key, pid, _meta, _time, _reason, _entry_node}) do
    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_join key=#{inspect(key)} pid=#{inspect(pid)} from #{node(pid)} cluster=#{inspect(cluster)}"
    end)
  end

  defp log_replicated_pg_op(state, {:leave, cluster, key, pid, _meta, _reason}) do
    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_leave key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
    end)
  end

  defp log_replicated_registry_op(
         state,
         {:register, cluster, key, pid, _meta, _time, _entry_node}
       ) do
    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_register key=#{inspect(key)} pid=#{inspect(pid)} from #{node(pid)} cluster=#{inspect(cluster)}"
    end)
  end

  defp log_replicated_registry_op(state, {:unregister, cluster, key, pid, _meta, _reason}) do
    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_unregister key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
    end)
  end

  defp send_replicated_batches(state, ops) do
    ops
    |> group_broadcast_ops_by_target(state, &sequenced_op_cluster/1)
    |> Enum.each(fn {target_node, target_ops} ->
      send_replica_delta_batch(state, target_node, target_ops)
    end)
  end

  defp send_replica_delta_batch(state, target_node, sequenced_ops) do
    runs =
      sequenced_ops
      |> Enum.group_by(fn {:sequenced, stream_id, _seq, _mutations} -> stream_id end)
      |> Enum.map(fn {stream_id, records} ->
        records =
          records
          |> Enum.map(fn {:sequenced, ^stream_id, seq, mutations} -> {seq, mutations} end)
          |> Enum.sort_by(&elem(&1, 0))

        {first_seq, _mutations} = hd(records)

        {_floor, head, _applied} =
          Data.replica_stream_head(state.name, state.shard_index, stream_id)

        {stream_id, first_seq, records, head}
      end)

    outgoing_replica_message(state, target_node, {:delta_batch, WireProtocol.version(), runs})
  end

  defp group_broadcast_ops_by_target(ops, state, cluster_fun) do
    {messages_by_node, _targets_cache} =
      Enum.reduce(ops, {%{}, %{}}, fn op, {messages_by_node, targets_cache} ->
        cluster = cluster_fun.(op)
        {target_nodes, targets_cache} = broadcast_targets(state, cluster, targets_cache)

        messages_by_node =
          Enum.reduce(target_nodes, messages_by_node, fn target_node, acc ->
            Map.update(acc, target_node, [op], &[op | &1])
          end)

        {messages_by_node, targets_cache}
      end)

    Enum.map(messages_by_node, fn {target_node, target_ops} ->
      {target_node, Enum.reverse(target_ops)}
    end)
  end

  defp broadcast_targets(state, cluster, cache) do
    case cache do
      %{^cluster => target_nodes} ->
        {target_nodes, cache}

      %{} ->
        target_nodes =
          case cluster do
            nil ->
              for {target_node, _last_seen} <- state.peer_last_seen, do: target_node

            _cluster ->
              for target_node <- Data.cluster_nodes(state.name, cluster),
                  target_node != node(),
                  do: target_node
          end

        {target_nodes, Map.put(cache, cluster, target_nodes)}
    end
  end

  defp pg_op_cluster({:join, cluster, _key, _pid, _meta, _time, _reason, _entry_node}),
    do: cluster

  defp pg_op_cluster({:leave, cluster, _key, _pid, _meta, _reason}), do: cluster

  defp pg_op_cluster({:sequenced, _stream_id, _seq, [op | _]}), do: pg_op_cluster(op)

  defp registry_op_cluster({:register, cluster, _key, _pid, _meta, _time, _entry_node}),
    do: cluster

  defp registry_op_cluster({:unregister, cluster, _key, _pid, _meta, _reason}), do: cluster

  defp registry_op_cluster({:sequenced, _stream_id, _seq, [op | _]}),
    do: registry_op_cluster(op)

  defp sequenced_op_cluster({:sequenced, stream_id, _seq, _mutations}),
    do: WireProtocol.stream_cluster(stream_id)

  defp replicated_op_for_active_cluster?(name, op, cluster_fun)
       when is_function(cluster_fun, 1) do
    case cluster_fun.(op) do
      nil -> true
      cluster -> cluster_member?(name, cluster)
    end
  end

  defp send_to_peer(state, target_node, message) do
    send_remote_shard_message(state, target_node, message)
  end

  defp broadcast_to_peers(state, message) do
    for {target_node, _pid} <- state.remote_shards do
      send_remote_control_message(state, target_node, message)
    end
  end

  defp send_remote_shard_message(state, target_node, message) do
    shard_name = shard_name(state.name, state.shard_index)

    case :erlang.send_nosuspend({shard_name, target_node}, message, [:noconnect]) do
      true ->
        :ok

      false ->
        :busy
    end
  end

  defp send_remote_control_message(state, target_node, message) do
    control_name = shard_name(state.name, 0)

    case :erlang.send_nosuspend({control_name, target_node}, message, [:noconnect]) do
      true -> :ok
      false -> :busy
    end
  end

  defp outgoing_replica_message(state, target_node, message) do
    case state.replica_transport.outgoing(
           state.name,
           target_node,
           state.shard_index,
           message,
           state.replica_transport_opts
         ) do
      :ok -> :ok
      :busy -> :ok
      :disconnected -> :ok
    end

    state
  end

  defp install_replica_authority(
         state,
         remote_pid,
         generation,
         epoch_revision,
         cluster_epochs,
         _transport_id,
         transport_descriptor
       ) do
    remote_node = node(remote_pid)

    case Data.put_remote_replica_info(
           state.name,
           0,
           remote_node,
           generation,
           epoch_revision,
           cluster_epochs
         ) do
      :stale ->
        state
        |> mark_cluster_control_dirty(remote_node)
        |> request_replica_authority(remote_node)

      {old_generation, stale_epochs} ->
        finish_replica_authority_install(
          state,
          remote_pid,
          remote_node,
          generation,
          epoch_revision,
          transport_descriptor,
          old_generation,
          stale_epochs
        )
    end
  end

  defp finish_replica_authority_install(
         state,
         remote_pid,
         remote_node,
         generation,
         epoch_revision,
         transport_descriptor,
         old_generation,
         stale_epochs
       ) do
    state = maybe_purge_remote_generation(state, remote_node, old_generation, generation)
    state = notify_replica_transport_peer_up(state, remote_node, transport_descriptor)

    state =
      if old_generation == generation do
        state
        |> purge_closed_remote_epochs(remote_node, stale_epochs)
        |> purge_remote_streams_outside_authority(remote_node)
      else
        state
      end

    state = install_replica_view(state, remote_node, generation)

    if replica_authority_current?(state, remote_node, generation, epoch_revision) and
         replica_view_current?(state, remote_node) do
      state = %{
        state
        | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid),
          peer_last_seen: Map.put(state.peer_last_seen, remote_node, monotonic_millis()),
          cluster_control_dirty: Map.delete(state.cluster_control_dirty, remote_node)
      }

      fan_out_to_siblings(
        state,
        {:replica_authority_installed_local, remote_node, generation, epoch_revision,
         old_generation, stale_epochs}
      )

      send_replica_heads(state, remote_node)
    else
      state
      |> mark_cluster_control_dirty(remote_node)
      |> request_replica_authority(remote_node)
    end
  end

  defp notify_replica_transport_peer_up(state, remote_node, transport_descriptor) do
    if function_exported?(state.replica_transport, :peer_up, 5) do
      :ok =
        state.replica_transport.peer_up(
          state.name,
          remote_node,
          state.shard_index,
          transport_descriptor,
          state.replica_transport_opts
        )
    end

    state
  end

  defp send_replica_hello(state, target_node) do
    descriptor = state.replica_transport.descriptor(state.name, state.replica_transport_opts)

    if state.shard_index == 0 do
      {generation, epoch_revision, cluster_epochs} =
        Data.local_replica_authority(state.name)

      send_remote_control_message(
        state,
        target_node,
        {:replica_hello, self(), WireProtocol.version(), generation, epoch_revision,
         cluster_epochs, state.replica_transport.id(), descriptor}
      )
    else
      send_remote_shard_message(
        state,
        target_node,
        {:replica_lane_hello, self(), WireProtocol.version(), Data.generation(state.name),
         Data.local_cluster_epoch_revision(state.name), state.replica_transport.id(), descriptor}
      )
    end

    state
  end

  defp request_replica_authority(state, remote_node) do
    send_remote_control_message(state, remote_node, {:replica_hello_request, self()})
    state
  end

  defp replica_authority_current?(state, remote_node, generation, epoch_revision) do
    Data.remote_generation(state.name, remote_node) == generation and
      Data.remote_cluster_epoch_observed_revision(state.name, remote_node) == epoch_revision and
      Data.remote_replica_authority_hint(state.name, remote_node) ==
        {generation, epoch_revision}
  end

  defp replica_exact_authority_current?(state, remote_node, generation, epoch_revision) do
    replica_authority_current?(state, remote_node, generation, epoch_revision) and
      Data.remote_cluster_epoch_exact_revision(state.name, remote_node) == epoch_revision
  end

  defp replica_view_current?(state, remote_node) do
    known_generation = Data.remote_generation(state.name, remote_node)
    observed_revision = Data.remote_cluster_epoch_observed_revision(state.name, remote_node)

    Data.remote_replica_authority_hint(state.name, remote_node) ==
      {known_generation, observed_revision} and
      Data.remote_cluster_epoch_revision(state.name, remote_node) == observed_revision and
      Data.remote_view_generation(state.name, state.shard_index, remote_node) ==
        known_generation and
      Data.remote_view_cluster_epoch_revision(state.name, state.shard_index, remote_node) ==
        Data.remote_cluster_epoch_exact_revision(state.name, remote_node) and
      Data.remote_view_observed_revision(state.name, state.shard_index, remote_node) ==
        observed_revision
  end

  defp observe_replica_authority_hint(state, remote_node, generation, epoch_revision) do
    if WireProtocol.valid_generation?(generation) and is_integer(epoch_revision) and
         epoch_revision >= 0 and
         Data.observe_remote_replica_hint(
           state.name,
           remote_node,
           generation,
           epoch_revision
         ) do
      # A heartbeat or lane hello can outrun a dropped authority control. Data
      # has already fenced every affected lane; retain an exact-hello retry
      # obligation independently of this one best-effort request.
      state
      |> ensure_replica_peer_retirement_deadline(remote_node)
      |> mark_authority_dirty(remote_node)
      |> request_replica_authority(remote_node)
    else
      state
    end
  end

  defp install_replica_view(state, remote_node, generation) do
    case Data.put_remote_view_info(
           state.name,
           state.shard_index,
           remote_node,
           generation,
           Data.remote_cluster_epoch_exact_revision(state.name, remote_node),
           Data.remote_cluster_epoch_observed_revision(state.name, remote_node)
         ) do
      :ok ->
        reproject_pending_registry_keys(state, remote_node)

      :stale ->
        state
        |> mark_authority_dirty(remote_node)
        |> request_replica_authority(remote_node)
    end
  end

  defp install_current_replica_lane(state, remote_node, generation) do
    old_generation =
      Data.remote_view_generation(state.name, state.shard_index, remote_node)

    state = maybe_purge_remote_generation(state, remote_node, old_generation, generation)
    state = purge_remote_streams_outside_authority(state, remote_node)
    state = install_replica_view(state, remote_node, generation)

    if replica_view_current?(state, remote_node) do
      state
      |> touch_replica_peer(remote_node)
      |> Map.update!(:cluster_control_dirty, &Map.delete(&1, remote_node))
      |> send_replica_heads(remote_node)
    else
      state
    end
  end

  defp schedule_anti_entropy(state) do
    ref = make_ref()

    Process.send_after(
      self(),
      {@anti_entropy_timer, ref},
      state.replicated_anti_entropy_interval
    )

    %{state | anti_entropy_ref: ref}
  end

  defp monotonic_millis, do: System.monotonic_time(:millisecond)

  defp put_remote_shard(state, remote_node, remote_pid) do
    %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}
  end

  defp touch_replica_peer(state, remote_node) do
    %{state | peer_last_seen: Map.put(state.peer_last_seen, remote_node, monotonic_millis())}
  end

  defp ensure_replica_peer_retirement_deadline(state, remote_node) do
    %{
      state
      | peer_last_seen: Map.put_new(state.peer_last_seen, remote_node, monotonic_millis())
    }
  end

  defp collect_replica_cluster_controls(_tag, _remote_pid, _generation, acc, 0),
    do: Enum.reverse(acc)

  defp collect_replica_cluster_controls(tag, remote_pid, generation, acc, remaining) do
    receive do
      {^tag, ^remote_pid, ^generation, revision, epochs} ->
        collect_replica_cluster_controls(
          tag,
          remote_pid,
          generation,
          [{revision, epochs} | acc],
          remaining - 1
        )
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp accepted_replica_cluster_epochs(state, remote_node, generation, controls) do
    case {
      Data.remote_replica_authority_hint(state.name, remote_node),
      Data.remote_view_generation(state.name, state.shard_index, remote_node)
    } do
      {{^generation, _hinted_revision}, ^generation} ->
        expected_revision =
          Data.remote_cluster_epoch_observed_revision(state.name, remote_node)

        accepted =
          Enum.filter(controls, fn {revision, _epochs} ->
            is_nil(expected_revision) or revision > expected_revision
          end)
          |> Enum.sort_by(&elem(&1, 0))
          |> Enum.uniq_by(&elem(&1, 0))

        case accepted do
          [] ->
            :stale

          accepted ->
            next_revision = (expected_revision || -1) + 1

            if contiguous_cluster_controls?(accepted, next_revision) do
              observed_revision = accepted |> List.last() |> elem(0)

              epochs =
                accepted
                |> Enum.flat_map(&elem(&1, 1))
                |> Map.new()
                |> Map.to_list()

              {:accept, expected_revision, observed_revision, epochs}
            else
              {:gap, accepted |> List.last() |> elem(0)}
            end
        end

      _other_authority ->
        :refresh
    end
  end

  defp contiguous_cluster_controls?(controls, expected_revision) do
    Enum.reduce_while(controls, expected_revision, fn
      {^expected_revision, _epochs}, ^expected_revision -> {:cont, expected_revision + 1}
      _control, _expected -> {:halt, false}
    end) != false
  end

  defp mark_cluster_control_dirty(state, remote_node) do
    %{
      state
      | cluster_control_dirty:
          Map.put(state.cluster_control_dirty, remote_node, monotonic_millis())
    }
  end

  defp mark_authority_dirty(%{shard_index: 0} = state, remote_node) do
    mark_cluster_control_dirty(state, remote_node)
  end

  defp mark_authority_dirty(state, remote_node) do
    if MapSet.member?(state.authority_dirty_notified, remote_node) do
      state
    else
      case send_local_control_message(state, {:replica_authority_dirty_local, remote_node}) do
        :ok ->
          %{
            state
            | authority_dirty_notified: MapSet.put(state.authority_dirty_notified, remote_node)
          }

        :disconnected ->
          state
      end
    end
  end

  defp request_quiet_cluster_hellos(state) do
    now = monotonic_millis()

    dirty =
      Enum.reduce(state.cluster_control_dirty, %{}, fn {remote_node, last_activity}, acc ->
        cond do
          is_nil(Data.remote_generation(state.name, remote_node)) and
              is_nil(Data.remote_replica_authority_hint(state.name, remote_node)) ->
            acc

          now - last_activity >= state.replicated_anti_entropy_interval ->
            request_replica_authority(state, remote_node)
            Map.put(acc, remote_node, now)

          true ->
            Map.put(acc, remote_node, last_activity)
        end
      end)

    %{state | cluster_control_dirty: dirty}
  end

  defp broadcast_replica_heartbeats(state) do
    transport_id = state.replica_transport.id()
    descriptor = state.replica_transport.descriptor(state.name, state.replica_transport_opts)

    Enum.reduce(state.remote_shards, state, fn {target_node, _pid}, acc ->
      send_remote_shard_message(
        acc,
        target_node,
        {:replica_heartbeat, self(), WireProtocol.version(), Data.generation(acc.name),
         Data.local_cluster_epoch_revision(acc.name), transport_id, descriptor}
      )

      acc
    end)
  end

  defp probe_replica_peers(state) do
    Enum.each(Node.list(), fn remote_node ->
      unless Map.has_key?(state.remote_shards, remote_node) do
        send_remote_shard_message(
          state,
          remote_node,
          {:peer_connect, self(), state.shard_index, state.num_shards,
           Data.my_clusters(state.name)}
        )
      end
    end)

    state
  end

  defp broadcast_replica_heads(state) do
    peers = Map.keys(state.peer_last_seen)

    heads_by_target =
      state.name
      |> Data.replica_stream_heads(state.shard_index)
      |> Enum.reduce(%{}, fn {stream_id, _floor, _head} = head, acc ->
        if current_local_replica_stream?(state, stream_id) do
          targets =
            case WireProtocol.stream_cluster(stream_id) do
              nil ->
                peers

              cluster ->
                state.name
                |> Data.cluster_nodes(cluster)
                |> Enum.filter(&Map.has_key?(state.peer_last_seen, &1))
            end

          Enum.reduce(targets, acc, fn target_node, inner ->
            Map.update(inner, target_node, [head], &[head | &1])
          end)
        else
          acc
        end
      end)

    Enum.reduce(heads_by_target, state, fn {target_node, heads}, acc ->
      outgoing_replica_message(
        acc,
        target_node,
        {:heads, WireProtocol.version(), Enum.reverse(heads)}
      )
    end)
  end

  defp expire_stale_replica_peers(state) do
    now = monotonic_millis()

    Enum.reduce(state.peer_last_seen, state, fn {remote_node, last_seen}, acc ->
      if now - last_seen > acc.replicated_peer_lease_timeout do
        expire_replica_peer(acc, remote_node)
      else
        acc
      end
    end)
  end

  defp expire_stale_snapshot_transfers(%{snapshot_transfers: transfers} = state)
       when map_size(transfers) == 0,
       do: state

  defp expire_stale_snapshot_transfers(state) do
    now = monotonic_millis()

    Enum.reduce(state.snapshot_transfers, state, fn {key, transfer}, acc ->
      if now - transfer.last_progress > acc.replicated_peer_lease_timeout do
        discard_snapshot_transfer(acc, key)
      else
        acc
      end
    end)
  end

  defp discard_snapshot_transfer(state, key) do
    case Map.pop(state.snapshot_transfers, key) do
      {nil, transfers} ->
        %{state | snapshot_transfers: transfers}

      {transfer, transfers} ->
        :ok = Snapshot.clear_staging_table(transfer.table)
        :ok = Snapshot.clear_staging_table(transfer.events)

        %{
          state
          | snapshot_transfers: transfers,
            snapshot_staging_pool: [
              {transfer.table, transfer.events} | state.snapshot_staging_pool
            ]
        }
    end
  end

  defp discard_snapshot_transfers_for_source(state, source_node) do
    Enum.reduce(state.snapshot_transfers, state, fn
      {{^source_node, _stream_id} = key, _transfer}, acc ->
        discard_snapshot_transfer(acc, key)

      {_key, _transfer}, acc ->
        acc
    end)
  end

  defp discard_snapshot_transfers_for_streams(state, stream_ids) do
    stream_ids = MapSet.new(stream_ids)

    Enum.reduce(state.snapshot_transfers, state, fn
      {{_source_node, stream_id} = key, _transfer}, acc ->
        if MapSet.member?(stream_ids, stream_id) do
          discard_snapshot_transfer(acc, key)
        else
          acc
        end
    end)
  end

  defp discard_snapshot_send_offsets_for_target(state, target_node) do
    offsets =
      Map.reject(state.snapshot_send_offsets, fn
        {{^target_node, _stream_id, _head}, _chunk_index} -> true
        {_key, _chunk_index} -> false
      end)

    %{state | snapshot_send_offsets: offsets}
  end

  defp discard_snapshot_send_offsets_for_streams(state, stream_ids) do
    stream_ids = MapSet.new(stream_ids)

    offsets =
      Map.reject(state.snapshot_send_offsets, fn
        {{_target_node, stream_id, _head}, _chunk_index} ->
          MapSet.member?(stream_ids, stream_id)
      end)

    %{state | snapshot_send_offsets: offsets}
  end

  defp expire_replica_peer(state, remote_node) do
    state = discard_snapshot_transfers_for_source(state, remote_node)
    state = discard_snapshot_send_offsets_for_target(state, remote_node)
    state = discard_pending_registry_reprojections(state, remote_node)
    %{name: name, shard_index: shard} = state

    Data.delete_replica_cursors_for_origin(name, shard, remote_node)

    {purged_reg, purged_pg} = Data.purge_node(name, shard, remote_node)
    affected_claims = Data.purge_registry_claims_for_origin(name, shard, remote_node)
    events = build_purged_events(name, purged_reg, purged_pg, :peer_lease_expired)

    {state, events} =
      Enum.reduce(affected_claims, {state, events}, fn {cluster, key}, {acc, inner_events} ->
        reconcile_registry_projection(acc, cluster, key, :peer_lease_expired, inner_events)
      end)

    notify_monitors(name, events)

    retirement = Data.expire_remote_replica_lane(name, shard, remote_node)
    if retirement == :node_retired, do: Data.purge_cluster_node(name, remote_node)

    if function_exported?(state.replica_transport, :peer_down, 4) do
      :ok =
        state.replica_transport.peer_down(
          name,
          remote_node,
          shard,
          state.replica_transport_opts
        )
    end

    %{
      state
      | remote_shards: Map.delete(state.remote_shards, remote_node),
        peer_last_seen: Map.delete(state.peer_last_seen, remote_node),
        cluster_control_dirty: Map.delete(state.cluster_control_dirty, remote_node),
        authority_dirty_notified: MapSet.delete(state.authority_dirty_notified, remote_node)
    }
  end

  defp send_replica_heads(state, target_node) do
    send_replica_heads(state, target_node, :all)
  end

  defp send_replica_heads(state, target_node, clusters) do
    heads = replica_heads_for_clusters(state, target_node, clusters)

    if heads == [] do
      state
    else
      outgoing_replica_message(state, target_node, {:heads, WireProtocol.version(), heads})
    end
  end

  defp replica_heads_for_clusters(state, target_node, :all) do
    state.name
    |> Data.replica_stream_heads(state.shard_index)
    |> Enum.filter(fn {stream_id, _floor, _head} ->
      replica_stream_target?(state, stream_id, target_node)
    end)
  end

  defp replica_heads_for_clusters(state, target_node, clusters) when is_list(clusters) do
    Enum.flat_map(clusters, fn cluster ->
      case Data.local_stream_id(state.name, state.shard_index, cluster) do
        nil ->
          []

        stream_id ->
          {floor, head, _applied} =
            Data.replica_stream_head(state.name, state.shard_index, stream_id)

          if head > 0 and replica_stream_target?(state, stream_id, target_node) do
            [{stream_id, floor, head}]
          else
            []
          end
      end
    end)
  end

  defp replica_stream_target?(state, stream_id, target_node) do
    current_local_replica_stream?(state, stream_id) and
      case WireProtocol.stream_cluster(stream_id) do
        nil -> Map.has_key?(state.peer_last_seen, target_node)
        cluster -> target_node in Data.cluster_nodes(state.name, cluster)
      end
  end

  defp current_local_replica_stream?(state, stream_id) do
    WireProtocol.valid_stream_id?(stream_id) and
      WireProtocol.stream_name(stream_id) == state.name and
      WireProtocol.stream_origin(stream_id) == node() and
      WireProtocol.stream_shard(stream_id) == state.shard_index and
      WireProtocol.stream_generation(stream_id) == Data.generation(state.name) and
      WireProtocol.stream_epoch(stream_id) ==
        Data.local_cluster_epoch(state.name, WireProtocol.stream_cluster(stream_id))
  end

  defp valid_remote_stream?(state, source_node, stream_id) do
    if WireProtocol.valid_stream_id?(stream_id) do
      cluster = WireProtocol.stream_cluster(stream_id)

      WireProtocol.stream_name(stream_id) == state.name and
        WireProtocol.stream_origin(stream_id) == source_node and
        WireProtocol.stream_shard(stream_id) == state.shard_index and
        replica_view_current?(state, source_node) and
        WireProtocol.stream_generation(stream_id) ==
          Data.remote_generation(state.name, source_node) and
        WireProtocol.stream_epoch(stream_id) ==
          Data.remote_cluster_epoch(state.name, source_node, cluster) and
        (is_nil(cluster) or cluster_member?(state.name, cluster))
    else
      false
    end
  end

  defp handle_replica_message(state, source_node, {:heads, version, heads})
       when version == @protocol_version and is_list(heads) do
    if Enum.all?(heads, &valid_replica_head?/1) do
      handle_replica_heads(state, source_node, heads)
    else
      state
    end
  end

  defp handle_replica_message(state, source_node, {:delta_batch, version, runs})
       when version == @protocol_version and is_list(runs) do
    if Enum.all?(runs, &valid_replica_delta_run?/1) do
      state = flush_pending_replicated_sender_barrier(state)

      Enum.reduce(runs, state, fn {stream_id, _first_seq, records, advertised_head}, acc ->
        apply_replica_delta_run(acc, source_node, stream_id, records, advertised_head)
      end)
    else
      state
    end
  end

  defp handle_replica_message(state, source_node, {:need, version, stream_id, next_seq})
       when version == @protocol_version and is_integer(next_seq) and next_seq > 0 do
    if WireProtocol.valid_stream_id?(stream_id) and
         WireProtocol.stream_origin(stream_id) == node() and
         WireProtocol.stream_shard(stream_id) == state.shard_index and
         replica_stream_target?(state, stream_id, source_node) do
      send_replica_repair(state, source_node, stream_id, next_seq)
    else
      state
    end
  end

  defp handle_replica_message(state, source_node, {:needs, version, needs})
       when version == @protocol_version and is_list(needs) do
    if Enum.all?(needs, &valid_replica_need?/1) do
      send_replica_repairs(state, source_node, needs)
    else
      state
    end
  end

  defp handle_replica_message(
         state,
         source_node,
         {:snapshot_chunk, version, stream_id, snapshot_seq, chunk_index, reg_data, pg_data}
       )
       when version == @protocol_version and is_integer(snapshot_seq) and snapshot_seq >= 0 and
              is_integer(chunk_index) and is_list(reg_data) and is_list(pg_data) do
    if valid_snapshot_stream?(state, source_node, stream_id, snapshot_seq) and
         valid_snapshot_chunk?(chunk_index, reg_data, pg_data) and
         valid_snapshot_rows?(state, source_node, stream_id, reg_data, pg_data) do
      stage_replica_snapshot_chunk(
        state,
        source_node,
        stream_id,
        snapshot_seq,
        chunk_index,
        reg_data,
        pg_data
      )
    else
      state
    end
  end

  defp handle_replica_message(
         state,
         source_node,
         {:snapshot_commit, version, stream_id, snapshot_seq, chunk_count, registry_count,
          pg_count}
       )
       when version == @protocol_version and is_integer(snapshot_seq) and snapshot_seq >= 0 and
              is_integer(chunk_count) and is_integer(registry_count) and is_integer(pg_count) do
    if valid_snapshot_stream?(state, source_node, stream_id, snapshot_seq) and
         valid_snapshot_commit_manifest?(chunk_count, registry_count, pg_count) do
      stage_replica_snapshot_commit(
        state,
        source_node,
        stream_id,
        snapshot_seq,
        chunk_count,
        registry_count,
        pg_count
      )
    else
      state
    end
  end

  defp handle_replica_message(state, _source_node, _message), do: state

  defp handle_replica_heads(state, source_node, heads) do
    needs =
      Enum.flat_map(heads, fn {stream_id, _floor, head} ->
        if valid_remote_stream?(state, source_node, stream_id) do
          cursor = Data.replica_cursor(state.name, state.shard_index, stream_id)
          if head > cursor, do: [{stream_id, cursor + 1}], else: []
        else
          []
        end
      end)

    needs
    |> Enum.chunk_every(state.replicated_sender_buffer_size)
    |> Enum.reduce(state, fn chunk, acc ->
      outgoing_replica_message(acc, source_node, {:needs, WireProtocol.version(), chunk})
    end)
  end

  defp valid_snapshot_stream?(state, source_node, stream_id, snapshot_seq) do
    valid_remote_stream?(state, source_node, stream_id) and
      snapshot_seq > Data.replica_cursor(state.name, state.shard_index, stream_id)
  end

  defp valid_snapshot_chunk?(chunk_index, reg_data, pg_data) do
    chunk_row_count = length(reg_data) + length(pg_data)
    chunk_index > 0 and (chunk_row_count > 0 or chunk_index == 1)
  end

  defp valid_snapshot_commit_manifest?(chunk_count, registry_count, pg_count) do
    total_count = registry_count + pg_count

    chunk_count > 0 and registry_count >= 0 and pg_count >= 0 and
      chunk_count <= max(total_count, 1) and
      (total_count > 0 or chunk_count == 1)
  end

  defp valid_snapshot_rows?(state, source_node, stream_id, reg_data, pg_data) do
    cluster = WireProtocol.stream_cluster(stream_id)

    Enum.all?(reg_data, fn
      {key, pid, meta, time}
      when is_binary(key) and is_pid(pid) and is_map(meta) and is_integer(time) ->
        node(pid) == source_node and
          shard_index_for(cluster, key, state.num_shards) == state.shard_index

      _other ->
        false
    end) and
      Enum.all?(pg_data, fn
        {key, pid, meta, time}
        when is_binary(key) and is_pid(pid) and is_map(meta) and is_integer(time) ->
          node(pid) == source_node and
            shard_index_for(cluster, key, state.num_shards) == state.shard_index

        _other ->
          false
      end) and unique_snapshot_rows?(reg_data, pg_data)
  end

  defp unique_snapshot_rows?(reg_data, pg_data) do
    reg_data
    |> MapSet.new(fn {key, _pid, _meta, _time} -> key end)
    |> MapSet.size() == length(reg_data) and
      pg_data
      |> MapSet.new(fn {key, pid, _meta, _time} -> {key, pid} end)
      |> MapSet.size() == length(pg_data)
  end

  defp valid_replica_head?({stream_id, floor, head}) do
    WireProtocol.valid_stream_id?(stream_id) and is_integer(floor) and floor >= 1 and
      is_integer(head) and head >= 0 and floor <= head + 1
  end

  defp valid_replica_head?(_head), do: false

  defp valid_replica_need?({stream_id, next_seq}) do
    WireProtocol.valid_stream_id?(stream_id) and is_integer(next_seq) and next_seq > 0
  end

  defp valid_replica_need?(_need), do: false

  defp valid_replica_delta_run?({stream_id, first_seq, records, advertised_head})
       when is_integer(first_seq) and first_seq > 0 and is_list(records) and records != [] and
              is_integer(advertised_head) and advertised_head >= first_seq do
    WireProtocol.valid_stream_id?(stream_id) and
      valid_replica_record_sequence?(records, first_seq, advertised_head)
  end

  defp valid_replica_delta_run?(_run), do: false

  defp valid_replica_record_sequence?(records, first_seq, advertised_head) do
    case Enum.reduce_while(records, first_seq, fn
           {seq, mutations}, expected when seq == expected and is_list(mutations) ->
             {:cont, expected + 1}

           _record, _expected ->
             {:halt, :invalid}
         end) do
      next_seq when is_integer(next_seq) -> next_seq - 1 <= advertised_head
      :invalid -> false
    end
  end

  defp stage_replica_snapshot_chunk(
         state,
         source_node,
         stream_id,
         snapshot_seq,
         chunk_index,
         reg_data,
         pg_data
       ) do
    key = {source_node, stream_id}

    case snapshot_transfer(state, key, snapshot_seq) do
      {:ignore, state} ->
        state

      {:ok, state, transfer} ->
        cond do
          MapSet.member?(transfer.received, chunk_index) and
              Snapshot.chunk_matches?(transfer.table, chunk_index, reg_data, pg_data) ->
            state

          MapSet.member?(transfer.received, chunk_index) ->
            discard_snapshot_transfer(state, key)

          not snapshot_chunk_within_manifest?(transfer, chunk_index, reg_data, pg_data) ->
            discard_snapshot_transfer(state, key)

          true ->
            case Snapshot.stage_rows(transfer.table, chunk_index, reg_data, pg_data) do
              :ok ->
                transfer = %{
                  transfer
                  | received: MapSet.put(transfer.received, chunk_index),
                    registry_seen: transfer.registry_seen + length(reg_data),
                    pg_seen: transfer.pg_seen + length(pg_data),
                    last_progress: monotonic_millis()
                }

                state = put_snapshot_transfer(state, key, transfer)
                maybe_commit_snapshot_transfer(state, key, source_node, stream_id)

              {:error, :duplicate_row} ->
                discard_snapshot_transfer(state, key)
            end
        end
    end
  end

  defp stage_replica_snapshot_commit(
         state,
         source_node,
         stream_id,
         snapshot_seq,
         chunk_count,
         registry_count,
         pg_count
       ) do
    key = {source_node, stream_id}
    manifest = {chunk_count, registry_count, pg_count}

    case snapshot_transfer(state, key, snapshot_seq) do
      {:ignore, state} ->
        state

      {:ok, state, %{manifest: nil} = transfer} ->
        if snapshot_transfer_within_manifest?(transfer, manifest) do
          transfer = %{transfer | manifest: manifest, last_progress: monotonic_millis()}
          state = put_snapshot_transfer(state, key, transfer)
          maybe_commit_snapshot_transfer(state, key, source_node, stream_id)
        else
          discard_snapshot_transfer(state, key)
        end

      {:ok, state, %{manifest: ^manifest}} ->
        maybe_commit_snapshot_transfer(state, key, source_node, stream_id)

      {:ok, state, _conflicting_transfer} ->
        discard_snapshot_transfer(state, key)
    end
  end

  defp snapshot_transfer(state, key, snapshot_seq) do
    case Map.get(state.snapshot_transfers, key) do
      nil ->
        {state, transfer} = new_snapshot_transfer(state, snapshot_seq)
        {:ok, put_snapshot_transfer(state, key, transfer), transfer}

      %{snapshot_seq: existing_seq} when existing_seq > snapshot_seq ->
        {:ignore, state}

      %{snapshot_seq: existing_seq} when existing_seq < snapshot_seq ->
        state = discard_snapshot_transfer(state, key)
        {state, transfer} = new_snapshot_transfer(state, snapshot_seq)
        {:ok, put_snapshot_transfer(state, key, transfer), transfer}

      %{snapshot_seq: ^snapshot_seq} = transfer ->
        {:ok, state, transfer}
    end
  end

  defp new_snapshot_transfer(state, snapshot_seq) do
    {table, events, pool} =
      case state.snapshot_staging_pool do
        [{table, events} | pool] -> {table, events, pool}
        [] -> {Snapshot.new_staging_table(), Snapshot.new_event_table(), []}
      end

    transfer = %{
      snapshot_seq: snapshot_seq,
      manifest: nil,
      registry_seen: 0,
      pg_seen: 0,
      received: MapSet.new(),
      last_progress: monotonic_millis(),
      table: table,
      events: events
    }

    {%{state | snapshot_staging_pool: pool}, transfer}
  end

  defp put_snapshot_transfer(state, key, transfer) do
    %{state | snapshot_transfers: Map.put(state.snapshot_transfers, key, transfer)}
  end

  defp maybe_commit_snapshot_transfer(state, key, source_node, stream_id) do
    transfer = Map.fetch!(state.snapshot_transfers, key)

    case transfer.manifest do
      {chunk_count, registry_count, pg_count} ->
        if MapSet.size(transfer.received) == chunk_count and
             transfer.registry_seen == registry_count and transfer.pg_seen == pg_count do
          commit_snapshot_transfer(state, key, source_node, stream_id, transfer)
        else
          state
        end

      nil ->
        state
    end
  end

  defp snapshot_chunk_within_manifest?(%{manifest: nil}, _chunk_index, _reg_data, _pg_data),
    do: true

  defp snapshot_chunk_within_manifest?(
         %{manifest: {chunk_count, registry_count, pg_count}} = transfer,
         chunk_index,
         reg_data,
         pg_data
       ) do
    chunk_index <= chunk_count and
      transfer.registry_seen + length(reg_data) <= registry_count and
      transfer.pg_seen + length(pg_data) <= pg_count
  end

  defp snapshot_transfer_within_manifest?(
         transfer,
         {chunk_count, registry_count, pg_count}
       ) do
    Enum.all?(transfer.received, &(&1 <= chunk_count)) and
      transfer.registry_seen <= registry_count and transfer.pg_seen <= pg_count
  end

  defp commit_snapshot_transfer(state, key, source_node, stream_id, transfer) do
    {chunk_count, _registry_count, _pg_count} = transfer.manifest

    state =
      if valid_snapshot_stream?(state, source_node, stream_id, transfer.snapshot_seq) do
        state = flush_pending_replicated_barrier(state)
        cluster = WireProtocol.stream_cluster(stream_id)

        :ok =
          Data.begin_replica_snapshot_install(
            state.name,
            state.shard_index,
            stream_id,
            transfer.snapshot_seq
          )

        event_buffer = Snapshot.new_event_buffer(transfer.events)

        {state, event_buffer} =
          Data.replace_registry_claims_for_stream_from_staging(
            state.name,
            state.shard_index,
            stream_id,
            transfer.snapshot_seq,
            transfer.table,
            chunk_count,
            {state, event_buffer},
            fn key, {acc, buffer} ->
              {acc, events} = reconcile_registry_projection(acc, cluster, key, :reconcile, [])
              {acc, Snapshot.buffer_events(Enum.reverse(events), buffer)}
            end,
            fn claims, {acc, buffer} ->
              reconcile_registry_snapshot_batch(acc, cluster, stream_id, claims, buffer)
            end
          )

        event_buffer =
          replace_remote_pg_snapshot_from_staging(
            state,
            source_node,
            cluster,
            transfer.table,
            chunk_count,
            event_buffer
          )

        _event_buffer = Snapshot.finish_event_buffer(event_buffer)

        :ok =
          Data.put_replica_cursor(
            state.name,
            state.shard_index,
            stream_id,
            transfer.snapshot_seq
          )

        notify_snapshot_events(state.name, transfer.events)
        state
      else
        state
      end

    discard_snapshot_transfer(state, key)
  end

  defp apply_replica_delta_run(state, source_node, stream_id, records, advertised_head) do
    if valid_remote_stream?(state, source_node, stream_id) do
      cursor = Data.replica_cursor(state.name, state.shard_index, stream_id)
      records = Enum.drop_while(records, fn {seq, _mutations} -> seq <= cursor end)

      case records do
        [] ->
          state

        [{first_seq, _mutations} | _] when first_seq > cursor + 1 ->
          request_replica_need(state, source_node, stream_id, cursor + 1)

        _ ->
          {contiguous, _next_seq} = take_contiguous_replica_records(records, cursor + 1, [])

          {accepted, rejected} =
            Enum.split_while(contiguous, fn {_seq, mutations} ->
              valid_replica_mutations?(state, stream_id, mutations)
            end)

          if rejected != [] do
            Logger.error(
              "#{log_prefix_shard(state)} rejected replica record with invalid origin/cluster authority from #{inspect(source_node)}"
            )
          end

          state =
            if accepted == [] do
              state
            else
              :ok = Data.ensure_replica_cursor(state.name, state.shard_index, stream_id)
              apply_received_replica_records(state, stream_id, accepted)
            end
            |> flush_pending_replicated_barrier()

          case List.last(accepted) do
            nil ->
              if rejected == [] do
                state
              else
                request_replica_need(state, source_node, stream_id, cursor + 1)
              end

            {last_seq, _mutations} ->
              :ok = Data.put_replica_cursor(state.name, state.shard_index, stream_id, last_seq)

              if last_seq < advertised_head or length(accepted) < length(records) do
                request_replica_need(state, source_node, stream_id, last_seq + 1)
              else
                state
              end
          end
      end
    else
      state
    end
  end

  defp take_contiguous_replica_records([], next_seq, acc), do: {Enum.reverse(acc), next_seq}

  defp take_contiguous_replica_records([{seq, mutations} | rest], seq, acc) do
    take_contiguous_replica_records(rest, seq + 1, [{seq, mutations} | acc])
  end

  defp take_contiguous_replica_records(_records, next_seq, acc),
    do: {Enum.reverse(acc), next_seq}

  defp valid_replica_mutations?(state, stream_id, mutations) do
    origin = WireProtocol.stream_origin(stream_id)
    cluster = WireProtocol.stream_cluster(stream_id)

    mutations != [] and
      Enum.all?(
        mutations,
        &valid_replica_mutation?(
          &1,
          cluster,
          origin,
          state.shard_index,
          state.num_shards
        )
      )
  end

  defp valid_replica_mutation?(
         {:register, cluster, key, pid, meta, time, entry_node},
         cluster,
         origin,
         shard,
         num_shards
       )
       when is_binary(key) and is_pid(pid) and is_map(meta) and is_integer(time),
       do:
         node(pid) == origin and entry_node == origin and
           shard_index_for(cluster, key, num_shards) == shard

  defp valid_replica_mutation?(
         {:unregister, cluster, key, pid, meta, _reason},
         cluster,
         origin,
         shard,
         num_shards
       )
       when is_binary(key) and is_pid(pid) and is_map(meta),
       do: node(pid) == origin and shard_index_for(cluster, key, num_shards) == shard

  defp valid_replica_mutation?(
         {:join, cluster, key, pid, meta, time, _reason, entry_node},
         cluster,
         origin,
         shard,
         num_shards
       )
       when is_binary(key) and is_pid(pid) and is_map(meta) and is_integer(time),
       do:
         node(pid) == origin and entry_node == origin and
           shard_index_for(cluster, key, num_shards) == shard

  defp valid_replica_mutation?(
         {:leave, cluster, key, pid, meta, _reason},
         cluster,
         origin,
         shard,
         num_shards
       )
       when is_binary(key) and is_pid(pid) and is_map(meta),
       do: node(pid) == origin and shard_index_for(cluster, key, num_shards) == shard

  defp valid_replica_mutation?(_mutation, _cluster, _origin, _shard, _num_shards), do: false

  defp apply_received_replica_records(state, stream_id, records) do
    records
    |> Enum.chunk_by(fn {_seq, mutations} -> replica_record_domain(mutations) end)
    |> Enum.reduce(state, fn records, acc ->
      case records |> hd() |> elem(1) |> replica_record_domain() do
        :registry ->
          acc = flush_pending_replicated_pg_barrier(acc)

          {acc, events} =
            Enum.reduce(records, {acc, []}, fn {seq, mutations}, {inner, events} ->
              {inner, record_events} =
                apply_received_registry_claims(inner, stream_id, seq, mutations)

              {inner, record_events ++ events}
            end)

          notify_monitors(acc.name, events)
          acc

        _pg_or_mixed ->
          Enum.reduce(records, acc, fn {seq, mutations}, inner ->
            apply_received_replica_record(inner, stream_id, seq, mutations)
          end)
      end
    end)
  end

  defp replica_record_domain(mutations) do
    case mutations |> Enum.map(&replica_mutation_domain/1) |> Enum.uniq() do
      [domain] -> domain
      [] -> :empty
      _ -> :mixed
    end
  end

  defp enqueue_received_replica_mutation(state, {:register, _, _, _, _, _, _} = op) do
    {state, _flushed?} = enqueue_replicated_registry_ops(state, [op])
    state
  end

  defp enqueue_received_replica_mutation(state, {:unregister, _, _, _, _, _} = op) do
    {state, _flushed?} = enqueue_replicated_registry_ops(state, [op])
    state
  end

  defp enqueue_received_replica_mutation(state, {:join, _, _, _, _, _, _, _} = op) do
    {state, _flushed?} = enqueue_replicated_pg_ops(state, [op])
    state
  end

  defp enqueue_received_replica_mutation(state, {:leave, _, _, _, _, _} = op) do
    {state, _flushed?} = enqueue_replicated_pg_ops(state, [op])
    state
  end

  defp apply_received_replica_record(state, stream_id, seq, mutations) do
    domains = mutations |> Enum.map(&replica_mutation_domain/1) |> Enum.uniq()

    if length(domains) > 1 do
      apply_received_mixed_replica_record(state, stream_id, seq, mutations)
    else
      apply_received_homogeneous_replica_record(state, stream_id, seq, mutations, domains)
    end
  end

  defp apply_received_homogeneous_replica_record(
         state,
         stream_id,
         seq,
         mutations,
         [:registry]
       ) do
    state = flush_pending_replicated_pg_barrier(state)
    {state, _events} = apply_received_registry_claims(state, stream_id, seq, mutations)
    state
  end

  defp apply_received_homogeneous_replica_record(state, _stream_id, _seq, mutations, [:pg]) do
    Enum.reduce(mutations, state, fn op, acc ->
      enqueue_received_replica_mutation(acc, op)
    end)
  end

  defp apply_received_homogeneous_replica_record(state, _stream_id, _seq, [], []), do: state

  # Process death may remove registry and PG rows in one authoritative record.
  # Apply its maximal same-domain segments in wire order and emit one monitor
  # batch, preserving the existing process-down batching contract.
  defp apply_received_mixed_replica_record(state, stream_id, seq, mutations) do
    state = flush_pending_replicated_barrier(state)

    {state, events} =
      mutations
      |> Enum.chunk_by(&replica_mutation_domain/1)
      |> Enum.reduce({state, []}, fn segment, {acc, events} ->
        case replica_mutation_domain(hd(segment)) do
          :registry ->
            {acc, segment_events} = apply_received_registry_claims(acc, stream_id, seq, segment)
            {acc, segment_events ++ events}

          :pg ->
            {insert_entries, delete_entries, segment_events} =
              apply_replicated_pg_ops(acc.name, acc.shard_index, segment)

            Data.pg_delete_many(acc.name, acc.shard_index, delete_entries)
            Data.pg_insert_many(acc.name, acc.shard_index, insert_entries)
            {acc, segment_events ++ events}
        end
      end)

    notify_monitors(state.name, events)
    state
  end

  defp replica_mutation_domain({:register, _, _, _, _, _, _}), do: :registry
  defp replica_mutation_domain({:unregister, _, _, _, _, _}), do: :registry
  defp replica_mutation_domain({:join, _, _, _, _, _, _, _}), do: :pg
  defp replica_mutation_domain({:leave, _, _, _, _, _}), do: :pg

  defp apply_received_registry_claims(state, stream_id, seq, ops) do
    keys =
      Enum.map(ops, fn
        {:register, _cluster, key, pid, meta, time, _entry_node} ->
          Data.put_registry_claim(
            state.name,
            state.shard_index,
            stream_id,
            seq,
            key,
            pid,
            meta,
            time
          )

          key

        {:unregister, _cluster, key, pid, _meta, _reason} ->
          Data.delete_registry_claim(state.name, state.shard_index, stream_id, seq, key, pid)
          key
      end)
      |> Enum.uniq()

    cluster = WireProtocol.stream_cluster(stream_id)

    Enum.reduce(keys, {state, []}, fn key, {acc, events} ->
      reconcile_registry_projection(acc, cluster, key, :reconcile, events)
    end)
  end

  defp send_replica_repair(state, target_node, stream_id, next_seq) do
    send_replica_repairs(state, target_node, [{stream_id, next_seq}])
  end

  defp request_replica_need(state, target_node, stream_id, next_seq) do
    outgoing_replica_message(
      state,
      target_node,
      {:needs, WireProtocol.version(), [{stream_id, next_seq}]}
    )
  end

  defp send_replica_repairs(state, target_node, needs) do
    {state, runs} =
      Enum.reduce(needs, {state, []}, fn {stream_id, next_seq}, {acc, runs} ->
        if WireProtocol.stream_origin(stream_id) == node() and
             WireProtocol.stream_shard(stream_id) == acc.shard_index and
             replica_stream_target?(acc, stream_id, target_node) do
          case replica_repair(acc, target_node, stream_id, next_seq) do
            {:run, run} -> {acc, [run | runs]}
            {:state, acc} -> {acc, runs}
          end
        else
          {acc, runs}
        end
      end)

    case runs do
      [] ->
        state

      runs ->
        outgoing_replica_message(
          state,
          target_node,
          {:delta_batch, WireProtocol.version(), Enum.reverse(runs)}
        )
    end
  end

  defp replica_repair(state, target_node, stream_id, next_seq) do
    {floor, head, _applied} =
      Data.replica_stream_head(state.name, state.shard_index, stream_id)

    cond do
      next_seq > head ->
        {:state, state}

      next_seq >= floor ->
        records =
          Data.replica_records(
            state.name,
            state.shard_index,
            stream_id,
            next_seq,
            state.replicated_sender_buffer_size
          )

        case records do
          [] ->
            {:state, send_replica_snapshot(state, target_node, stream_id, head)}

          [{first_seq, _} | _] ->
            {:run, {stream_id, first_seq, records, head}}
        end

      true ->
        {:state, send_replica_snapshot(state, target_node, stream_id, head)}
    end
  end

  defp send_replica_snapshot(state, target_node, stream_id, head) do
    case state.snapshot_send do
      {worker, _token, _snapshot_key} when is_pid(worker) ->
        if Process.alive?(worker),
          do: state,
          else: start_replica_snapshot_send(state, target_node, stream_id, head)

      nil ->
        start_replica_snapshot_send(state, target_node, stream_id, head)
    end
  end

  defp start_replica_snapshot_send(state, target_node, stream_id, head) do
    owner = self()
    token = make_ref()
    snapshot_key = {target_node, stream_id, head}

    offsets =
      state.snapshot_send_offsets
      |> Enum.reject(fn
        {{^target_node, ^stream_id, other_head}, _chunk_index} -> other_head != head
        {_other_key, _chunk_index} -> false
      end)
      |> Map.new()

    resume = Map.get(offsets, snapshot_key, {:chunk, 1})

    snapshot_context = %{
      name: state.name,
      shard_index: state.shard_index,
      replicated_snapshot_chunk_target_bytes: state.replicated_snapshot_chunk_target_bytes,
      replica_transport: state.replica_transport,
      replica_transport_opts: state.replica_transport_opts
    }

    worker =
      spawn(fn ->
        result =
          try do
            stream_replica_snapshot(
              snapshot_context,
              target_node,
              stream_id,
              head,
              resume
            )
          catch
            kind, reason ->
              Logger.error(
                "#{log_prefix_shard(snapshot_context)} snapshot capture failed: " <>
                  Exception.format_banner(kind, reason)
              )

              :retry
          end

        send(
          owner,
          {:replica_snapshot_send_complete, token, self(), snapshot_key, result}
        )
      end)

    %{state | snapshot_send: {worker, token, snapshot_key}, snapshot_send_offsets: offsets}
  end

  defp stream_replica_snapshot(
         state,
         target_node,
         stream_id,
         head,
         {:commit, manifest}
       ) do
    if current_snapshot_send?(state, target_node, stream_id, head) do
      send_replica_snapshot_commit(state, target_node, stream_id, head, manifest)
    else
      :complete
    end
  end

  defp stream_replica_snapshot(state, target_node, stream_id, head, {:chunk, start_index}) do
    if current_snapshot_send?(state, target_node, stream_id, head) do
      do_stream_replica_snapshot(state, target_node, stream_id, head, start_index)
    else
      :complete
    end
  end

  defp do_stream_replica_snapshot(state, target_node, stream_id, head, start_index) do
    cluster = WireProtocol.stream_cluster(stream_id)
    envelope_bytes = Snapshot.stream_envelope_bytes(stream_id, head)

    emit = fn registry, pg, chunk_index ->
      message =
        {:snapshot_chunk, WireProtocol.version(), stream_id, head, chunk_index, registry, pg}

      case state.replica_transport.outgoing(
             state.name,
             target_node,
             state.shard_index,
             message,
             state.replica_transport_opts
           ) do
        :ok -> :ok
        result when result in [:busy, :disconnected] -> throw({:snapshot_resume, chunk_index})
      end
    end

    try do
      stream =
        Snapshot.new_stream(
          state.replicated_snapshot_chunk_target_bytes,
          envelope_bytes,
          start_index,
          emit
        )

      stream =
        Data.reduce_registry_claim_batches_for_stream(
          state.name,
          state.shard_index,
          stream_id,
          stream,
          &Snapshot.stream_registry_many/2
        )

      stream =
        Data.reduce_pg_entry_batches_for_origin(
          state.name,
          state.shard_index,
          cluster,
          node(),
          stream,
          &Snapshot.stream_pg_many/2
        )

      stream = Snapshot.finish_stream(stream)
      manifest = {stream.chunk_count, stream.registry_count, stream.pg_count}

      # Chunks are provisional. Appending advances the head before changing
      # materialized rows and advances `applied` only afterward, so an
      # unchanged fully-applied head proves the completed scan is one exact
      # state at `head`. Only the terminal commit makes those chunks visible.
      if current_snapshot_send?(state, target_node, stream_id, head) do
        send_replica_snapshot_commit(state, target_node, stream_id, head, manifest)
      else
        :complete
      end
    catch
      {:snapshot_resume, chunk_index} -> {:resume, chunk_index}
    end
  end

  defp send_replica_snapshot_commit(
         state,
         target_node,
         stream_id,
         head,
         {chunk_count, registry_count, pg_count} = manifest
       ) do
    message =
      {:snapshot_commit, WireProtocol.version(), stream_id, head, chunk_count, registry_count,
       pg_count}

    case state.replica_transport.outgoing(
           state.name,
           target_node,
           state.shard_index,
           message,
           state.replica_transport_opts
         ) do
      :ok -> :complete
      result when result in [:busy, :disconnected] -> {:resume_commit, manifest}
    end
  end

  defp current_snapshot_send?(state, {target_node, stream_id, head}),
    do: current_snapshot_send?(state, target_node, stream_id, head)

  defp current_snapshot_send?(state, target_node, stream_id, head) do
    cluster = WireProtocol.stream_cluster(stream_id)

    {_floor, current_head, applied} =
      Data.replica_stream_head(state.name, state.shard_index, stream_id)

    Data.local_stream_id(state.name, state.shard_index, cluster) == stream_id and
      current_head == head and applied == head and
      target_node in Data.cluster_nodes(state.name, cluster)
  end

  defp replace_remote_pg_snapshot_from_staging(
         state,
         source_node,
         cluster,
         staging_table,
         _chunk_count,
         event_buffer
       ) do
    event_buffer =
      Snapshot.reduce_pg_batches(staging_table, event_buffer, fn rows, buffer ->
        {inserts, buffer} =
          Enum.reduce(rows, {[], buffer}, fn {key, pid, meta, time}, {inserts, inner} ->
            case Data.pg_lookup(state.name, state.shard_index, cluster, key, pid) do
              nil ->
                event = build_event(state.name, :joined, key, pid, meta, %{cluster: cluster})

                {
                  [{cluster, key, pid, meta, time, source_node} | inserts],
                  Snapshot.buffer_event(event, inner)
                }

              {^meta, ^time, ^source_node} ->
                {inserts, inner}

              {old_meta, _old_time, ^source_node} ->
                inner =
                  if old_meta != meta do
                    Snapshot.buffer_event(
                      build_event(state.name, :joined, key, pid, meta, %{
                        previous_meta: old_meta,
                        cluster: cluster
                      }),
                      inner
                    )
                  else
                    inner
                  end

                {[{cluster, key, pid, meta, time, source_node} | inserts], inner}
            end
          end)

        :ok = Data.pg_insert_many(state.name, state.shard_index, Enum.reverse(inserts))
        buffer
      end)

    event_buffer =
      Data.fold_pg_entries_for_origin(
        state.name,
        state.shard_index,
        cluster,
        source_node,
        event_buffer,
        fn {key, pid, old_meta, _old_time}, buffer ->
          if Snapshot.member_pg?(staging_table, key, pid) do
            buffer
          else
            :ok = Data.pg_delete(state.name, state.shard_index, cluster, key, pid)

            event =
              build_event(state.name, :left, key, pid, old_meta, %{
                reason: :reconcile,
                cluster: cluster
              })

            Snapshot.buffer_event(event, buffer)
          end
        end
      )

    event_buffer
  end

  defp maybe_purge_remote_generation(state, _remote_node, nil, _generation), do: state

  defp maybe_purge_remote_generation(state, _remote_node, generation, generation), do: state

  defp maybe_purge_remote_generation(state, remote_node, _old_generation, _generation) do
    state = discard_snapshot_transfers_for_source(state, remote_node)
    state = discard_pending_registry_reprojections(state, remote_node)
    Data.delete_replica_cursors_for_origin(state.name, state.shard_index, remote_node)
    {_reg, _pg} = Data.purge_node(state.name, state.shard_index, remote_node)

    affected =
      Data.purge_registry_claims_for_origin(
        state.name,
        state.shard_index,
        remote_node
      )

    {state, events} =
      Enum.reduce(affected, {state, []}, fn {cluster, key}, {acc, inner_events} ->
        reconcile_registry_projection(acc, cluster, key, :nodedown, inner_events)
      end)

    notify_monitors(state.name, events)
    state
  end

  defp purge_closed_remote_epochs(state, _remote_node, []), do: state

  defp purge_closed_remote_epochs(state, remote_node, cluster_epochs) do
    generation = Data.remote_generation(state.name, remote_node)

    stream_ids =
      Enum.map(cluster_epochs, fn {cluster, epoch} ->
        WireProtocol.stream_id(
          state.name,
          remote_node,
          generation,
          state.shard_index,
          cluster,
          epoch
        )
      end)

    state = discard_snapshot_transfers_for_streams(state, stream_ids)

    Enum.each(stream_ids, fn stream_id ->
      :ok = Data.delete_replica_cursor(state.name, state.shard_index, stream_id)
    end)

    affected_keys =
      Data.purge_registry_claims_for_streams(
        state.name,
        state.shard_index,
        stream_ids
      )

    clusters = cluster_epochs |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

    purged_pg =
      Data.delete_pg_for_origin_clusters(
        state.name,
        state.shard_index,
        clusters,
        remote_node
      )

    events = build_purged_events(state.name, [], purged_pg, :cluster_disconnect, [])

    {state, events} =
      Enum.reduce(affected_keys, {state, events}, fn {cluster, key},
                                                     {inner_state, inner_events} ->
        reconcile_registry_projection(
          inner_state,
          cluster,
          key,
          :cluster_disconnect,
          inner_events
        )
      end)

    notify_monitors(state.name, events)

    state
  end

  defp purge_superseded_remote_streams(state, remote_node, current_epochs) do
    generation = Data.remote_generation(state.name, remote_node)

    current_epochs = Map.new(current_epochs)

    superseded =
      Enum.flat_map(current_epochs, fn {cluster, current_epoch} ->
        current_stream =
          WireProtocol.stream_id(
            state.name,
            remote_node,
            generation,
            state.shard_index,
            cluster,
            current_epoch
          )

        state.name
        |> Data.replica_cursor_streams_for_origin_cluster(
          state.shard_index,
          remote_node,
          cluster
        )
        |> Enum.reject(&(&1 == current_stream))
      end)

    purge_superseded_remote_streams(state, remote_node, current_epochs, superseded)
  end

  defp purge_remote_streams_outside_authority(state, remote_node) do
    generation = Data.remote_generation(state.name, remote_node)

    streams =
      Data.replica_cursor_streams_for_origin(
        state.name,
        state.shard_index,
        remote_node
      )

    # A shard only needs authority for clusters for which it has retained
    # receive state. This keeps local fanout proportional to actual shard data,
    # rather than rebuilding the node-wide epoch map in every lane.
    current_epochs =
      streams
      |> Enum.map(&WireProtocol.stream_cluster/1)
      |> Enum.uniq()
      |> Map.new(fn cluster ->
        {cluster, Data.remote_cluster_epoch(state.name, remote_node, cluster)}
      end)

    superseded =
      Enum.reject(streams, fn stream_id ->
        WireProtocol.stream_generation(stream_id) == generation and
          Map.get(current_epochs, WireProtocol.stream_cluster(stream_id)) ==
            WireProtocol.stream_epoch(stream_id)
      end)

    purge_superseded_remote_streams(state, remote_node, current_epochs, superseded)
  end

  defp purge_superseded_remote_streams(state, _remote_node, _current_epochs, []), do: state

  defp purge_superseded_remote_streams(
         state,
         remote_node,
         current_epochs,
         superseded
       ) do
    state = discard_snapshot_transfers_for_streams(state, superseded)
    generation = Data.remote_generation(state.name, remote_node)

    superseded
    |> Enum.group_by(&WireProtocol.stream_cluster/1)
    |> Enum.reduce(state, fn {cluster, cluster_streams}, acc ->
      Enum.each(cluster_streams, fn stream_id ->
        :ok = Data.delete_replica_cursor(state.name, state.shard_index, stream_id)
      end)

      case Map.get(current_epochs, cluster) do
        nil ->
          :ok

        current_epoch ->
          current_stream =
            WireProtocol.stream_id(
              state.name,
              remote_node,
              generation,
              state.shard_index,
              cluster,
              current_epoch
            )

          :ok = Data.delete_replica_cursor(state.name, state.shard_index, current_stream)
      end

      affected_keys =
        Data.purge_registry_claims_for_streams(
          state.name,
          state.shard_index,
          cluster_streams
        )

      # PG rows do not carry their stream epoch. Remove the origin/cluster
      # slice and reset the current cursor so its exact state is rebuilt by
      # the next head advertisement (delta when retained, snapshot after
      # pruning). Registry claims do carry epochs and are removed narrowly.
      purged_pg =
        Data.delete_pg_for_origin_clusters(
          state.name,
          state.shard_index,
          [cluster],
          remote_node
        )

      events = build_purged_events(state.name, [], purged_pg, :cluster_disconnect, [])

      {acc, events} =
        Enum.reduce(affected_keys, {acc, events}, fn {affected_cluster, key},
                                                     {inner, inner_events} ->
          reconcile_registry_projection(
            inner,
            affected_cluster,
            key,
            :cluster_disconnect,
            inner_events
          )
        end)

      notify_monitors(state.name, events)

      acc
    end)
  end

  defp append_process_down_records(state, reason_by_pid, reg_entries, pg_entries) do
    mutations_by_cluster =
      Enum.reduce(reg_entries, %{}, fn {pid, cluster, key, meta}, acc ->
        op = {:unregister, cluster, key, pid, meta, Map.fetch!(reason_by_pid, pid)}
        Map.update(acc, cluster, [op], &[op | &1])
      end)
      |> then(fn acc ->
        Enum.reduce(pg_entries, acc, fn {pid, cluster, key, meta}, inner ->
          op = {:leave, cluster, key, pid, meta, Map.fetch!(reason_by_pid, pid)}
          Map.update(inner, cluster, [op], &[op | &1])
        end)
      end)

    Enum.flat_map(mutations_by_cluster, fn {cluster, mutations} ->
      case Data.local_stream_id(state.name, state.shard_index, cluster) do
        nil ->
          []

        stream_id ->
          {seq, mutations} =
            Data.append_replica_record(
              state.name,
              state.shard_index,
              stream_id,
              Enum.reverse(mutations)
            )

          [{:sequenced, stream_id, seq, mutations}]
      end
    end)
  end

  defp finish_process_down_records(state, records) do
    Enum.each(records, fn {:sequenced, stream_id, seq, mutations} ->
      apply_registry_claim_mutations(state, stream_id, seq, mutations)
      :ok = Data.mark_local_replica_applied(state.name, state.shard_index, stream_id, seq)
    end)

    :ok =
      Data.prune_replica_oplog(
        state.name,
        state.shard_index,
        state.replicated_oplog_max_entries
      )

    records
    |> Enum.reduce(%{}, fn {:sequenced, _stream_id, _seq, [op | _]} = record, acc ->
      cluster = WireProtocol.op_cluster(op)

      Enum.reduce(process_down_targets(state, cluster), acc, fn target_node, inner ->
        Map.update(inner, target_node, [record], &[record | &1])
      end)
    end)
    |> Enum.reduce(state, fn {target_node, target_records}, acc ->
      send_replica_delta_batch(acc, target_node, Enum.reverse(target_records))
    end)
  end

  defp process_down_targets(state, nil) do
    for {target_node, _last_seen} <- state.peer_last_seen, do: target_node
  end

  defp process_down_targets(%{name: name}, cluster) do
    for target_node <- Data.cluster_nodes(name, cluster), target_node != node(), do: target_node
  end

  defp collect_local_process_downs(acc, monitors, 0), do: {Enum.reverse(acc), monitors}

  defp collect_local_process_downs(acc, monitors, remaining) do
    receive do
      {:DOWN, _mref, :process, pid, reason} when is_map_key(monitors, pid) ->
        collect_local_process_downs([{pid, reason} | acc], monitors, remaining - 1)
    after
      0 ->
        {Enum.reverse(acc), monitors}
    end
  end

  defp build_process_down_events(name, reg_entries, pg_entries, reason_by_pid) do
    events =
      Enum.reduce(reg_entries, [], fn {pid, cluster, key, meta}, acc ->
        [
          build_event(name, :unregistered, key, pid, meta, %{
            reason: Map.fetch!(reason_by_pid, pid),
            cluster: cluster
          })
          | acc
        ]
      end)

    Enum.reduce(pg_entries, events, fn {pid, cluster, key, meta}, acc ->
      [
        build_event(name, :left, key, pid, meta, %{
          reason: Map.fetch!(reason_by_pid, pid),
          cluster: cluster
        })
        | acc
      ]
    end)
  end

  defp fan_out_to_siblings(state, message) do
    %{name: name, shard_index: shard_index, num_shards: num_shards} = state

    for i <- 0..(num_shards - 1), i != shard_index do
      case Process.whereis(shard_name(name, i)) do
        pid when is_pid(pid) -> send(pid, message)
        nil -> :ok
      end
    end
  end

  defp send_local_control_message(state, message) do
    case Process.whereis(shard_name(state.name, 0)) do
      pid when is_pid(pid) ->
        send(pid, message)
        :ok

      nil ->
        :disconnected
    end
  end

  defp monitor_pid(state, pid) do
    Map.get(state.monitors, pid) || Process.monitor(pid)
  end

  defp put_monitor(state, pid, mref) do
    %{state | monitors: Map.put_new(state.monitors, pid, mref)}
  end

  defp maybe_demonitor_pid(state, name, shard, pid) do
    case Data.maybe_demonitor(name, shard, pid) do
      :ok ->
        case Map.pop(state.monitors, pid) do
          {nil, monitors} ->
            %{state | monitors: monitors}

          {mref, monitors} ->
            Process.demonitor(mref, [:flush])
            %{state | monitors: monitors}
        end

      :still_monitored ->
        state
    end
  end

  defp rebuild_monitors(state) do
    %{name: name, shard_index: shard} = state
    local = node()

    # Scan all entries in this shard's tables and re-establish monitors
    # Only monitor local pids — remote pids are cleaned up by their owning
    # node's DOWN handler (broadcast) or by nodedown.
    reg_table = Data.reg_by_pid_table(name, shard)

    monitors =
      try do
        :ets.foldl(
          fn {{pid, _cluster, _key}, _meta, _time, entry_node}, acc ->
            if entry_node == local and not Map.has_key?(acc, pid) do
              mref = Process.monitor(pid)
              Map.put(acc, pid, mref)
            else
              acc
            end
          end,
          %{},
          reg_table
        )
      rescue
        ArgumentError -> %{}
      end

    pg_table = Data.pg_by_pid_table(name, shard)

    monitors =
      try do
        :ets.foldl(
          fn {{pid, _cluster, _key}, _meta, _time, entry_node}, acc ->
            if entry_node == local and not Map.has_key?(acc, pid) do
              mref = Process.monitor(pid)
              Map.put(acc, pid, mref)
            else
              acc
            end
          end,
          monitors,
          pg_table
        )
      rescue
        ArgumentError -> monitors
      end

    %{state | monitors: monitors}
  end

  defp replay_local_journal(state) do
    state.name
    |> Data.local_replica_unapplied(state.shard_index)
    |> Enum.each(fn {stream_id, seq, mutations} ->
      if current_local_stream?(state, stream_id) do
        apply_registry_claim_mutations(state, stream_id, seq, mutations)
        Enum.each(mutations, &replay_local_mutation(state, &1))
      end

      :ok = Data.mark_local_replica_applied(state.name, state.shard_index, stream_id, seq)
    end)

    :ok =
      Data.prune_replica_oplog(
        state.name,
        state.shard_index,
        state.replicated_oplog_max_entries
      )

    state
  end

  defp rebuild_registry_projections(state) do
    claim_keys =
      state.name
      |> Data.reg_claim_by_key_table(state.shard_index)
      |> :ets.select([
        {{{:"$1", :"$2", :_, :_, :_}, :_, :_, :_, :_}, [], [{{:"$1", :"$2"}}]}
      ])

    visible_keys =
      state.name
      |> Data.reg_by_key_table(state.shard_index)
      |> :ets.select([
        {{{:"$1", :"$2"}, :_, :_, :_, :_}, [], [{{:"$1", :"$2"}}]}
      ])

    reconcile_registry_keys(
      state,
      Enum.uniq(claim_keys ++ visible_keys),
      :journal_replay,
      []
    )
  end

  defp current_local_stream?(state, stream_id) do
    cluster = WireProtocol.stream_cluster(stream_id)

    WireProtocol.stream_name(stream_id) == state.name and
      WireProtocol.stream_origin(stream_id) == node() and
      WireProtocol.stream_generation(stream_id) == Data.generation(state.name) and
      WireProtocol.stream_shard(stream_id) == state.shard_index and
      WireProtocol.stream_epoch(stream_id) == Data.local_cluster_epoch(state.name, cluster)
  end

  defp apply_registry_claim_mutations(state, stream_id, seq, mutations) do
    Enum.each(mutations, fn
      {:register, _cluster, key, pid, meta, time, _entry_node} ->
        Data.put_registry_claim(
          state.name,
          state.shard_index,
          stream_id,
          seq,
          key,
          pid,
          meta,
          time
        )

      {:unregister, _cluster, key, pid, _meta, _reason} ->
        Data.delete_registry_claim(state.name, state.shard_index, stream_id, seq, key, pid)

      _pg_mutation ->
        :ok
    end)

    :ok
  end

  defp replay_local_mutation(state, {:register, cluster, key, pid, meta, time, entry_node}) do
    Data.registry_insert(
      state.name,
      state.shard_index,
      cluster,
      key,
      pid,
      meta,
      time,
      entry_node
    )
  end

  defp replay_local_mutation(state, {:unregister, cluster, key, pid, meta, reason}) do
    Data.registry_delete_matching_many(
      state.name,
      state.shard_index,
      [{pid, cluster, key, meta, reason}]
    )

    :ok
  end

  defp replay_local_mutation(
         state,
         {:join, cluster, key, pid, meta, time, _reason, entry_node}
       ) do
    Data.pg_insert(
      state.name,
      state.shard_index,
      cluster,
      key,
      pid,
      meta,
      time,
      entry_node
    )
  end

  defp replay_local_mutation(state, {:leave, cluster, key, pid, meta, reason}) do
    Data.pg_delete_matching_many(
      state.name,
      state.shard_index,
      [{pid, cluster, key, meta, reason}]
    )

    :ok
  end

  defp cluster_member?(name, cluster) do
    node() in Data.cluster_nodes(name, cluster)
  end

  defp resolve_replicated_registry_conflict(
         state,
         cluster,
         key,
         {local_pid, local_meta, local_time},
         {remote_pid, remote_meta, remote_time},
         {initial, _current},
         entries,
         events,
         broadcasts,
         maybe_demonitor_pids
       ) do
    winner_pid =
      resolve_conflict_winner(
        state,
        cluster,
        key,
        {local_pid, local_meta, local_time},
        {remote_pid, remote_meta, remote_time}
      )

    entry = {cluster, key}

    cond do
      winner_pid == remote_pid ->
        exit_local_conflict_loser(local_pid, key, remote_meta)
        time = System.system_time()

        event =
          build_event(state.name, :unregistered, key, local_pid, local_meta, %{
            reason: :resolve_conflict,
            cluster: cluster
          })

        {
          Map.put(entries, entry, {initial, {remote_pid, remote_meta, time, node(remote_pid)}}),
          [event | events],
          [
            {:unregister, cluster, key, local_pid, local_meta, :resolve_conflict}
            | broadcasts
          ],
          MapSet.put(maybe_demonitor_pids, local_pid)
        }

      winner_pid == local_pid ->
        time = System.system_time()

        {
          Map.put(entries, entry, {initial, {local_pid, local_meta, time, node(local_pid)}}),
          events,
          [{:register, cluster, key, local_pid, local_meta, time, node(local_pid)} | broadcasts],
          maybe_demonitor_pids
        }

      true ->
        exit_local_conflict_loser(local_pid, key, nil)

        event =
          build_event(state.name, :unregistered, key, local_pid, local_meta, %{
            reason: :resolve_conflict,
            cluster: cluster
          })

        {
          Map.put(entries, entry, {initial, nil}),
          [event | events],
          [{:unregister, cluster, key, local_pid, local_meta, :resolve_conflict} | broadcasts],
          MapSet.put(maybe_demonitor_pids, local_pid)
        }
    end
  end

  defp reconcile_registry_projection(state, cluster, key, reason, events) do
    claims = Data.registry_claims(state.name, state.shard_index, cluster, key)
    state = remember_pending_registry_reprojections(state, cluster, key, claims)
    winner = select_registry_claim_winner(state, cluster, key, claims)

    {state, retirement} = retire_local_registry_losers(state, cluster, key, claims, winner)

    case retirement do
      :authority_changed ->
        reconcile_registry_projection(state, cluster, key, reason, events)

      retired? ->
        winner =
          if retired? do
            state.name
            |> Data.registry_claims(state.shard_index, cluster, key)
            |> then(&select_registry_claim_winner(state, cluster, key, &1))
          else
            winner
          end

        current = Data.registry_lookup(state.name, state.shard_index, cluster, key)
        projection_reason = if retired?, do: :resolve_conflict, else: reason

        project_registry_winner(
          state,
          cluster,
          key,
          current,
          winner,
          projection_reason,
          events
        )
    end
  end

  defp reconcile_registry_snapshot_batch(state, cluster, stream_id, claims, event_buffer) do
    source_node = WireProtocol.stream_origin(stream_id)
    generation = WireProtocol.stream_generation(stream_id)
    epoch = WireProtocol.stream_epoch(stream_id)
    claim_table = Data.reg_claim_by_key_table(state.name, state.shard_index)
    projection_table = Data.reg_by_key_table(state.name, state.shard_index)

    classified =
      Enum.map(claims, fn {key, _pid, _meta, _time} = claim ->
        claim_key = {cluster, key, source_node, generation, epoch}

        {
          claim,
          Data.registry_claim_uncontended_in_table?(claim_table, claim_key)
        }
      end)

    entries =
      Enum.map(claims, fn {key, pid, meta, time} ->
        {cluster, key, pid, meta, time, source_node}
      end)

    if Enum.all?(classified, &elem(&1, 1)) and
         Data.registry_insert_new_many(state.name, state.shard_index, entries) do
      event_buffer =
        Enum.reduce(claims, event_buffer, fn {key, pid, meta, _time}, buffer ->
          event = build_event(state.name, :registered, key, pid, meta, %{cluster: cluster})
          Snapshot.buffer_event(event, buffer)
        end)

      {state, event_buffer}
    else
      reconcile_registry_snapshot_batch_rows(
        state,
        cluster,
        source_node,
        projection_table,
        classified,
        event_buffer
      )
    end
  end

  defp reconcile_registry_snapshot_batch_rows(
         state,
         cluster,
         source_node,
         projection_table,
         classified,
         event_buffer
       ) do
    {state, event_buffer, inserts} =
      Enum.reduce(classified, {state, event_buffer, []}, fn
        {{key, pid, meta, time}, uncontended?}, {acc, buffer, inserts} ->
          current = Data.registry_lookup_in_table(projection_table, cluster, key)

          case {uncontended?, current} do
            {true, nil} ->
              event = build_event(acc.name, :registered, key, pid, meta, %{cluster: cluster})

              {
                acc,
                Snapshot.buffer_event(event, buffer),
                [{cluster, key, pid, meta, time, source_node} | inserts]
              }

            {true, {^pid, old_meta, old_time, ^source_node}} ->
              if old_meta == meta and old_time == time do
                {acc, buffer, inserts}
              else
                event =
                  build_event(acc.name, :registered, key, pid, meta, %{
                    previous_meta: old_meta,
                    cluster: cluster
                  })

                {
                  acc,
                  Snapshot.buffer_event(event, buffer),
                  [{cluster, key, pid, meta, time, source_node} | inserts]
                }
              end

            _ ->
              :ok =
                Data.registry_insert_many(acc.name, acc.shard_index, Enum.reverse(inserts))

              {acc, events} =
                reconcile_registry_projection(acc, cluster, key, :reconcile, [])

              {acc, Snapshot.buffer_events(Enum.reverse(events), buffer), []}
          end
      end)

    :ok = Data.registry_insert_many(state.name, state.shard_index, Enum.reverse(inserts))
    {state, event_buffer}
  end

  defp reconcile_registry_keys(state, keys, reason, events) do
    Enum.reduce(keys, {state, events}, fn {cluster, key}, {acc, inner_events} ->
      reconcile_registry_projection(acc, cluster, key, reason, inner_events)
    end)
  end

  defp remember_pending_registry_reprojections(state, _cluster, _key, []), do: state
  defp remember_pending_registry_reprojections(state, _cluster, _key, [_claim]), do: state

  defp remember_pending_registry_reprojections(state, cluster, key, claims) do
    Enum.reduce(claims, state, fn
      {_pid, _meta, _time, origin, generation, epoch, _seq}, acc when origin != node() ->
        local_cluster_active? =
          is_nil(cluster) or not is_nil(Data.local_cluster_epoch(acc.name, cluster))

        waiting_for_lane_view? =
          local_cluster_active? and generation == Data.remote_generation(acc.name, origin) and
            epoch == Data.remote_cluster_epoch(acc.name, origin, cluster) and
            not replica_view_current?(acc, origin)

        if waiting_for_lane_view? do
          pending =
            Map.update(
              acc.pending_registry_reprojections,
              origin,
              MapSet.new([{cluster, key}]),
              &MapSet.put(&1, {cluster, key})
            )

          %{acc | pending_registry_reprojections: pending}
        else
          acc
        end

      _local_claim, acc ->
        acc
    end)
  end

  defp reproject_pending_registry_keys(state, remote_node) do
    case Map.pop(state.pending_registry_reprojections, remote_node) do
      {nil, _pending} ->
        state

      {keys, pending} ->
        state = %{state | pending_registry_reprojections: pending}
        {state, events} = reconcile_registry_keys(state, keys, :reconcile, [])
        notify_monitors(state.name, events)
        state
    end
  end

  defp discard_pending_registry_reprojections(state, remote_node) do
    %{
      state
      | pending_registry_reprojections:
          Map.delete(state.pending_registry_reprojections, remote_node)
    }
  end

  defp select_registry_claim_winner(_state, _cluster, _key, []), do: nil
  defp select_registry_claim_winner(_state, _cluster, _key, [claim]), do: claim

  defp select_registry_claim_winner(%{name: name} = state, cluster, key, claims) do
    resolver = Map.get(Group.get_config(name), :resolve_registry_conflict)

    claims
    |> Enum.filter(&registry_claim_current?(state, cluster, &1))
    |> Enum.max_by(
      fn {pid, meta, time, _origin, _generation, _epoch, _seq} ->
        registry_conflict_order_key(name, key, {pid, meta, time}, resolver)
      end,
      fn -> nil end
    )
  end

  defp retire_local_registry_losers(state, cluster, key, claims, winner) do
    winner_pid = if winner, do: elem(winner, 0), else: nil

    local_losers =
      Enum.filter(claims, fn {pid, _meta, _time, origin_node, _generation, _epoch, _seq} ->
        origin_node == node() and pid != winner_pid
      end)
      |> Enum.filter(&registry_claim_current?(state, cluster, &1))

    cond do
      local_losers == [] ->
        {state, false}

      registry_winner_authoritative?(state, cluster, winner) ->
        state =
          Enum.reduce(local_losers, state, fn
            {pid, meta, _time, _origin_node, _generation, _epoch, _seq}, acc ->
              op = {:unregister, cluster, key, pid, meta, :resolve_conflict}
              record = append_local_replica_record(acc, op)
              acc = finish_local_replica_record(acc, record, :registry)
              winner_meta = if winner, do: elem(winner, 1), else: nil
              exit_local_conflict_loser(pid, key, winner_meta)
              acc
          end)

        {state, true}

      true ->
        # Authority changed after winner selection. Nothing irreversible has
        # happened yet; select again against the now-current authority.
        {state, :authority_changed}
    end
  end

  defp registry_claim_current?(
         state,
         cluster,
         {_pid, _meta, _time, origin, generation, epoch, _seq}
       ) do
    local_cluster_active? =
      is_nil(cluster) or not is_nil(Data.local_cluster_epoch(state.name, cluster))

    local_cluster_active? and
      if origin == node() do
        generation == Data.generation(state.name) and
          epoch == Data.local_cluster_epoch(state.name, cluster)
      else
        replica_view_current?(state, origin) and
          generation == Data.remote_generation(state.name, origin) and
          epoch == Data.remote_cluster_epoch(state.name, origin, cluster)
      end
  end

  defp registry_winner_authoritative?(_state, _cluster, nil), do: true

  defp registry_winner_authoritative?(
         _state,
         _cluster,
         {_pid, _meta, _time, origin, _generation, _epoch, _seq}
       )
       when origin == node(),
       do: true

  defp registry_winner_authoritative?(
         state,
         cluster,
         {_pid, _meta, _time, origin, generation, epoch, _seq}
       ) do
    Data.remote_registry_claim_authoritative?(
      state.name,
      state.shard_index,
      origin,
      generation,
      cluster,
      epoch
    )
  end

  defp project_registry_winner(state, _cluster, _key, nil, nil, _reason, events),
    do: {state, events}

  defp project_registry_winner(
         state,
         cluster,
         key,
         nil,
         {pid, meta, time, origin_node, _generation, _epoch, _seq},
         _reason,
         events
       ) do
    Data.registry_insert(
      state.name,
      state.shard_index,
      cluster,
      key,
      pid,
      meta,
      time,
      origin_node
    )

    event = build_event(state.name, :registered, key, pid, meta, %{cluster: cluster})
    {state, [event | events]}
  end

  defp project_registry_winner(
         state,
         cluster,
         key,
         {pid, old_meta, old_time, old_node},
         {pid, meta, time, origin_node, _generation, _epoch, _seq},
         _reason,
         events
       ) do
    if old_meta == meta and old_time == time and old_node == origin_node do
      {state, events}
    else
      Data.registry_insert(
        state.name,
        state.shard_index,
        cluster,
        key,
        pid,
        meta,
        time,
        origin_node
      )

      event =
        build_event(state.name, :registered, key, pid, meta, %{
          previous_meta: old_meta,
          cluster: cluster
        })

      {state, [event | events]}
    end
  end

  defp project_registry_winner(
         state,
         cluster,
         key,
         {old_pid, old_meta, _old_time, old_node},
         nil,
         reason,
         events
       ) do
    Data.registry_delete(state.name, state.shard_index, cluster, key, old_pid)

    state =
      if old_node == node() do
        maybe_demonitor_pid(state, state.name, state.shard_index, old_pid)
      else
        state
      end

    event =
      build_event(state.name, :unregistered, key, old_pid, old_meta, %{
        reason: reason,
        cluster: cluster
      })

    {state, [event | events]}
  end

  defp project_registry_winner(
         state,
         cluster,
         key,
         {old_pid, old_meta, _old_time, old_node},
         {pid, meta, time, origin_node, _generation, _epoch, _seq},
         reason,
         events
       ) do
    Data.registry_delete(state.name, state.shard_index, cluster, key, old_pid)

    state =
      if old_node == node() do
        maybe_demonitor_pid(state, state.name, state.shard_index, old_pid)
      else
        state
      end

    Data.registry_insert(
      state.name,
      state.shard_index,
      cluster,
      key,
      pid,
      meta,
      time,
      origin_node
    )

    unregistered =
      build_event(state.name, :unregistered, key, old_pid, old_meta, %{
        reason: reason,
        cluster: cluster
      })

    registered = build_event(state.name, :registered, key, pid, meta, %{cluster: cluster})
    {state, [registered, unregistered | events]}
  end

  defp resolve_conflict_winner(
         %{name: name},
         _cluster,
         key,
         {local_pid, local_meta, local_time},
         {remote_pid, remote_meta, remote_time}
       ) do
    config = Group.get_config(name)
    resolver = Map.get(config, :resolve_registry_conflict)

    local = {local_pid, local_meta, local_time}
    remote = {remote_pid, remote_meta, remote_time}

    winner =
      if registry_conflict_order_key(name, key, remote, resolver) >
           registry_conflict_order_key(name, key, local, resolver) do
        remote_pid
      else
        local_pid
      end

    if is_nil(resolver) do
      log_default_registry_conflict(key, local_pid, remote_pid, winner)
    end

    winner
  end

  defp registry_conflict_order_key(_name, _key, {pid, _meta, time}, nil), do: {time, pid}

  defp registry_conflict_order_key(name, key, {pid, _meta, _time} = claim, {
         mod,
         func,
         extra_args
       }) do
    {apply(mod, func, [name, key, claim | extra_args]), pid}
  end

  defp log_default_registry_conflict(key, pid1, pid2, winner_pid) do
    Logger.error(fn ->
      "#{inspect(__MODULE__)}: registry conflict detected: key=#{inspect(key)}, " <>
        "pid1=#{inspect(pid1)}, pid2=#{inspect(pid2)}, picking #{inspect(winner_pid)} as winner"
    end)
  end

  defp exit_local_conflict_loser(pid, key, winner_meta) when node(pid) == node() do
    Process.exit(pid, {:group_registry_conflict, key, winner_meta})
    :ok
  end

  defp exit_local_conflict_loser(_pid, _key, _winner_meta), do: :ok

  defp compute_shared_clusters(my_clusters, remote_clusters) do
    my_set = MapSet.new(my_clusters)
    remote_set = MapSet.new(remote_clusters)
    MapSet.intersection(my_set, remote_set) |> MapSet.to_list()
  end

  defp purge_cluster_entries(name, shard, cluster, target) do
    # Local disconnect uses :all to discard the complete replicated view.
    # Remote disconnects remove only data owned by the departing node.
    node_guard = if target == :all, do: [], else: [{:==, :"$5", target}]
    reg_table = Data.reg_by_key_table(name, shard)

    purged_reg =
      :ets.select(reg_table, [
        {{{cluster, :"$1"}, :"$2", :"$3", :"$4", :"$5"}, node_guard,
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)

    for {^cluster, key, pid, _meta, _time} <- purged_reg do
      Data.registry_delete(name, shard, cluster, key, pid)
    end

    pg_table = Data.pg_by_key_table(name, shard)

    purged_pg =
      :ets.select(pg_table, [
        {{{cluster, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, node_guard,
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)

    for {^cluster, key, pid, _meta, _time} <- purged_pg do
      Data.pg_delete(name, shard, cluster, key, pid)
    end

    {purged_reg, purged_pg}
  end

  defp build_purged_events(name, purged_reg, purged_pg, reason, events \\ []) do
    events =
      Enum.reduce(purged_reg, events, fn {cluster, key, pid, meta, _time}, acc ->
        [
          build_event(name, :unregistered, key, pid, meta, %{reason: reason, cluster: cluster})
          | acc
        ]
      end)

    Enum.reduce(purged_pg, events, fn {cluster, key, pid, meta, _time}, acc ->
      [build_event(name, :left, key, pid, meta, %{reason: reason, cluster: cluster}) | acc]
    end)
  end

  defp build_event(name, event_type, key, pid, meta, extra) do
    extract_fn = extract_meta_fn(name)

    %Group.Event{
      type: event_type,
      supervisor: name,
      cluster: Map.get(extra, :cluster),
      key: key,
      pid: pid,
      meta: extract_fn.(meta),
      previous_meta:
        case Map.get(extra, :previous_meta) do
          nil -> nil
          prev -> extract_fn.(prev)
        end,
      reason: Map.get(extra, :reason)
    }
  end

  defp extract_meta_fn(name) do
    case Group.get_config(name) do
      %{extract_meta: {mod, func, args}} -> fn meta -> apply(mod, func, [meta | args]) end
      %{extract_meta: func} when is_function(func, 1) -> func
      _ -> & &1
    end
  end

  # =====================================================================
  # Monitor notification
  # =====================================================================

  defp notify_monitors(_name, []), do: :ok

  defp notify_monitors(name, events) do
    {subscriber_events, _cache_by_cluster} =
      Enum.reduce(Enum.reverse(events), {%{}, %{}}, fn event, {acc, cache_by_cluster} ->
        cluster_cache = Map.get(cache_by_cluster, event.cluster, %{})

        {matching_pids, cluster_cache} =
          matching_subscribers(name, event.cluster, event.key, cluster_cache)

        acc =
          Enum.reduce(matching_pids, acc, fn sub_pid, inner ->
            Map.update(inner, sub_pid, [event], &[event | &1])
          end)

        {acc, Map.put(cache_by_cluster, event.cluster, cluster_cache)}
      end)

    for {sub_pid, sub_events} <- subscriber_events do
      send(sub_pid, {:group, Enum.reverse(sub_events), %{name: name}})
    end

    :ok
  end

  defp notify_snapshot_events(name, table) do
    {events, _count} =
      Snapshot.fold_events(table, {[], 0}, fn event, {events, count} ->
        events = [event | events]
        count = count + 1

        if count >= @snapshot_event_batch_size do
          notify_monitors(name, events)
          {[], 0}
        else
          {events, count}
        end
      end)

    notify_monitors(name, events)
  end

  defp matching_subscribers(name, cluster, key, cache) do
    {all_subscribers, cache} = get_cached_subscribers(name, cluster, :all, cache)
    {exact_subscribers, cache} = get_cached_subscribers(name, cluster, {:exact, key}, cache)

    subscriber_set =
      %{}
      |> put_subscribers(all_subscribers)
      |> put_subscribers(exact_subscribers)

    {subscriber_set, cache} =
      Enum.reduce(prefix_patterns_for_key(key), {subscriber_set, cache}, fn prefix,
                                                                            {acc, inner_cache} ->
        {prefix_subscribers, inner_cache} =
          get_cached_subscribers(name, cluster, {:prefix, prefix}, inner_cache)

        {put_subscribers(acc, prefix_subscribers), inner_cache}
      end)

    {Map.keys(subscriber_set), cache}
  end

  defp get_cached_subscribers(name, cluster, pattern, cache) do
    case cache do
      %{^pattern => subscribers} ->
        {subscribers, cache}

      %{} ->
        subscribers = lookup_subscribers(name, cluster, pattern)
        {subscribers, Map.put(cache, pattern, subscribers)}
    end
  end

  defp lookup_subscribers(name, cluster, pattern) do
    Group.registry_name(name)
    |> Registry.lookup({name, cluster, pattern})
    |> Enum.map(fn {pid, _value} -> pid end)
  rescue
    ArgumentError -> []
  end

  defp put_subscribers(acc, subscribers) do
    Enum.reduce(subscribers, acc, fn subscriber, inner -> Map.put(inner, subscriber, true) end)
  end

  defp prefix_patterns_for_key(key) do
    for {position, _length} <- :binary.matches(key, "/") do
      binary_part(key, 0, position + 1)
    end
  end

  # =====================================================================
  # Logging helpers
  # =====================================================================

  defp log(state, message_fn) when is_function(message_fn, 0) do
    case Group.get_config(state.name) do
      %{log: false} -> :ok
      _ -> Logger.info(message_fn)
    end
  end

  defp log_verbose(state, message_fn) when is_function(message_fn, 0) do
    case Group.get_config(state.name) do
      %{log: :verbose} -> Logger.info(message_fn)
      _ -> :ok
    end
  end

  defp log_once(state, message_fn) do
    if state.shard_index == 0, do: log(state, message_fn)
  end

  defp log_prefix(state) do
    "[Group #{inspect(state.name)}]"
  end

  defp log_prefix_shard(state) do
    "[Group #{inspect(state.name)}/#{state.shard_index}]"
  end
end
