defmodule GroupTest.ExtractMeta do
  def strip(meta), do: Map.take(meta, [:public])
end

defmodule GroupTest.ResolveRegistryConflict do
  def pick(
        _name,
        _key,
        {local_pid, _local_meta, _local_time},
        {remote_pid, _remote_meta, _remote_time},
        winner
      ) do
    case winner do
      :local -> local_pid
      :remote -> remote_pid
      :none -> :none
    end
  end
end

defmodule GroupTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  setup do
    name = :"test_group_#{System.unique_integer([:positive])}"
    start_supervised!({Group, name: name, shards: 4, log: false})
    {:ok, name: name}
  end

  describe "join/3 and leave/2" do
    test "joined process appears in members/2", %{name: name} do
      key = "chat/room/#{System.unique_integer([:positive])}"
      meta = %{role: :listener}

      :ok = Group.join(name, key, meta)

      members = Group.members(name, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, ^meta} = hd(members)
    end

    test "joined process triggers :joined event to subscribers", %{name: name} do
      key = "chat/room/#{System.unique_integer([:positive])}"

      # Subscribe first
      :ok = Group.monitor(name, key)

      # Spawn a process to join
      test_pid = self()

      spawn_pid =
        spawn(fn ->
          :ok = Group.join(name, key, %{role: :worker})
          send(test_pid, :joined)
          # Keep alive to avoid immediate :left event
          Process.sleep(:infinity)
        end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Spawned process didn't join in time")
      end

      # Should receive :joined event
      assert_receive {:group, [%Group.Event{type: :joined} = event], _}, 1000
      assert event.supervisor == name
      assert event.key == key
      assert event.pid == spawn_pid
      assert event.meta == %{role: :worker}
      assert event.previous_meta == nil
    end

    test "leave/2 removes from members and triggers :left event", %{name: name} do
      key = "chat/room/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.join(name, key, %{role: :listener})

      assert_receive {:group, [%Group.Event{type: :joined}], _}, 1000

      assert length(Group.members(name, key)) == 1

      :ok = Group.leave(name, key)

      # Should receive :left event
      assert_receive {:group, [%Group.Event{type: :left} = event], _}, 1000
      assert event.key == key
      assert event.pid == self()
      assert event.reason != nil

      assert Group.members(name, key) == []
    end

    test "process death triggers automatic :left event", %{name: name} do
      key = "chat/room/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.join(name, key, %{role: :temp})
          send(test_pid, :ready)

          receive do
            :exit -> :ok
          end
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      assert_receive {:group, [%Group.Event{type: :joined, pid: ^pid}], _}, 1000
      assert length(Group.members(name, key)) == 1

      # Kill the process
      Process.exit(pid, :kill)

      # Should receive :left event
      assert_receive {:group, [%Group.Event{type: :left} = event], _}, 1000
      assert event.pid == pid
      assert event.key == key

      # Should be removed from members
      assert Group.members(name, key) == []
    end

    test "leave/2 returns error when not a member", %{name: name} do
      key = "nonexistent/key"
      assert {:error, :not_in_group} = Group.leave(name, key)
    end

    test "re-join updates metadata in place", %{name: name} do
      key = "rejoin/test/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)

      # First join succeeds
      assert :ok = Group.join(name, key, %{v: 1})

      assert_receive {:group, [%Group.Event{type: :joined, previous_meta: nil, meta: %{v: 1}}],
                      _},
                     1000

      # Second join also succeeds and updates metadata
      assert :ok = Group.join(name, key, %{v: 2})

      # Should receive :joined event with previous_meta
      assert_receive {:group, [%Group.Event{type: :joined} = event], _}, 1000
      assert event.meta == %{v: 2}
      assert event.previous_meta == %{v: 1}

      # Metadata is updated
      [{_pid, %{v: 2}}] = Group.members(name, key)
    end
  end

  describe "register/unregister" do
    test "register makes process discoverable via lookup", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, key, %{module: :test})

      {pid, meta} = Group.lookup(name, key)
      assert pid == self()
      assert meta == %{module: :test}
    end

    test "register triggers :registered event", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.register(name, key, %{module: :test})

      assert_receive {:group, [%Group.Event{type: :registered} = event], _}, 1000
      assert event.key == key
      assert event.pid == self()
      assert event.meta == %{module: :test}
      assert event.previous_meta == nil
    end

    test "double register returns :taken", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, key, %{module: :test})

      # Another process tries to register same key
      test_pid = self()

      spawn(fn ->
        result = Group.register(name, key, %{module: :other})
        send(test_pid, {:register_result, result})
      end)

      assert_receive {:register_result, {:error, :taken}}, 1000
    end

    test "re-register by same process updates meta", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.register(name, key, %{v: 1})
      assert_receive {:group, [%Group.Event{type: :registered, previous_meta: nil}], _}, 1000

      :ok = Group.register(name, key, %{v: 2})

      assert_receive {:group,
                      [%Group.Event{type: :registered, meta: %{v: 2}, previous_meta: %{v: 1}}],
                      _},
                     1000

      {_pid, %{v: 2}} = Group.lookup(name, key)
    end

    test "unregister removes from lookup and fires event", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.register(name, key, %{module: :test})
      assert_receive {:group, [%Group.Event{type: :registered}], _}, 1000

      :ok = Group.unregister(name, key)
      assert_receive {:group, [%Group.Event{type: :unregistered} = event], _}, 1000
      assert event.key == key
      assert event.reason == :unregister

      assert Group.lookup(name, key) == nil
    end

    test "process death auto-unregisters", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, key, %{module: :test})
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("Process didn't register in time")
      end

      assert_receive {:group, [%Group.Event{type: :registered, pid: ^pid}], _}, 1000
      assert Group.lookup(name, key) != nil

      Process.exit(pid, :kill)

      assert_receive {:group, [%Group.Event{type: :unregistered, pid: ^pid}], _}, 1000
      assert Group.lookup(name, key) == nil
    end
  end

  describe "members/2" do
    test "returns only joined processes", %{name: name} do
      key = "only/joined/#{System.unique_integer([:positive])}"

      :ok = Group.join(name, key, %{role: :standalone})

      members = Group.members(name, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, %{role: :standalone}} = hd(members)
    end

    test "returns empty list for non-existent key", %{name: name} do
      assert Group.members(name, "nonexistent/key") == []
    end

    test "does not return registered processes", %{name: name} do
      key = "registered/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, key, %{type: :server})

      assert Group.members(name, key) == []
    end

    test "returns only joined processes when both registered and joined entries exist", %{
      name: name
    } do
      key = "both/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, key, %{type: :server})

      test_pid = self()

      joiner =
        spawn(fn ->
          :ok = Group.join(name, key, %{type: :client})
          send(test_pid, :joined)
          Process.sleep(:infinity)
        end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Process didn't join")
      end

      members = Group.members(name, key)
      assert [{^joiner, %{type: :client}}] = members
    end
  end

  describe "prefix members" do
    test "returns joined processes matching prefix", %{name: name} do
      prefix = "room/#{System.unique_integer([:positive])}/"
      key1 = prefix <> "a"
      key2 = prefix <> "b"
      other_key = "other/key"

      :ok = Group.join(name, key1, %{id: 1})
      :ok = Group.join(name, key2, %{id: 2})
      :ok = Group.join(name, other_key, %{id: 3})

      members = Group.members(name, prefix)
      assert length(members) == 2
      metas = Enum.map(members, fn {_pid, meta} -> meta end) |> Enum.sort_by(& &1.id)
      assert metas == [%{id: 1}, %{id: 2}]
    end

    test "does not return registered processes matching prefix", %{name: name} do
      prefix = "user/#{System.unique_integer([:positive])}/"
      key1 = prefix <> "alice"
      key2 = prefix <> "bob"
      other_key = "other/reg"

      test_pid = self()

      for {key, meta} <- [
            {key1, %{name: "alice"}},
            {key2, %{name: "bob"}},
            {other_key, %{name: "other"}}
          ] do
        pid =
          spawn(fn ->
            :ok = Group.register(name, key, meta)
            send(test_pid, {:registered, self()})
            Process.sleep(:infinity)
          end)

        receive do
          {:registered, ^pid} -> pid
        after
          1000 -> flunk("register timed out")
        end

        pid
      end

      assert Group.members(name, prefix) == []
    end

    test "returns only joined processes matching prefix", %{name: name} do
      prefix = "mixed/#{System.unique_integer([:positive])}/"
      reg_key = prefix <> "server"
      join_key = prefix <> "client"

      test_pid = self()

      reg_pid =
        spawn(fn ->
          :ok = Group.register(name, reg_key, %{type: :server})
          send(test_pid, {:registered, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:registered, ^reg_pid} -> :ok
      after
        1000 -> flunk("register timed out")
      end

      :ok = Group.join(name, join_key, %{type: :client})

      members = Group.members(name, prefix)
      assert [{_, %{type: :client}}] = members
    end

    test "returns empty list for prefix with no matches", %{name: name} do
      assert Group.members(name, "nonexistent/prefix/") == []
    end

    test "exact key lookup still works (no trailing slash)", %{name: name} do
      key = "exact/#{System.unique_integer([:positive])}"
      :ok = Group.join(name, key, %{exact: true})

      members = Group.members(name, key)
      assert length(members) == 1
    end

    test "prefix works with named clusters", %{name: name} do
      cluster = "game_#{System.unique_integer([:positive])}"
      prefix = "room/#{System.unique_integer([:positive])}/"

      :ok = Group.connect(name, cluster)

      # Join in named cluster
      :ok = Group.join(name, prefix <> "a", %{cluster: :named}, cluster: cluster)
      # Join in default cluster (same prefix)
      :ok = Group.join(name, prefix <> "b", %{cluster: :default})

      named_members = Group.members(name, prefix, cluster: cluster)
      assert length(named_members) == 1
      assert [{_, %{cluster: :named}}] = named_members

      default_members = Group.members(name, prefix)
      assert length(default_members) == 1
      assert [{_, %{cluster: :default}}] = default_members
    end

    test "prefix finds keys across different shards", %{name: name} do
      # Use enough keys that they're likely to hash to different shards (4 shards in test)
      prefix = "shard_spread/#{System.unique_integer([:positive])}/"

      for i <- 1..20 do
        :ok = Group.join(name, prefix <> "item_#{i}", %{i: i})
      end

      members = Group.members(name, prefix)
      assert length(members) == 20

      # Verify all items present
      found_ids = Enum.map(members, fn {_pid, meta} -> meta.i end) |> Enum.sort()
      assert found_ids == Enum.to_list(1..20)
    end

    test "register raises on key ending with /", %{name: name} do
      assert_raise ArgumentError, ~r/must not end with/, fn ->
        Group.register(name, "bad/key/", %{})
      end
    end

    test "join raises on key ending with /", %{name: name} do
      assert_raise ArgumentError, ~r/must not end with/, fn ->
        Group.join(name, "bad/key/", %{})
      end
    end

    test "unregister raises on key ending with /", %{name: name} do
      assert_raise ArgumentError, ~r/must not end with/, fn ->
        Group.unregister(name, "bad/key/")
      end
    end

    test "leave raises on key ending with /", %{name: name} do
      assert_raise ArgumentError, ~r/must not end with/, fn ->
        Group.leave(name, "bad/key/")
      end
    end
  end

  describe "self-events" do
    test "joining process receives its own :joined event if subscribed", %{name: name} do
      key = "self/events/#{System.unique_integer([:positive])}"

      # Subscribe first
      :ok = Group.monitor(name, key)

      # Then join
      :ok = Group.join(name, key, %{self: true})

      # Should receive our own :joined event
      assert_receive {:group, [%Group.Event{type: :joined} = event], _}, 1000
      assert event.pid == self()
      assert event.meta == %{self: true}
      assert event.previous_meta == nil
    end
  end

  describe "monitor/demonitor" do
    test "double subscribe is idempotent", %{name: name} do
      key = "user/test"

      assert :ok = Group.monitor(name, key)
      assert :ok = Group.monitor(name, key)

      # Spawn a process to join (use join, not start_child)
      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.join(name, key, %{role: :worker})
          send(test_pid, :joined)
          Process.sleep(:infinity)
        end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      # Should only receive one event (not duplicated)
      assert_receive {:group, [%Group.Event{type: :joined, pid: ^pid}], _}, 1000
      refute_receive {:group, _, _}, 100
    end

    test "overlapping subscriptions still deliver one event", %{name: name} do
      scope = "sprite_channels/#{System.unique_integer([:positive])}"
      key = "#{scope}/state"

      :ok = Group.monitor(name, :all)
      :ok = Group.monitor(name, key)
      :ok = Group.monitor(name, "sprite_channels/")
      :ok = Group.monitor(name, "#{scope}/")

      :ok = Group.join(name, key, %{v: 1})

      assert_receive {:group, [%Group.Event{type: :joined, key: ^key} = event], _}, 1000
      assert event.pid == self()
      refute_receive {:group, _, _}, 100
    end

    test "demonitor stops events", %{name: name} do
      key = "user/"

      :ok = Group.monitor(name, key)

      test_pid = self()

      spawn(fn ->
        :ok = Group.join(name, "user/first", %{})
        send(test_pid, :first_joined)
        Process.sleep(:infinity)
      end)

      receive do
        :first_joined -> :ok
      after
        1000 -> flunk("First process didn't join in time")
      end

      assert_receive {:group, [%Group.Event{type: :joined, key: "user/first"}], _}, 1000

      # Unsubscribe
      :ok = Group.demonitor(name, key)

      spawn(fn ->
        :ok = Group.join(name, "user/second", %{})
        send(test_pid, :second_joined)
        Process.sleep(:infinity)
      end)

      receive do
        :second_joined -> :ok
      after
        1000 -> flunk("Second process didn't join in time")
      end

      # Should NOT receive the second event
      refute_receive {:group, _, _}, 200
    end
  end

  describe "named clusters" do
    test "connect and disconnect reject non-binary cluster names", %{name: name} do
      cluster = "validated/#{System.unique_integer([:positive])}"
      key = "cluster-validation/#{System.unique_integer([:positive])}"
      :ok = Group.register(name, key, %{})

      for bad <- [nil, :default, 42] do
        assert_raise ArgumentError, ~r/cluster name must be a binary/, fn ->
          Group.connect(name, [bad])
        end

        assert_raise ArgumentError, ~r/cluster name must be a binary/, fn ->
          Group.disconnect(name, [bad])
        end
      end

      # The rejected disconnects must not have purged the default cluster view.
      assert Group.lookup(name, key) == {self(), %{}}

      assert :ok = Group.connect(name, cluster)
      assert Group.connected?(name, cluster)
      assert :ok = Group.disconnect(name, cluster)
      refute Group.connected?(name, cluster)
    end

    test "purging a node removes a forward-only membership row", %{name: name} do
      cluster = "orphaned/#{System.unique_integer([:positive])}"
      dead_node = :"dead_#{System.unique_integer([:positive])}@127.0.0.1"
      forward = Group.Replica.Data.cluster_nodes_table(name)
      reverse = Group.Replica.Data.node_clusters_table(name)

      :ok = Group.Replica.Data.add_cluster_node(name, cluster, dead_node)

      # Reproduce the one-sided state possible when add/remove/purge operations
      # on the two public ETS indexes interleave.
      :ets.delete_object(reverse, {dead_node, cluster})
      assert {cluster, dead_node} in :ets.lookup(forward, cluster)
      assert :ets.lookup(reverse, dead_node) == []

      :ok = Group.Replica.Data.purge_cluster_node(name, dead_node)

      refute {cluster, dead_node} in :ets.lookup(forward, cluster)
      assert :ets.lookup(reverse, dead_node) == []
    end

    test "concurrent membership mutations keep both indexes consistent", %{name: name} do
      cluster = "concurrent/#{System.unique_integer([:positive])}"
      remote_node = :"remote_#{System.unique_integer([:positive])}@127.0.0.1"
      forward = Group.Replica.Data.cluster_nodes_table(name)
      reverse = Group.Replica.Data.node_clusters_table(name)

      1..200
      |> Task.async_stream(
        fn i ->
          if rem(i, 2) == 0 do
            Group.Replica.Data.add_cluster_node(name, cluster, remote_node)
          else
            Group.Replica.Data.remove_cluster_node(name, cluster, remote_node)
          end
        end,
        max_concurrency: 20,
        ordered: false
      )
      |> Stream.run()

      forward? = {cluster, remote_node} in :ets.lookup(forward, cluster)
      reverse? = {remote_node, cluster} in :ets.lookup(reverse, remote_node)
      assert forward? == reverse?
    end

    test "replication queued after disconnect cannot repopulate the cluster" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 1,
          replicated_registry_receiver_buffer_size: 1
        )

      cluster = "disconnect/race/#{System.unique_integer([:positive])}"
      registry_key = "late/registry"
      pg_key = "late/pg"
      remote_pid = spawn_forever()
      shard = Group.Replica.shard_name(name, 0)

      on_exit(fn ->
        resume_shard_if_alive(shard)
        kill_if_alive(remote_pid)
      end)

      assert :ok = Group.connect(name, cluster)
      :ok = :sys.suspend(shard)

      disconnect_caller =
        spawn_requester(fn -> Group.disconnect(name, cluster) end, :disconnect_result)

      on_exit(fn -> kill_if_alive(disconnect_caller) end)

      wait_until(fn ->
        {:messages, messages} = Process.info(Process.whereis(shard), :messages)

        disconnect_queued? =
          Enum.any?(messages, fn
            {:group_local_request, _alias, {:cluster_disconnect, [^cluster]}} -> true
            {:group_local_request, _caller, _ref, {:cluster_disconnect, [^cluster]}} -> true
            _ -> false
          end)

        not Group.connected?(name, cluster) and disconnect_queued?
      end)

      send(shard, replicated_register(cluster, registry_key, remote_pid, %{}, :register))
      send(shard, replicated_pg_join(cluster, pg_key, remote_pid, %{}, :join))
      :ok = :sys.resume(shard)

      assert_receive {:disconnect_result, ^disconnect_caller, :ok}, 1_000
      :sys.get_state(shard)

      refute Group.connected?(name, cluster)
      assert Group.lookup(name, registry_key, cluster: cluster) == nil
      assert Group.members(name, pg_key, cluster: cluster) == []
    end

    test "connect/disconnect/connected? manage cluster lifecycle", %{name: name} do
      cluster = "game_servers"

      # Initially not connected
      refute Group.connected?(name, cluster)

      # Connect
      assert :ok = Group.connect(name, cluster)
      assert Group.connected?(name, cluster)

      # Disconnect
      assert :ok = Group.disconnect(name, cluster)
    end

    test "connect with ttl creates a lease only on first connect", %{name: name} do
      cluster = "ttl/#{System.unique_integer([:positive])}"

      assert :ok = Group.connect(name, cluster, ttl: 50)
      assert Group.connected?(name, cluster)
      assert {50, expires_at} = Group.Replica.Data.cluster_lease(name, cluster)
      assert is_integer(expires_at)

      assert :ok = Group.connect(name, cluster, ttl: 5_000)
      assert Group.Replica.Data.cluster_lease(name, cluster) == {50, expires_at}

      assert :ok = Group.disconnect(name, cluster)
      assert Group.Replica.Data.cluster_lease(name, cluster) == nil
    end

    test "plain connect does not create a ttl lease", %{name: name} do
      cluster = "plain/#{System.unique_integer([:positive])}"

      assert :ok = Group.connect(name, cluster)
      assert Group.connected?(name, cluster)
      assert Group.Replica.Data.cluster_lease(name, cluster) == nil
    end

    test "expired inactive ttl lease disconnects on sweep", %{name: name} do
      cluster = "inactive/#{System.unique_integer([:positive])}"

      assert :ok = Group.connect(name, cluster, ttl: 50)
      expire_cluster_lease(name, cluster)
      force_cluster_lease_sweep(name)

      refute Group.connected?(name, cluster)
      assert Group.Replica.Data.cluster_lease(name, cluster) == nil
    end

    test "expired ttl lease with local registry activity extends instead of disconnecting", %{
      name: name
    } do
      cluster = "registry/#{System.unique_integer([:positive])}"
      key = "players/#{System.unique_integer([:positive])}"

      assert :ok = Group.connect(name, cluster, ttl: 50)
      assert :ok = Group.register(name, key, %{kind: :registry}, cluster: cluster)

      expire_cluster_lease(name, cluster)
      force_cluster_lease_sweep(name)

      assert Group.connected?(name, cluster)
      assert {50, expires_at} = Group.Replica.Data.cluster_lease(name, cluster)
      assert expires_at > System.monotonic_time(:millisecond)

      assert :ok = Group.unregister(name, key, cluster: cluster)
      expire_cluster_lease(name, cluster)
      force_cluster_lease_sweep(name)

      refute Group.connected?(name, cluster)
    end

    test "expired ttl lease with local pg activity extends instead of disconnecting", %{
      name: name
    } do
      cluster = "pg/#{System.unique_integer([:positive])}"
      key = "rooms/#{System.unique_integer([:positive])}"

      assert :ok = Group.connect(name, cluster, ttl: 50)
      assert :ok = Group.join(name, key, %{kind: :pg}, cluster: cluster)

      expire_cluster_lease(name, cluster)
      force_cluster_lease_sweep(name)

      assert Group.connected?(name, cluster)
      assert {50, expires_at} = Group.Replica.Data.cluster_lease(name, cluster)
      assert expires_at > System.monotonic_time(:millisecond)

      assert :ok = Group.leave(name, key, cluster: cluster)
      expire_cluster_lease(name, cluster)
      force_cluster_lease_sweep(name)

      refute Group.connected?(name, cluster)
    end

    test "expired ttl lease with cluster-scoped monitor activity extends instead of disconnecting",
         %{name: name} do
      cluster = "monitor/#{System.unique_integer([:positive])}"
      key = "watch/#{System.unique_integer([:positive])}"

      assert :ok = Group.connect(name, cluster, ttl: 50)
      assert :ok = Group.monitor(name, key, cluster: cluster)

      expire_cluster_lease(name, cluster)
      force_cluster_lease_sweep(name)

      assert Group.connected?(name, cluster)
      assert {50, expires_at} = Group.Replica.Data.cluster_lease(name, cluster)
      assert expires_at > System.monotonic_time(:millisecond)

      assert :ok = Group.demonitor(name, key, cluster: cluster)
      expire_cluster_lease(name, cluster)
      force_cluster_lease_sweep(name)

      refute Group.connected?(name, cluster)
    end

    test "lease manager restart preserves leases and resumes sweeping", %{name: name} do
      cluster = "restart/#{System.unique_integer([:positive])}"
      lease_manager = Group.ClusterLease.lease_name(name)
      manager_pid = Process.whereis(lease_manager)

      assert :ok = Group.connect(name, cluster, ttl: 50)
      assert {50, _expires_at} = Group.Replica.Data.cluster_lease(name, cluster)

      ref = Process.monitor(manager_pid)
      :ok = GenServer.stop(manager_pid, :shutdown)
      assert_receive {:DOWN, ^ref, :process, ^manager_pid, :shutdown}, 1000

      wait_until(fn ->
        case Process.whereis(lease_manager) do
          nil -> false
          new_pid -> new_pid != manager_pid
        end
      end)

      expire_cluster_lease(name, cluster)
      force_cluster_lease_sweep(name)

      refute Group.connected?(name, cluster)
      assert Group.Replica.Data.cluster_lease(name, cluster) == nil
    end

    test "join/leave work with cluster: option", %{name: name} do
      cluster = "game_cluster"
      key = "room/#{System.unique_integer([:positive])}"

      # Connect to the cluster first
      :ok = Group.connect(name, cluster)

      # Join in the named cluster
      :ok = Group.join(name, key, %{role: :player}, cluster: cluster)

      # Should appear in named cluster members
      members = Group.members(name, key, cluster: cluster)
      assert length(members) == 1
      my_pid = self()
      assert [{^my_pid, %{role: :player}}] = members

      # Should NOT appear in default cluster members
      assert Group.members(name, key) == []

      # Leave the named cluster
      :ok = Group.leave(name, key, cluster: cluster)
      assert Group.members(name, key, cluster: cluster) == []
    end

    test "events in one cluster don't leak to another", %{name: name} do
      cluster1 = "cluster_a"
      cluster2 = "cluster_b"
      key = "shared/key/#{System.unique_integer([:positive])}"

      # Connect to both clusters
      :ok = Group.connect(name, cluster1)
      :ok = Group.connect(name, cluster2)

      # Subscribe to cluster1 only
      :ok = Group.monitor(name, :all, cluster: cluster1)

      # Spawn process to join cluster1
      test_pid = self()

      pid1 =
        spawn(fn ->
          :ok = Group.join(name, key, %{cluster: 1}, cluster: cluster1)
          send(test_pid, {:joined, 1})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, 1} -> :ok
      after
        1000 -> flunk("Process didn't join cluster1 in time")
      end

      # Should receive event from cluster1
      assert_receive {:group, [%Group.Event{type: :joined, pid: ^pid1, cluster: ^cluster1}], _},
                     1000

      # Spawn process to join cluster2
      _pid2 =
        spawn(fn ->
          :ok = Group.join(name, key, %{cluster: 2}, cluster: cluster2)
          send(test_pid, {:joined, 2})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, 2} -> :ok
      after
        1000 -> flunk("Process didn't join cluster2 in time")
      end

      # Should NOT receive event from cluster2
      refute_receive {:group, _, _}, 200

      # Now subscribe to cluster2 and verify we can receive events
      :ok = Group.monitor(name, :all, cluster: cluster2)

      pid3 =
        spawn(fn ->
          :ok = Group.join(name, key, %{cluster: 2, extra: true}, cluster: cluster2)
          send(test_pid, {:joined, 3})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, 3} -> :ok
      after
        1000 -> flunk("Process didn't join cluster2 in time")
      end

      assert_receive {:group, [%Group.Event{type: :joined, pid: ^pid3, cluster: ^cluster2}], _},
                     1000
    end

    test "members/3 returns only members from specified cluster", %{name: name} do
      cluster = "isolated_cluster"
      key = "room/#{System.unique_integer([:positive])}"

      :ok = Group.connect(name, cluster)

      # Join default cluster
      :ok = Group.join(name, key, %{location: :default})

      # Join named cluster (need different process since same pid can't join same key twice)
      test_pid = self()

      other_pid =
        spawn(fn ->
          :ok = Group.join(name, key, %{location: :named}, cluster: cluster)
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      # Default cluster should only have our process
      default_members = Group.members(name, key)
      assert length(default_members) == 1
      my_pid = self()
      assert [{^my_pid, %{location: :default}}] = default_members

      # Named cluster should only have the spawned process
      named_members = Group.members(name, key, cluster: cluster)
      assert length(named_members) == 1
      assert [{^other_pid, %{location: :named}}] = named_members
    end

    test "default cluster works without cluster: option", %{name: name} do
      key = "default/test/#{System.unique_integer([:positive])}"

      # Subscribe without cluster option (default cluster)
      :ok = Group.monitor(name, key)

      # Join without cluster option (default cluster)
      :ok = Group.join(name, key, %{v: 1})

      # Should receive event with cluster: nil
      assert_receive {:group, [%Group.Event{type: :joined} = event], _}, 1000
      assert event.cluster == nil
      assert event.meta == %{v: 1}
      assert event.previous_meta == nil

      # Members without cluster option
      members = Group.members(name, key)
      assert length(members) == 1
    end

    test "dispatch works with cluster: option", %{name: name} do
      cluster = "broadcast_cluster"
      key = "broadcast/#{System.unique_integer([:positive])}"

      :ok = Group.connect(name, cluster)

      # Join the named cluster
      :ok = Group.join(name, key, %{}, cluster: cluster)

      # Broadcast to named cluster
      :ok = Group.dispatch(name, key, {:test_message, :from_cluster}, cluster: cluster)

      assert_receive {:test_message, :from_cluster}, 1000

      # Broadcast to default cluster (we're not there)
      :ok = Group.dispatch(name, key, {:test_message, :from_default})

      # Should NOT receive (we're not in default cluster for this key)
      refute_receive {:test_message, :from_default}, 200
    end

    test "dispatch_local only sends to local members", %{name: name} do
      key = "dispatch_local/#{System.unique_integer([:positive])}"

      # Join from self (local)
      :ok = Group.join(name, key, %{})

      :ok = Group.dispatch_local(name, key, {:local_msg, 1})
      assert_receive {:local_msg, 1}, 1000

      # Also works with cluster: option
      cluster = "dispatch_local_cluster"
      :ok = Group.connect(name, cluster)
      :ok = Group.join(name, key, %{}, cluster: cluster)

      :ok = Group.dispatch_local(name, key, {:local_msg, 2}, cluster: cluster)
      assert_receive {:local_msg, 2}, 1000

      # Default cluster dispatch_local should not deliver cluster message
      :ok = Group.dispatch_local(name, key, {:local_msg, 3})
      assert_receive {:local_msg, 3}, 1000
      refute_receive {:local_msg, _}, 200
    end

    test "monitor/demonitor work with cluster: option", %{name: name} do
      cluster = "sub_cluster"
      key = "sub/test/#{System.unique_integer([:positive])}"

      :ok = Group.connect(name, cluster)

      # Subscribe to named cluster
      :ok = Group.monitor(name, key, cluster: cluster)

      # Spawn and join
      test_pid = self()

      spawn(fn ->
        :ok = Group.join(name, key, %{}, cluster: cluster)
        send(test_pid, :joined)
        Process.sleep(:infinity)
      end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      assert_receive {:group, [%Group.Event{type: :joined, cluster: ^cluster}], _}, 1000

      # Unsubscribe from named cluster
      :ok = Group.demonitor(name, key, cluster: cluster)

      # Spawn another process to join
      spawn(fn ->
        :ok = Group.join(name, key, %{second: true}, cluster: cluster)
        send(test_pid, :joined2)
        Process.sleep(:infinity)
      end)

      receive do
        :joined2 -> :ok
      after
        1000 -> flunk("Second process didn't join in time")
      end

      # Should NOT receive event after unsubscribe
      refute_receive {:group, _, _}, 200
    end
  end

  describe "call timeout option" do
    test "register honors timeout option" do
      name = start_single_shard_group()
      key = "timeout/register/#{System.unique_integer([:positive])}"
      shard = suspend_only_shard(name)

      try do
        assert_genserver_call_timeout(fn ->
          Group.register(name, key, %{}, timeout: 10)
        end)
      after
        resume_shard_if_alive(shard)
      end
    end

    test "register timeout does not leak late local reply into caller mailbox" do
      name = start_single_shard_group()
      key = "timeout/register/leak/#{System.unique_integer([:positive])}"
      shard = suspend_only_shard(name)

      try do
        assert_genserver_call_timeout(fn ->
          Group.register(name, key, %{}, timeout: 10)
        end)
      after
        resume_shard_if_alive(shard)
      end

      wait_until(fn ->
        match?({pid, %{}} when pid == self(), Group.lookup(name, key))
      end)

      refute_receive {:group_local_reply, _, _}, 50

      assert :ok = Group.unregister(name, key)
    end

    test "unregister honors timeout option" do
      name = start_single_shard_group()
      key = "timeout/unregister/#{System.unique_integer([:positive])}"
      :ok = Group.register(name, key, %{})
      shard = suspend_only_shard(name)

      try do
        assert_genserver_call_timeout(fn ->
          Group.unregister(name, key, timeout: 10)
        end)
      after
        resume_shard_if_alive(shard)
      end
    end

    test "join honors timeout option" do
      name = start_single_shard_group()
      key = "timeout/join/#{System.unique_integer([:positive])}"
      shard = suspend_only_shard(name)

      try do
        assert_genserver_call_timeout(fn ->
          Group.join(name, key, %{}, timeout: 10)
        end)
      after
        resume_shard_if_alive(shard)
      end
    end

    test "leave honors timeout option" do
      name = start_single_shard_group()
      key = "timeout/leave/#{System.unique_integer([:positive])}"
      :ok = Group.join(name, key, %{})
      shard = suspend_only_shard(name)

      try do
        assert_genserver_call_timeout(fn ->
          Group.leave(name, key, timeout: 10)
        end)
      after
        resume_shard_if_alive(shard)
      end
    end

    test "connect honors timeout option" do
      name = start_single_shard_group()
      shard = suspend_only_shard(name)

      try do
        assert_genserver_call_timeout(fn ->
          Group.connect(name, "timeout_cluster", timeout: 10)
        end)
      after
        resume_shard_if_alive(shard)
      end
    end

    test "disconnect honors timeout option" do
      name = start_single_shard_group()
      :ok = Group.connect(name, "timeout_cluster")
      shard = suspend_only_shard(name)

      try do
        assert_genserver_call_timeout(fn ->
          Group.disconnect(name, "timeout_cluster", timeout: 10)
        end)
      after
        resume_shard_if_alive(shard)
      end
    end
  end

  describe "local request fairness" do
    test "local register gets a turn ahead of replicated registry backlog" do
      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 1,
          replicated_registry_receiver_flush_interval: 60_000
        )

      shard = suspend_only_shard(name)
      remote_pid = spawn_forever()
      local_key = "fair/register/local/#{System.unique_integer([:positive])}"
      backlog_prefix = "fair/register/backlog/#{System.unique_integer([:positive])}"

      on_exit(fn ->
        kill_if_alive(remote_pid)
      end)

      enqueue_replicated_registry_backlog(shard, backlog_prefix, remote_pid, 1_000)

      caller =
        spawn_requester(
          fn ->
            Group.register(name, local_key, %{local: true})
          end,
          :local_register_result
        )

      on_exit(fn -> kill_if_alive(caller) end)
      Process.sleep(20)
      :ok = :sys.resume(shard)

      assert_receive {:local_register_result, ^caller, :ok}, 1_000
      assert shard_message_queue_len(shard) > 0
      assert Group.lookup(name, local_key) == {caller, %{local: true}}
      wait_until(fn -> shard_message_queue_len(shard) == 0 end, 5_000)
    end

    test "local join gets a turn ahead of replicated registry backlog" do
      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 1,
          replicated_registry_receiver_flush_interval: 60_000
        )

      shard = suspend_only_shard(name)
      remote_pid = spawn_forever()
      local_key = "fair/join-registry/local/#{System.unique_integer([:positive])}"
      backlog_prefix = "fair/join-registry/backlog/#{System.unique_integer([:positive])}"

      on_exit(fn ->
        kill_if_alive(remote_pid)
      end)

      enqueue_replicated_registry_backlog(shard, backlog_prefix, remote_pid, 1_000)

      caller =
        spawn_requester(
          fn ->
            Group.join(name, local_key, %{local: true})
          end,
          :local_join_result
        )

      on_exit(fn -> kill_if_alive(caller) end)
      Process.sleep(20)
      :ok = :sys.resume(shard)

      assert_receive {:local_join_result, ^caller, :ok}, 1_000
      assert shard_message_queue_len(shard) > 0
      assert Group.members(name, local_key) == [{caller, %{local: true}}]
      wait_until(fn -> shard_message_queue_len(shard) == 0 end, 5_000)
    end

    test "local join gets a turn ahead of replicated PG backlog" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 1,
          replicated_pg_receiver_flush_interval: 60_000
        )

      shard = suspend_only_shard(name)
      remote_pid = spawn_forever()
      local_key = "fair/join/local/#{System.unique_integer([:positive])}"
      backlog_prefix = "fair/join/backlog/#{System.unique_integer([:positive])}"

      on_exit(fn ->
        kill_if_alive(remote_pid)
      end)

      enqueue_replicated_pg_backlog(shard, backlog_prefix, remote_pid, 1_000)

      caller =
        spawn_requester(
          fn ->
            Group.join(name, local_key, %{local: true})
          end,
          :local_join_result
        )

      on_exit(fn -> kill_if_alive(caller) end)
      Process.sleep(20)
      :ok = :sys.resume(shard)

      assert_receive {:local_join_result, ^caller, :ok}, 1_000
      assert shard_message_queue_len(shard) > 0
      assert Group.members(name, local_key) == [{caller, %{local: true}}]
      wait_until(fn -> shard_message_queue_len(shard) == 0 end, 5_000)
    end

    test "local connect and disconnect each get a turn ahead of replicated PG backlog" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 1,
          replicated_pg_receiver_flush_interval: 60_000
        )

      shard = suspend_only_shard(name)
      remote_pid = spawn_forever()
      cluster = "fair/connect/#{System.unique_integer([:positive])}"
      connect_prefix = "fair/connect/backlog/#{System.unique_integer([:positive])}"

      on_exit(fn ->
        kill_if_alive(remote_pid)
      end)

      enqueue_replicated_pg_backlog(shard, connect_prefix, remote_pid, 1_000)

      connect_caller =
        spawn_requester(
          fn ->
            Group.connect(name, cluster)
          end,
          :local_connect_result
        )

      on_exit(fn -> kill_if_alive(connect_caller) end)
      Process.sleep(20)
      :ok = :sys.resume(shard)

      assert_receive {:local_connect_result, ^connect_caller, :ok}, 1_000
      assert shard_message_queue_len(shard) > 0
      assert Group.connected?(name, cluster)
      wait_until(fn -> shard_message_queue_len(shard) == 0 end, 5_000)

      :ok = :sys.suspend(shard)

      disconnect_prefix = "fair/disconnect/backlog/#{System.unique_integer([:positive])}"

      enqueue_replicated_pg_backlog(shard, disconnect_prefix, remote_pid, 1_000)

      disconnect_caller =
        spawn_requester(
          fn ->
            Group.disconnect(name, cluster)
          end,
          :local_disconnect_result
        )

      on_exit(fn -> kill_if_alive(disconnect_caller) end)
      Process.sleep(20)
      :ok = :sys.resume(shard)

      assert_receive {:local_disconnect_result, ^disconnect_caller, :ok}, 1_000
      assert shard_message_queue_len(shard) > 0
      refute Group.connected?(name, cluster)
      wait_until(fn -> shard_message_queue_len(shard) == 0 end, 5_000)
    end

    test "fairness preserves FIFO within the local request lane" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 1,
          replicated_pg_receiver_flush_interval: 60_000
        )

      shard = suspend_only_shard(name)
      remote_pid = spawn_forever()
      backlog_prefix = "fair/fifo/backlog/#{System.unique_integer([:positive])}"
      key1 = "fair/fifo/local/#{System.unique_integer([:positive])}/1"
      key2 = "fair/fifo/local/#{System.unique_integer([:positive])}/2"

      on_exit(fn ->
        kill_if_alive(remote_pid)
      end)

      enqueue_replicated_pg_backlog(shard, backlog_prefix, remote_pid, 1_000)

      caller1 =
        spawn_requester(
          fn ->
            Group.join(name, key1, %{order: 1})
          end,
          :fifo_result
        )

      Process.sleep(20)

      caller2 =
        spawn_requester(
          fn ->
            Group.join(name, key2, %{order: 2})
          end,
          :fifo_result
        )

      on_exit(fn ->
        kill_if_alive(caller1)
        kill_if_alive(caller2)
      end)

      Process.sleep(20)
      :ok = :sys.resume(shard)

      assert_receive {:fifo_result, ^caller1, :ok}, 1_000
      assert_receive {:fifo_result, ^caller2, :ok}, 1_000
      assert shard_message_queue_len(shard) > 0
      assert Group.members(name, key1) == [{caller1, %{order: 1}}]
      assert Group.members(name, key2) == [{caller2, %{order: 2}}]
      wait_until(fn -> shard_message_queue_len(shard) == 0 end, 5_000)
    end

    test "configurable local fairness quota drains multiple local requests before older non-local work" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 1,
          replicated_pg_receiver_flush_interval: 60_000,
          replicated_pg_receiver_local_request_quota: 2
        )

      shard = suspend_only_shard(name)
      remote_pid = spawn_forever()
      remote_key1 = "fair/quota/remote/#{System.unique_integer([:positive])}/1"
      remote_key2 = "fair/quota/remote/#{System.unique_integer([:positive])}/2"
      local_key1 = "fair/quota/local/#{System.unique_integer([:positive])}/1"
      local_key2 = "fair/quota/local/#{System.unique_integer([:positive])}/2"
      local_key3 = "fair/quota/local/#{System.unique_integer([:positive])}/3"

      on_exit(fn ->
        kill_if_alive(remote_pid)
      end)

      send(shard, replicated_pg_join(nil, remote_key1, remote_pid, %{remote: 1}, :join))
      send(shard, replicated_pg_join(nil, remote_key2, remote_pid, %{remote: 2}, :join))
      send(shard, {:group_dispatch, [self()], {:quota_marker, shard}})

      caller1 =
        spawn_requester(
          fn ->
            Group.join(name, local_key1, %{order: 1})
          end,
          :quota_result
        )

      caller2 =
        spawn_requester(
          fn ->
            Group.join(name, local_key2, %{order: 2})
          end,
          :quota_result
        )

      caller3 =
        spawn_requester(
          fn ->
            Group.join(name, local_key3, %{order: 3})
          end,
          :quota_result
        )

      on_exit(fn ->
        kill_if_alive(caller1)
        kill_if_alive(caller2)
        kill_if_alive(caller3)
      end)

      wait_until(fn -> shard_message_queue_len(shard) >= 6 end, 1_000)
      :ok = :sys.resume(shard)

      assert_receive {:quota_result, ^caller1, :ok}, 1_000
      assert_receive {:quota_result, ^caller2, :ok}, 1_000
      assert_receive {:quota_result, ^caller3, :ok}, 1_000
      assert_receive {:quota_marker, ^shard}, 1_000

      assert Group.members(name, remote_key1) == [{remote_pid, %{remote: 1}}]
      assert Group.members(name, remote_key2) == [{remote_pid, %{remote: 2}}]
      assert Group.members(name, local_key1) == [{caller1, %{order: 1}}]
      assert Group.members(name, local_key2) == [{caller2, %{order: 2}}]
      assert Group.members(name, local_key3) == [{caller3, %{order: 3}}]
    end

    test "local PG batching applies mixed join and leave requests correctly" do
      name = start_single_shard_group(replicated_pg_receiver_local_request_quota: 3)

      existing_key = "fair/pg-batch/existing/#{System.unique_integer([:positive])}"
      join_key1 = "fair/pg-batch/join/#{System.unique_integer([:positive])}/1"
      join_key2 = "fair/pg-batch/join/#{System.unique_integer([:positive])}/2"
      parent = self()

      leaver =
        spawn(fn ->
          :ok = Group.join(name, existing_key, %{existing: true})
          send(parent, {:pg_batch_ready, self()})

          receive do
            :leave ->
              result = Group.leave(name, existing_key)
              send(parent, {:pg_batch_result, self(), result})
              Process.sleep(:infinity)
          end
        end)

      on_exit(fn ->
        kill_if_alive(leaver)
      end)

      assert_receive {:pg_batch_ready, ^leaver}, 1_000
      assert Group.members(name, existing_key) == [{leaver, %{existing: true}}]

      shard = suspend_only_shard(name)
      send(leaver, :leave)

      caller1 =
        spawn_requester(
          fn ->
            Group.join(name, join_key1, %{order: 1})
          end,
          :pg_batch_result
        )

      caller2 =
        spawn_requester(
          fn ->
            Group.join(name, join_key2, %{order: 2})
          end,
          :pg_batch_result
        )

      on_exit(fn ->
        kill_if_alive(caller1)
        kill_if_alive(caller2)
      end)

      wait_until(fn -> shard_message_queue_len(shard) >= 3 end, 1_000)
      :ok = :sys.resume(shard)

      assert_receive {:pg_batch_result, ^leaver, :ok}, 1_000
      assert_receive {:pg_batch_result, ^caller1, :ok}, 1_000
      assert_receive {:pg_batch_result, ^caller2, :ok}, 1_000

      assert Group.members(name, existing_key) == []
      assert Group.members(name, join_key1) == [{caller1, %{order: 1}}]
      assert Group.members(name, join_key2) == [{caller2, %{order: 2}}]
    end
  end

  describe "ETS table consistency" do
    test "tables are consistent after register + unregister", %{name: name} do
      :ok = Group.register(name, "ets/reg1", %{v: 1})
      :ok = Group.register(name, "ets/reg2", %{v: 2})
      :ok = Group.unregister(name, "ets/reg1")

      assert Group.TestCluster.assert_ets_consistent(name) == :ok
    end

    test "tables are consistent after join + leave", %{name: name} do
      :ok = Group.join(name, "ets/grp1", %{v: 1})
      :ok = Group.join(name, "ets/grp2", %{v: 2})
      :ok = Group.leave(name, "ets/grp1")

      assert Group.TestCluster.assert_ets_consistent(name) == :ok
    end

    test "tables are consistent after process death cleans up", %{name: name} do
      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, "ets/death_reg", %{})
          :ok = Group.join(name, "ets/death_grp", %{})
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("timeout")
      end

      # Verify entries exist
      assert Group.lookup(name, "ets/death_reg") != nil
      assert length(Group.members(name, "ets/death_grp")) == 1

      # Kill and wait for cleanup
      Process.exit(pid, :kill)
      Process.sleep(100)

      assert Group.lookup(name, "ets/death_reg") == nil
      assert Group.members(name, "ets/death_grp") == []

      assert Group.TestCluster.assert_ets_consistent(name) == :ok
    end

    test "tables are empty after all processes die", %{name: name} do
      test_pid = self()

      pids =
        for i <- 1..10 do
          spawn(fn ->
            :ok = Group.register(name, "ets/clean_reg_#{i}", %{i: i})
            :ok = Group.join(name, "ets/clean_grp", %{i: i})
            send(test_pid, {:ready, self()})
            Process.sleep(:infinity)
          end)
        end

      for pid <- pids do
        receive do
          {:ready, ^pid} -> :ok
        after
          1000 -> flunk("timeout")
        end
      end

      # Kill all
      Enum.each(pids, &Process.exit(&1, :kill))
      Process.sleep(200)

      # Tables should be consistent and empty
      assert Group.TestCluster.assert_ets_consistent(name) == :ok

      num_shards = Group.get_config(name).num_shards

      total_reg =
        Enum.sum(
          for s <- 0..(num_shards - 1) do
            :ets.info(Group.Replica.Data.reg_by_key_table(name, s), :size)
          end
        )

      total_reg_pid =
        Enum.sum(
          for s <- 0..(num_shards - 1) do
            :ets.info(Group.Replica.Data.reg_by_pid_table(name, s), :size)
          end
        )

      total_pg =
        Enum.sum(
          for s <- 0..(num_shards - 1) do
            :ets.info(Group.Replica.Data.pg_by_key_table(name, s), :size)
          end
        )

      total_pg_pid =
        Enum.sum(
          for s <- 0..(num_shards - 1) do
            :ets.info(Group.Replica.Data.pg_by_pid_table(name, s), :size)
          end
        )

      assert total_reg == 0, "reg_by_key has #{total_reg} orphaned entries"
      assert total_reg_pid == 0, "reg_by_pid has #{total_reg_pid} orphaned entries"
      assert total_pg == 0, "pg_by_key has #{total_pg} orphaned entries"
      assert total_pg_pid == 0, "pg_by_pid has #{total_pg_pid} orphaned entries"
    end

    test "tables are consistent after cluster disconnect", %{name: name} do
      cluster = "ets_cleanup_cluster"
      :ok = Group.connect(name, cluster)

      test_pid = self()

      spawn(fn ->
        :ok = Group.register(name, "ets/cluster_reg", %{}, cluster: cluster)
        :ok = Group.join(name, "ets/cluster_grp", %{}, cluster: cluster)
        send(test_pid, :ready)
        Process.sleep(:infinity)
      end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("timeout")
      end

      assert Group.lookup(name, "ets/cluster_reg", cluster: cluster) != nil

      :ok = Group.disconnect(name, cluster)

      # Cluster entries should be purged, tables consistent
      assert Group.TestCluster.assert_ets_consistent(name) == :ok
    end
  end

  describe "local_registry_count/1" do
    test "counts registered processes", %{name: name} do
      assert Group.local_registry_count(name) == 0

      :ok = Group.register(name, "key1", %{})
      assert Group.local_registry_count(name) == 1

      test_pid = self()

      spawn(fn ->
        :ok = Group.register(name, "key2", %{})
        send(test_pid, :registered)
        Process.sleep(:infinity)
      end)

      receive do
        :registered -> :ok
      after
        1000 -> flunk("didn't register")
      end

      assert Group.local_registry_count(name) == 2
    end
  end

  describe "local_member_count/2" do
    test "counts local group members", %{name: name} do
      group = "my_group"
      assert Group.local_member_count(name, group) == 0

      :ok = Group.join(name, group, %{})
      assert Group.local_member_count(name, group) == 1

      test_pid = self()

      spawn(fn ->
        :ok = Group.join(name, group, %{})
        send(test_pid, :joined)
        Process.sleep(:infinity)
      end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("didn't join")
      end

      assert Group.local_member_count(name, group) == 2
    end

    test "counts local group members by prefix", %{name: name} do
      prefix = "my_group/"
      group1 = prefix <> "a"
      group2 = prefix <> "b"
      other = "other_group/a"
      assert Group.local_member_count(name, prefix) == 0

      :ok = Group.join(name, group1, %{})
      :ok = Group.join(name, group2, %{})
      :ok = Group.join(name, other, %{})

      assert Group.local_member_count(name, prefix) == 2
      assert Group.local_member_count(name, other) == 1
    end
  end

  describe "local_entries/1" do
    test "returns local registry and pg entries across clusters", %{name: name} do
      :ok = Group.connect(name, "game")

      :ok = Group.register(name, "users/self", %{kind: :reg_self})
      :ok = Group.join(name, "rooms/self", %{kind: :pg_self})

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, "users/other", %{kind: :reg_other}, cluster: "game")
          :ok = Group.join(name, "rooms/other", %{kind: :pg_other}, cluster: "game")
          send(test_pid, {:ready, self()})
          Process.sleep(:infinity)
        end)

      on_exit(fn ->
        if Process.alive?(pid), do: Process.exit(pid, :kill)
      end)

      assert_receive {:ready, ^pid}, 1000

      entries =
        Group.local_entries(name)
        |> Enum.sort_by(fn {type, cluster, key, entry_pid, _meta} ->
          {type, cluster || "", key, inspect(entry_pid)}
        end)

      assert entries == [
               {:pg, nil, "rooms/self", self(), %{kind: :pg_self}},
               {:pg, "game", "rooms/other", pid, %{kind: :pg_other}},
               {:registry, nil, "users/self", self(), %{kind: :reg_self}},
               {:registry, "game", "users/other", pid, %{kind: :reg_other}}
             ]
    end

    test "applies configured extract_meta callback" do
      name = :"test_group_extract_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Group,
         name: name, shards: 1, log: false, extract_meta: {GroupTest.ExtractMeta, :strip, []}}
      )

      :ok = Group.register(name, "users/self", %{public: :keep, private: :drop})
      :ok = Group.join(name, "rooms/self", %{public: :keep_pg, private: :drop_pg})

      entries =
        Group.local_entries(name)
        |> Enum.sort_by(fn {type, _cluster, key, _pid, _meta} -> {type, key} end)

      assert entries == [
               {:pg, nil, "rooms/self", self(), %{public: :keep_pg}},
               {:registry, nil, "users/self", self(), %{public: :keep}}
             ]
    end
  end

  describe "concurrent operations" do
    test "concurrent join/leave on same key doesn't produce duplicates", %{name: name} do
      key = "concurrent/#{System.unique_integer([:positive])}"
      test_pid = self()

      pids =
        for _i <- 1..10 do
          spawn(fn ->
            :ok = Group.join(name, key, %{})
            send(test_pid, {:joined, self()})
            Process.sleep(:infinity)
          end)
        end

      for _ <- 1..10 do
        receive do
          {:joined, _} -> :ok
        after
          2000 -> flunk("timeout waiting for joins")
        end
      end

      members = Group.members(name, key)
      member_pids = Enum.map(members, fn {pid, _} -> pid end)
      assert length(member_pids) == length(Enum.uniq(member_pids))
      assert length(member_pids) == 10

      # Kill a few and verify cleanup
      Enum.take(pids, 3)
      |> Enum.each(&Process.exit(&1, :kill))

      Process.sleep(100)

      members = Group.members(name, key)
      assert length(members) == 7
    end

    test "concurrent register attempts on same key", %{name: name} do
      key = "race/#{System.unique_integer([:positive])}"
      test_pid = self()

      for _i <- 1..5 do
        spawn(fn ->
          result = Group.register(name, key, %{pid: self()})
          send(test_pid, {:result, self(), result})
          Process.sleep(:infinity)
        end)
      end

      results =
        for _ <- 1..5 do
          receive do
            {:result, pid, result} -> {pid, result}
          after
            2000 -> flunk("timeout")
          end
        end

      ok_results = Enum.filter(results, fn {_, r} -> r == :ok end)
      error_results = Enum.filter(results, fn {_, r} -> r == {:error, :taken} end)

      assert length(ok_results) == 1
      assert length(error_results) == 4
    end
  end

  describe "event batching" do
    test "process death batches :unregistered and :left into one message", %{name: name} do
      key = "batch/reg_and_join/#{System.unique_integer([:positive])}"
      :ok = Group.monitor(name, :all)

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, key, %{r: 1})
          :ok = Group.join(name, key, %{j: 1})
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      assert_receive :ready, 1000

      # Drain the individual :registered and :joined events
      assert_receive {:group, [%Group.Event{type: :registered}], _}, 1000
      assert_receive {:group, [%Group.Event{type: :joined}], _}, 1000

      # Kill the process — DOWN handler should batch both cleanup events
      Process.exit(pid, :kill)

      assert_receive {:group, events, _}, 1000
      types = events |> Enum.map(& &1.type) |> Enum.sort()
      assert types == [:left, :unregistered]
      assert Enum.all?(events, &(&1.key == key))
      assert Enum.all?(events, &(&1.pid == pid))
    end

    test "process death batches multiple :left events for same-shard keys", %{name: name} do
      num_shards = Group.get_config(name).num_shards

      # Find 3 keys that hash to the same shard
      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "batch/multi_#{i}" end)
        |> Stream.filter(fn key -> :erlang.phash2({nil, key}, num_shards) == 0 end)
        |> Enum.take(3)

      :ok = Group.monitor(name, :all)
      test_pid = self()

      pid =
        spawn(fn ->
          for key <- keys, do: :ok = Group.join(name, key, %{k: key})
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      assert_receive :ready, 1000

      # Drain individual :joined events
      for _key <- keys do
        assert_receive {:group, [%Group.Event{type: :joined}], _}, 1000
      end

      # Kill — all 3 :left events should arrive in one batch
      Process.exit(pid, :kill)

      assert_receive {:group, events, _}, 1000
      assert length(events) == 3
      assert Enum.all?(events, &(&1.type == :left))
      assert Enum.map(events, & &1.key) |> Enum.sort() == Enum.sort(keys)
    end

    test "single operations send single-event messages, not empty batches", %{name: name} do
      key = "batch/single/#{System.unique_integer([:positive])}"
      :ok = Group.monitor(name, :all)

      :ok = Group.register(name, key, %{})

      assert_receive {:group, events, _}, 1000
      assert [%Group.Event{type: :registered}] = events

      :ok = Group.unregister(name, key)

      assert_receive {:group, events, _}, 1000
      assert [%Group.Event{type: :unregistered}] = events
    end
  end

  describe "replicated PG receiver buffering" do
    test "flushes buffered replicated joins when buffer size is reached" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 2,
          replicated_pg_receiver_flush_interval: 60_000
        )

      shard = Group.Replica.shard_name(name, 0)
      key1 = "replicated/size/#{System.unique_integer([:positive])}/1"
      key2 = "replicated/size/#{System.unique_integer([:positive])}/2"
      pid1 = spawn_forever()
      pid2 = spawn_forever()

      on_exit(fn ->
        kill_if_alive(pid1)
        kill_if_alive(pid2)
      end)

      :ok = Group.monitor(name, :all)

      send(shard, replicated_pg_join(nil, key1, pid1, %{v: 1}, :join))

      assert Group.members(name, key1) == []
      refute_receive {:group, _events, _info}, 50

      send(shard, replicated_pg_join(nil, key2, pid2, %{v: 2}, :join))

      assert_receive {:group, events, _}, 1000
      assert Enum.map(events, & &1.key) == [key1, key2]
      assert Enum.map(events, & &1.type) == [:joined, :joined]
      assert Group.members(name, key1) == [{pid1, %{v: 1}}]
      assert Group.members(name, key2) == [{pid2, %{v: 2}}]
    end

    test "barrier messages flush buffered replicated ops in order" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 32,
          replicated_pg_receiver_flush_interval: 60_000
        )

      shard = Group.Replica.shard_name(name, 0)
      key = "replicated/barrier/#{System.unique_integer([:positive])}"
      pid = spawn_forever()

      on_exit(fn -> kill_if_alive(pid) end)

      :ok = Group.monitor(name, :all)

      send(shard, replicated_pg_join(nil, key, pid, %{v: 1}, :join))
      send(shard, replicated_pg_join(nil, key, pid, %{v: 2}, :update))
      send(shard, replicated_pg_leave(nil, key, pid, %{v: 2}, :leave))

      assert Group.members(name, key) == []
      flush_replicated_pg_barrier(shard)

      assert_receive {:group, events, _}, 1000

      assert [
               %Group.Event{
                 type: :joined,
                 key: ^key,
                 pid: ^pid,
                 meta: %{v: 1},
                 previous_meta: nil
               },
               %Group.Event{
                 type: :joined,
                 key: ^key,
                 pid: ^pid,
                 meta: %{v: 2},
                 previous_meta: %{v: 1}
               },
               %Group.Event{type: :left, key: ^key, pid: ^pid, meta: %{v: 2}, reason: :leave}
             ] = events

      assert_receive {:replicated_pg_buffer_flushed, ^shard}, 1000
      assert Group.members(name, key) == []
    end

    test "batchable traffic can flush an overdue buffer without relying on the timer" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 32,
          replicated_pg_receiver_flush_interval: 1_000
        )

      shard = Group.Replica.shard_name(name, 0)
      key1 = "replicated/due/#{System.unique_integer([:positive])}/1"
      key2 = "replicated/due/#{System.unique_integer([:positive])}/2"
      pid1 = spawn_forever()
      pid2 = spawn_forever()

      on_exit(fn ->
        kill_if_alive(pid1)
        kill_if_alive(pid2)
      end)

      :ok = Group.monitor(name, :all)

      send(shard, replicated_pg_join(nil, key1, pid1, %{v: 1}, :join))
      Process.sleep(10)

      :sys.replace_state(shard, fn state ->
        %{
          state
          | pending_replicated_pg_started_at: System.monotonic_time(:millisecond) - 5_000,
            pending_replicated_pg_flush_ref: nil
        }
      end)

      send(shard, replicated_pg_join(nil, key2, pid2, %{v: 2}, :join))

      assert_receive {:group, events, _}, 1000
      assert Enum.map(events, & &1.key) == [key1, key2]

      state = :sys.get_state(shard)
      assert state.pending_replicated_pg_len == 0
      assert Group.members(name, key1) == [{pid1, %{v: 1}}]
      assert Group.members(name, key2) == [{pid2, %{v: 2}}]
    end

    test "terminate flushes buffered replicated ops before shard restart" do
      name =
        start_single_shard_group(
          replicated_pg_receiver_buffer_size: 32,
          replicated_pg_receiver_flush_interval: 60_000
        )

      shard = Group.Replica.shard_name(name, 0)
      shard_pid = Process.whereis(shard)
      key = "replicated/terminate/#{System.unique_integer([:positive])}"
      pid = spawn_forever()

      on_exit(fn -> kill_if_alive(pid) end)

      :ok = Group.monitor(name, :all)

      send(shard, replicated_pg_join(nil, key, pid, %{v: 1}, :join))
      assert Group.members(name, key) == []

      ref = Process.monitor(shard_pid)
      :ok = GenServer.stop(shard_pid, :shutdown)
      assert_receive {:DOWN, ^ref, :process, ^shard_pid, :shutdown}, 1000
      assert_receive {:group, [%Group.Event{type: :joined, key: ^key, pid: ^pid}], _}, 1000

      wait_until(fn ->
        case Process.whereis(shard) do
          nil -> false
          new_pid -> new_pid != shard_pid
        end
      end)

      assert Group.members(name, key) == [{pid, %{v: 1}}]
    end
  end

  describe "replicated registry receiver buffering" do
    test "barrier messages flush buffered replicated register and unregister ops in order" do
      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 32,
          replicated_registry_receiver_flush_interval: 60_000
        )

      shard = Group.Replica.shard_name(name, 0)
      key = "replicated-registry/barrier/#{System.unique_integer([:positive])}"
      pid = spawn_forever()
      time1 = System.system_time()
      time2 = time1 + 1

      on_exit(fn -> kill_if_alive(pid) end)

      :ok = Group.monitor(name, :all)

      send(shard, replicated_register(nil, key, pid, %{v: 1}, :register, time1))
      send(shard, replicated_register(nil, key, pid, %{v: 2}, :update, time2))
      send(shard, replicated_unregister(nil, key, pid, %{v: 2}, :unregister))

      assert Group.lookup(name, key) == nil
      flush_replicated_registry_barrier(shard)

      assert_receive {:group, events, _}, 1_000

      assert [
               %Group.Event{
                 type: :registered,
                 key: ^key,
                 pid: ^pid,
                 meta: %{v: 1},
                 previous_meta: nil
               },
               %Group.Event{
                 type: :registered,
                 key: ^key,
                 pid: ^pid,
                 meta: %{v: 2},
                 previous_meta: %{v: 1}
               },
               %Group.Event{
                 type: :unregistered,
                 key: ^key,
                 pid: ^pid,
                 meta: %{v: 2},
                 reason: :unregister
               }
             ] = events

      assert_receive {:replicated_registry_buffer_flushed, ^shard}, 1_000
      assert Group.lookup(name, key) == nil
    end

    test "batchable registry traffic can flush an overdue buffer without relying on the timer" do
      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 32,
          replicated_registry_receiver_flush_interval: 1_000
        )

      shard = Group.Replica.shard_name(name, 0)
      key1 = "replicated-registry/due/#{System.unique_integer([:positive])}/1"
      key2 = "replicated-registry/due/#{System.unique_integer([:positive])}/2"
      pid1 = spawn_forever()
      pid2 = spawn_forever()

      on_exit(fn ->
        kill_if_alive(pid1)
        kill_if_alive(pid2)
      end)

      :ok = Group.monitor(name, :all)

      send(shard, replicated_register(nil, key1, pid1, %{v: 1}, :register))
      Process.sleep(10)

      :sys.replace_state(shard, fn state ->
        %{
          state
          | pending_replicated_registry_started_at: System.monotonic_time(:millisecond) - 5_000,
            pending_replicated_registry_flush_ref: nil
        }
      end)

      send(shard, replicated_register(nil, key2, pid2, %{v: 2}, :register))

      assert_receive {:group, events, _}, 1_000
      assert Enum.map(events, & &1.key) == [key1, key2]

      state = :sys.get_state(shard)
      assert state.pending_replicated_registry_len == 0
      assert Group.lookup(name, key1) == {pid1, %{v: 1}}
      assert Group.lookup(name, key2) == {pid2, %{v: 2}}
    end

    test "terminate flushes buffered replicated registry ops before shard restart" do
      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 32,
          replicated_registry_receiver_flush_interval: 60_000
        )

      shard = Group.Replica.shard_name(name, 0)
      shard_pid = Process.whereis(shard)
      key = "replicated-registry/terminate/#{System.unique_integer([:positive])}"
      pid = spawn_forever()

      on_exit(fn -> kill_if_alive(pid) end)

      :ok = Group.monitor(name, :all)

      send(shard, replicated_register(nil, key, pid, %{v: 1}, :register))
      assert Group.lookup(name, key) == nil

      ref = Process.monitor(shard_pid)
      :ok = GenServer.stop(shard_pid, :shutdown)
      assert_receive {:DOWN, ^ref, :process, ^shard_pid, :shutdown}, 1_000
      assert_receive {:group, [%Group.Event{type: :registered, key: ^key, pid: ^pid}], _}, 1_000

      wait_until(fn ->
        case Process.whereis(shard) do
          nil -> false
          new_pid -> new_pid != shard_pid
        end
      end)

      assert Group.lookup(name, key) == {pid, %{v: 1}}
    end

    test "replaces stale remote registry owner and clears the old by-pid entry" do
      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 32,
          replicated_registry_receiver_flush_interval: 60_000
        )

      shard = Group.Replica.shard_name(name, 0)
      key = "replicated-registry/replace/#{System.unique_integer([:positive])}"
      old_pid = spawn_forever()
      new_pid = spawn_forever()
      time1 = System.system_time()
      time2 = time1 + 1

      on_exit(fn ->
        kill_if_alive(old_pid)
        kill_if_alive(new_pid)
      end)

      Group.Replica.Data.registry_insert(
        name,
        0,
        nil,
        key,
        old_pid,
        %{v: 1},
        time1,
        :"remote_a@127.0.0.1"
      )

      send(shard, replicated_register(nil, key, new_pid, %{v: 2}, :register, time2))
      flush_replicated_registry_barrier(shard)

      assert_receive {:replicated_registry_buffer_flushed, ^shard}, 1_000
      assert Group.lookup(name, key) == {new_pid, %{v: 2}}
      assert Group.Replica.Data.registry_lookup_by_pid(name, 0, old_pid) == []

      assert [{nil, ^key, %{v: 2}, ^time2, _entry_node}] =
               Group.Replica.Data.registry_lookup_by_pid(name, 0, new_pid)
    end

    test "custom conflict resolver terminates the losing registry owner" do
      key = "replicated-registry/custom-loser/#{System.unique_integer([:positive])}"

      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 1,
          resolve_registry_conflict: {GroupTest.ResolveRegistryConflict, :pick, [:remote]}
        )

      parent = self()

      local_owner =
        spawn(fn ->
          :ok = Group.register(name, key, %{owner: :local})
          send(parent, {:custom_conflict_owner_ready, self()})
          Process.sleep(:infinity)
        end)

      remote_pid = spawn_forever()

      on_exit(fn ->
        kill_if_alive(local_owner)
        kill_if_alive(remote_pid)
      end)

      assert_receive {:custom_conflict_owner_ready, ^local_owner}, 1_000
      owner_ref = Process.monitor(local_owner)
      shard = Group.Replica.shard_name(name, 0)

      send(
        shard,
        replicated_register(
          nil,
          key,
          remote_pid,
          %{owner: :remote},
          :register,
          System.system_time()
        )
      )

      assert_receive {:DOWN, ^owner_ref, :process, ^local_owner,
                      {:group_registry_conflict, ^key, %{owner: :remote}}},
                     1_000

      assert Group.lookup(name, key) == {remote_pid, %{owner: :remote}}
    end

    test "batched remote conflict keeps the staged local winner when later unregister arrives" do
      key = "replicated-registry/conflict-local/#{System.unique_integer([:positive])}"

      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 32,
          replicated_registry_receiver_flush_interval: 60_000,
          resolve_registry_conflict: {GroupTest.ResolveRegistryConflict, :pick, [:local]}
        )

      parent = self()

      local_owner =
        spawn(fn ->
          :ok = Group.register(name, key, %{owner: :local})
          send(parent, {:local_registry_owner_ready, self()})
          Process.sleep(:infinity)
        end)

      remote_pid = spawn_forever()

      on_exit(fn ->
        kill_if_alive(local_owner)
        kill_if_alive(remote_pid)
      end)

      assert_receive {:local_registry_owner_ready, ^local_owner}, 1_000
      assert Group.lookup(name, key) == {local_owner, %{owner: :local}}

      :ok = Group.monitor(name, :all)
      shard = Group.Replica.shard_name(name, 0)

      send(
        shard,
        replicated_register(
          nil,
          key,
          remote_pid,
          %{owner: :remote},
          :register,
          System.system_time()
        )
      )

      send(shard, replicated_unregister(nil, key, remote_pid, %{owner: :remote}, :unregister))
      flush_replicated_registry_barrier(shard)

      assert_receive {:replicated_registry_buffer_flushed, ^shard}, 1_000
      refute_receive {:group, _events, _info}, 50
      assert Group.lookup(name, key) == {local_owner, %{owner: :local}}
      assert Group.Replica.Data.registry_lookup_by_pid(name, 0, remote_pid) == []
    end

    test "batched remote conflict removes the staged remote winner when later unregister arrives" do
      key = "replicated-registry/conflict-remote/#{System.unique_integer([:positive])}"

      name =
        start_single_shard_group(
          replicated_registry_receiver_buffer_size: 32,
          replicated_registry_receiver_flush_interval: 60_000,
          resolve_registry_conflict: {GroupTest.ResolveRegistryConflict, :pick, [:remote]}
        )

      parent = self()

      local_owner =
        spawn(fn ->
          :ok = Group.register(name, key, %{owner: :local})
          send(parent, {:remote_registry_owner_ready, self()})
          Process.sleep(:infinity)
        end)

      remote_pid = spawn_forever()

      on_exit(fn ->
        kill_if_alive(local_owner)
        kill_if_alive(remote_pid)
      end)

      assert_receive {:remote_registry_owner_ready, ^local_owner}, 1_000
      assert Group.lookup(name, key) == {local_owner, %{owner: :local}}

      :ok = Group.monitor(name, :all)
      shard = Group.Replica.shard_name(name, 0)

      send(
        shard,
        replicated_register(
          nil,
          key,
          remote_pid,
          %{owner: :remote},
          :register,
          System.system_time()
        )
      )

      send(shard, replicated_unregister(nil, key, remote_pid, %{owner: :remote}, :unregister))
      flush_replicated_registry_barrier(shard)

      assert_receive {:group, events, _}, 1_000

      assert [
               %Group.Event{
                 type: :unregistered,
                 key: ^key,
                 pid: ^local_owner,
                 meta: %{owner: :local},
                 reason: :resolve_conflict
               },
               %Group.Event{
                 type: :unregistered,
                 key: ^key,
                 pid: ^remote_pid,
                 meta: %{owner: :remote},
                 reason: :unregister
               }
             ] = events

      assert_receive {:replicated_registry_buffer_flushed, ^shard}, 1_000
      assert Group.lookup(name, key) == nil
      assert Group.Replica.Data.registry_lookup_by_pid(name, 0, local_owner) == []
      assert Group.Replica.Data.registry_lookup_by_pid(name, 0, remote_pid) == []
    end
  end

  defp start_single_shard_group(opts \\ []) do
    name = :"test_timeout_group_#{System.unique_integer([:positive])}"
    opts = Keyword.merge([name: name, shards: 1, log: false], opts)
    start_supervised!({Group, opts})
    name
  end

  defp suspend_only_shard(name) do
    shard = Group.Replica.shard_name(name, 0)
    :ok = :sys.suspend(shard)
    shard
  end

  defp resume_shard_if_alive(shard) do
    if Process.whereis(shard) do
      :ok = :sys.resume(shard)
    end

    :ok
  end

  defp assert_genserver_call_timeout(fun) do
    assert {:timeout, {GenServer, :call, _}} = catch_exit(fun.())
  end

  defp replicated_pg_join(cluster, key, pid, meta, reason) do
    {:replicate_pg_batch,
     [{:join, cluster, key, pid, meta, System.system_time(), reason, node(pid)}]}
  end

  defp replicated_pg_leave(cluster, key, pid, meta, reason) do
    {:replicate_pg_batch, [{:leave, cluster, key, pid, meta, reason}]}
  end

  defp replicated_register(cluster, key, pid, meta, _reason, time \\ System.system_time()) do
    {:replicate_registry_batch, [{:register, cluster, key, pid, meta, time, node(pid)}]}
  end

  defp replicated_unregister(cluster, key, pid, meta, reason) do
    {:replicate_registry_batch, [{:unregister, cluster, key, pid, meta, reason}]}
  end

  defp enqueue_replicated_pg_backlog(shard, key_prefix, pid, count) do
    for i <- 1..count do
      send(shard, replicated_pg_join(nil, "#{key_prefix}/#{i}", pid, %{}, :join))
    end

    :ok
  end

  defp enqueue_replicated_registry_backlog(shard, key_prefix, pid, count) do
    for i <- 1..count do
      send(shard, replicated_register(nil, "#{key_prefix}/#{i}", pid, %{seq: i}, :register))
    end

    :ok
  end

  defp spawn_requester(fun, tag) do
    parent = self()

    spawn(fn ->
      result = fun.()
      send(parent, {tag, self(), result})
      Process.sleep(:infinity)
    end)
  end

  defp shard_message_queue_len(shard) do
    case Process.info(Process.whereis(shard), :message_queue_len) do
      {:message_queue_len, len} -> len
      nil -> 0
    end
  end

  defp flush_replicated_pg_barrier(shard) do
    send(shard, {:group_dispatch, [self()], {:replicated_pg_buffer_flushed, shard}})
  end

  defp flush_replicated_registry_barrier(shard) do
    send(shard, {:group_dispatch, [self()], {:replicated_registry_buffer_flushed, shard}})
  end

  defp force_cluster_lease_sweep(name) do
    lease_manager = Group.ClusterLease.lease_name(name)
    send(lease_manager, :force_sweep)
    :sys.get_state(lease_manager)
    :ok
  end

  defp expire_cluster_lease(name, cluster) do
    {ttl_ms, _expires_at} = Group.Replica.Data.cluster_lease(name, cluster)

    Group.Replica.Data.put_cluster_lease(
      name,
      cluster,
      ttl_ms,
      System.monotonic_time(:millisecond) - 1
    )

    ttl_ms
  end

  defp spawn_forever do
    spawn(fn -> Process.sleep(:infinity) end)
  end

  defp kill_if_alive(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :kill)
    end

    :ok
  end

  defp wait_until(fun, timeout \\ 1_000)

  defp wait_until(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true")
      end

      Process.sleep(10)
      do_wait_until(fun, deadline)
    end
  end
end
