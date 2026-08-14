defmodule GroupTest.ExtractMeta do
  def strip(meta), do: Map.take(meta, [:public])
end

defmodule GroupTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  setup do
    name = :"test_group_#{System.unique_integer([:positive])}"
    start_supervised!({Group, name: name, shards: 4, log: false})
    {:ok, name: name}
  end

  describe "startup options" do
    test "runtime config omits unused callback state", %{name: name} do
      refute Map.has_key?(Group.get_config(name), :callbacks)
    end

    test "rejects invalid shard counts" do
      for shards <- [0, -1, 1.5, :many] do
        name = :"invalid_shards_#{System.unique_integer([:positive])}"

        error =
          assert_raise ArgumentError, fn ->
            Group.Supervisor.init(name: name, shards: shards, log: false)
          end

        assert error.message =~ ":shards"
        assert error.message =~ "positive integer"
      end
    end
  end

  describe "replica ingress fairness" do
    test "an oversized incoming batch yields to an already queued local write", %{name: name} do
      key =
        1..1_000
        |> Enum.map(&"ingress-fairness/#{&1}")
        |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 4) == 0))

      shard = Process.whereis(Group.Replica.shard_name(name, 0))
      :ok = :sys.suspend(shard)

      on_exit(fn ->
        if Process.alive?(shard), do: :erlang.trace(shard, false, [:call])
        :erlang.trace_pattern({Group.Replica, :handle_replica_message, 3}, false, [:local])
        Group.TestCluster.resume_if_alive(shard)
      end)

      parent = self()
      owner = spawn(fn -> replica_ingress_fairness_owner(parent) end)

      :erlang.trace(shard, true, [:call, {:tracer, owner}])
      :erlang.trace_pattern({Group.Replica, :handle_replica_message, 3}, true, [:local])

      source_node = :"ingress-source@test"
      batch_size = 65
      messages = List.duplicate({:malformed_replica_message, make_ref()}, batch_size)

      assert :ok = Group.Transport.incoming_batch(name, source_node, 0, messages)

      epoch = Group.Replica.Data.local_cluster_epoch(name, nil)
      send(owner, {:write, shard, {:register, nil, epoch, key, owner, %{kind: :local}}})

      wait_until(fn ->
        match?(
          {:message_queue_len, length} when length >= 2,
          Process.info(shard, :message_queue_len)
        )
      end)

      :ok = :sys.resume(shard)

      assert_receive {:local_write_finished, ^owner, :ok, calls_before_local}, 5_000
      assert calls_before_local < batch_size
      assert match?({^owner, %{kind: :local}}, Group.lookup(name, key))
    end
  end

  describe "monitor_generation/1" do
    test "notifies long-lived owners when local membership storage exits", %{name: name} do
      assert {:ok, generation_pid, monitor_ref} = Group.monitor_generation(name)

      Process.exit(generation_pid, :kill)

      assert_receive {:DOWN, ^monitor_ref, :process, ^generation_pid, :killed}
    end

    test "returns not_running for an unknown Group" do
      name = :"missing_group_#{System.unique_integer([:positive])}"
      assert {:error, :not_running} = Group.monitor_generation(name)
    end
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

  describe "named-cluster mutation fencing" do
    test "a shard rejects registry and PG writes after their cluster epoch retires", %{name: name} do
      cluster = "retired/#{System.unique_integer([:positive])}"
      registry_key = "retired/registry"
      pg_key = "retired/pg"

      assert :ok = Group.connect(name, cluster)
      old_epoch = Group.Replica.Data.local_cluster_epoch(name, cluster)
      assert :ok = Group.disconnect(name, cluster)
      assert :ok = Group.connect(name, cluster)
      refute Group.Replica.Data.local_cluster_epoch(name, cluster) == old_epoch

      registry_shard = Group.Replica.shard_for(name, cluster, registry_key)
      pg_shard = Group.Replica.shard_for(name, cluster, pg_key)

      registry_result =
        Group.Replica.local_request(
          registry_shard,
          {:register, cluster, old_epoch, registry_key, self(), %{stale: true}},
          5_000
        )

      pg_result =
        Group.Replica.local_request(
          pg_shard,
          {:join, cluster, old_epoch, pg_key, self(), %{stale: true}},
          5_000
        )

      assert registry_result == {:error, :stale_cluster_epoch}
      assert pg_result == {:error, :stale_cluster_epoch}
      assert Group.lookup(name, registry_key, cluster: cluster) == nil
      assert Group.members(name, pg_key, cluster: cluster) == []
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

    test "lookup propagates ArgumentError from metadata extraction", %{name: name} do
      key = "user/extractor-error/#{System.unique_integer([:positive])}"
      :ok = Group.register(name, key, %{module: :test})

      assert_raise ArgumentError, "extractor failed", fn ->
        Group.lookup(name, key,
          extract_meta: fn _meta -> raise ArgumentError, "extractor failed" end
        )
      end
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

    test "limits exact-key results without changing the return shape", %{name: name} do
      key = "limited/#{System.unique_integer([:positive])}"
      test_pid = self()

      pids =
        for id <- 1..5 do
          pid =
            spawn(fn ->
              :ok = Group.join(name, key, %{id: id})
              send(test_pid, {:joined, self()})
              Process.sleep(:infinity)
            end)

          on_exit(fn ->
            if Process.alive?(pid), do: Process.exit(pid, :kill)
          end)

          pid
        end

      for pid <- pids, do: assert_receive({:joined, ^pid}, 1000)

      members = Group.members(name, key, limit: 2)

      assert length(members) == 2
      assert Enum.all?(members, fn {pid, %{id: id}} -> pid in pids and id in 1..5 end)
      assert Group.members(name, key, limit: 0) == []
      assert length(Group.members(name, key)) == 5
    end

    test "rejects invalid limits", %{name: name} do
      assert_raise ArgumentError, ~r/expected :limit to be a non-negative integer/, fn ->
        Group.members(name, "limited", limit: -1)
      end

      assert_raise ArgumentError, ~r/expected :limit to be a non-negative integer/, fn ->
        Group.members(name, "limited", limit: "1")
      end
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

    test "applies one global limit across prefix-query shards", %{name: name} do
      prefix = "limited_shard_spread/#{System.unique_integer([:positive])}/"

      for i <- 1..20 do
        :ok = Group.join(name, prefix <> "item_#{i}", %{i: i})
      end

      members = Group.members(name, prefix, limit: 3)

      assert length(members) == 3
      assert Enum.all?(members, fn {pid, %{i: i}} -> pid == self() and i in 1..20 end)
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

      :ok = Group.Replica.Data.add_cluster_node(name, [cluster], dead_node)

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
            Group.Replica.Data.add_cluster_node(name, [cluster], remote_node)
          else
            Group.Replica.Data.remove_cluster_node(name, [cluster], remote_node)
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
            {:group_local_request, _alias, {:cluster_disconnect, [^cluster]}} ->
              true

            {:group_local_request, _alias, {:cluster_disconnect, [^cluster], _epochs}} ->
              true

            {:group_local_request, _caller, _ref, {:cluster_disconnect, [^cluster]}} ->
              true

            {:group_local_request, _caller, _ref, {:cluster_disconnect, [^cluster], _epochs}} ->
              true

            _ ->
              false
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

    test "remote dispatch uses a non-suspending send without connecting", %{name: name} do
      key = "dispatch/nonblocking/#{System.unique_integer([:positive])}"
      remote_node = :"missing_#{System.unique_integer([:positive])}@127.0.0.1"
      member = spawn_forever()
      num_shards = Group.get_config(name).num_shards
      key_shard = Group.Replica.shard_index_for(nil, key, num_shards)

      on_exit(fn -> kill_if_alive(member) end)

      now = System.system_time()
      Group.Replica.Data.registry_insert(name, key_shard, nil, key, member, %{}, now, remote_node)
      Group.Replica.Data.pg_insert(name, key_shard, nil, key, member, %{}, now, remote_node)

      test_pid = self()

      dispatcher =
        spawn(fn ->
          receive do
            :dispatch ->
              send(test_pid, {:dispatch_result, self(), Group.dispatch(name, key, :hello)})
          end
        end)

      on_exit(fn -> kill_if_alive(dispatcher) end)
      :erlang.trace(dispatcher, true, [:call])
      :erlang.trace_pattern({:erlang, :send_nosuspend, 3}, true, [:local])

      on_exit(fn ->
        if Process.alive?(dispatcher), do: :erlang.trace(dispatcher, false, [:call])
        :erlang.trace_pattern({:erlang, :send_nosuspend, 3}, false, [:local])
      end)

      send(dispatcher, :dispatch)
      assert_receive {:dispatch_result, ^dispatcher, :ok}, 1_000

      assert_receive {:trace, ^dispatcher, :call,
                      {:erlang, :send_nosuspend, [^member, :hello, [:noconnect]]}},
                     1_000

      dispatch_shard = :erlang.phash2(dispatcher, num_shards)
      shard_name = Group.Replica.shard_name(name, dispatch_shard)

      assert_receive {:trace, ^dispatcher, :call,
                      {:erlang, :send_nosuspend,
                       [
                         {^shard_name, ^remote_node},
                         {:group_dispatch, [^member], :hello},
                         [:noconnect]
                       ]}},
                     1_000
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

    test "disconnect timeout still fans cleanup out to every shard" do
      name = :"test_disconnect_timeout_#{System.unique_integer([:positive])}"
      cluster = "timeout_cluster"
      start_supervised!({Group, name: name, shards: 2, log: false})

      key =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(&"timeout/shard-one/#{&1}")
        |> Enum.find(fn key -> Group.Replica.shard_index_for(cluster, key, 2) == 1 end)

      assert :ok = Group.connect(name, cluster)
      assert :ok = Group.join(name, key, %{}, cluster: cluster)
      assert Group.members(name, key, cluster: cluster) == [{self(), %{}}]

      remote_route = :"disconnect-timeout@remote"
      :ok = Group.Replica.Data.add_cluster_node(name, [cluster], remote_route)

      shard_zero = Group.Replica.shard_name(name, 0)
      :ok = :sys.suspend(shard_zero)

      try do
        assert_genserver_call_timeout(fn ->
          Group.disconnect(name, cluster, timeout: 10)
        end)
      after
        resume_shard_if_alive(shard_zero)
      end

      :sys.get_state(shard_zero)
      refute Group.connected?(name, cluster)
      assert Group.members(name, key, cluster: cluster) == []

      wait_until(fn ->
        Group.Replica.Data.closed_local_clusters(name) == [] and
          Group.Replica.Data.cluster_nodes(name, cluster) == []
      end)
    end

    test "reconnect waits for every old-epoch shard cleanup before admitting new writes" do
      name = :"test_reconnect_barrier_#{System.unique_integer([:positive])}"
      cluster = "reconnect_barrier"
      key = "reconnect/barrier/#{System.unique_integer([:positive])}"
      start_supervised!({Group, name: name, shards: 2, log: false})

      assert :ok = Group.connect(name, cluster)
      shard_zero = Group.Replica.shard_name(name, 0)
      :ok = :sys.suspend(shard_zero)

      reconnect_caller =
        try do
          assert_genserver_call_timeout(fn ->
            Group.disconnect(name, cluster, timeout: 10)
          end)

          caller =
            spawn_requester(
              fn -> Group.connect(name, cluster) end,
              :reconnect_barrier_result
            )

          refute_receive {:reconnect_barrier_result, ^caller, _result}, 50
          caller
        after
          resume_shard_if_alive(shard_zero)
        end

      on_exit(fn -> kill_if_alive(reconnect_caller) end)
      assert_receive {:reconnect_barrier_result, ^reconnect_caller, :ok}, 1_000

      assert Group.Replica.Data.closed_local_clusters(name) == []
      assert :ok = Group.join(name, key, %{epoch: :new}, cluster: cluster)
      assert Group.members(name, key, cluster: cluster) == [{self(), %{epoch: :new}}]
      assert :ok = Group.TestCluster.assert_replica_consistent(name)
    end

    test "a delayed duplicate disconnect cannot purge a reconnected epoch" do
      name = start_single_shard_group()
      cluster = "duplicate-disconnect/#{System.unique_integer([:positive])}"
      reg_key = "duplicate-disconnect/registry/#{System.unique_integer([:positive])}"
      pg_key = "duplicate-disconnect/pg/#{System.unique_integer([:positive])}"

      :ok = Group.connect(name, cluster)
      old_epoch = Group.Replica.Data.local_cluster_epoch(name, cluster)
      :ok = Group.disconnect(name, cluster)
      :ok = Group.connect(name, cluster)
      :ok = Group.register(name, reg_key, %{epoch: :new}, cluster: cluster)
      :ok = Group.join(name, pg_key, %{epoch: :new}, cluster: cluster)

      # A concurrent second disconnect that observed the already-closed local
      # epoch can queue either an unfenced request or the completed old epoch.
      # Model both arriving only after reconnect.
      for stale_epoch <- [nil, old_epoch] do
        assert :ok =
                 Group.Replica.local_request(
                   Group.Replica.shard_name(name, 0),
                   {:cluster_disconnect, [cluster], [{cluster, stale_epoch}]},
                   5_000
                 )

        assert Group.connected?(name, cluster)
        assert Group.lookup(name, reg_key, cluster: cluster) == {self(), %{epoch: :new}}
        assert Group.members(name, pg_key, cluster: cluster) == [{self(), %{epoch: :new}}]
      end

      assert :ok = Group.TestCluster.assert_replica_consistent(name)
    end
  end

  describe "local request fairness" do
    test "a control flood yields to a queued local request after bounded work" do
      name = start_single_shard_group()
      shard = suspend_only_shard(name)
      remote_node = :"missing-control-peer@nohost"
      key = "fair/control/local/#{System.unique_integer([:positive])}"

      send(
        shard,
        {:group_replica_frame, remote_node, {:heads, Group.Replica.WireProtocol.version(), []}}
      )

      for _ <- 1..10_000 do
        send(shard, {:nodedown, remote_node})
      end

      ref = make_ref()
      epoch = Group.Replica.Data.generation(name)

      send(
        shard,
        {:group_local_request, self(), ref, {:register, nil, epoch, key, self(), %{local: true}}}
      )

      wait_until(fn -> shard_message_queue_len(shard) >= 10_002 end, 2_000)
      :ok = :sys.resume(shard)

      assert_receive {:group_local_reply, ^ref, :ok}, 100
      assert Group.lookup(name, key) == {self(), %{local: true}}

      # The assertion has already proved bounded yielding. Drop the synthetic
      # duplicate-control tail instead of charging every PR for draining it.
      old_shard = Process.whereis(shard)
      Process.exit(old_shard, :kill)
      wait_until(fn -> is_pid(Process.whereis(shard)) and Process.whereis(shard) != old_shard end)
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
    test "local activity checks use bounded ETS selects", %{name: name} do
      registry_key = "presence/registry/#{System.unique_integer([:positive])}"
      pg_key = "presence/pg/#{System.unique_integer([:positive])}"
      :ok = Group.register(name, registry_key, %{})
      :ok = Group.join(name, pg_key, %{})
      num_shards = Group.get_config(name).num_shards
      parent = self()

      :erlang.trace_pattern({:ets, :select, 3}, true, [:local])
      :erlang.trace_pattern({:ets, :select_count, 2}, true, [:local])

      on_exit(fn ->
        :erlang.trace_pattern({:ets, :select, 3}, false, [:local])
        :erlang.trace_pattern({:ets, :select_count, 2}, false, [:local])
      end)

      checks = [
        registry: fn -> Group.Replica.Data.local_registry_present?(name, num_shards, nil) end,
        pg: fn -> Group.Replica.Data.local_pg_present?(name, num_shards, nil) end
      ]

      for {kind, check} <- checks do
        worker =
          spawn(fn ->
            receive do
              :check -> send(parent, {:presence_result, kind, check.()})
            end
          end)

        :erlang.trace(worker, true, [:call])
        send(worker, :check)

        assert_receive {:trace, ^worker, :call, {:ets, :select, [_table, _match_spec, 1]}},
                       1_000

        refute_receive {:trace, ^worker, :call, {:ets, :select_count, _args}}, 20
        assert_receive {:presence_result, ^kind, true}, 1_000
      end
    end

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
    test "exact member counts query only the owning shard", %{name: name} do
      num_shards = Group.get_config(name).num_shards
      target_shard = num_shards - 1

      key =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(&"count/exact/#{&1}")
        |> Enum.find(fn key ->
          Group.Replica.shard_index_for(nil, key, num_shards) == target_shard
        end)

      :ok = Group.join(name, key, %{})
      expected_table = Group.Replica.Data.pg_by_key_table(name, target_shard)
      parent = self()

      :erlang.trace_pattern({:ets, :select_count, 2}, true, [:local])

      on_exit(fn ->
        :erlang.trace_pattern({:ets, :select_count, 2}, false, [:local])
      end)

      checks = [
        total: fn -> Group.member_count(name, key) end,
        local: fn -> Group.local_member_count(name, key) end
      ]

      for {kind, check} <- checks do
        worker =
          spawn(fn ->
            receive do
              :count -> send(parent, {:count_result, kind, check.()})
            end
          end)

        :erlang.trace(worker, true, [:call])
        send(worker, :count)

        assert_receive {:trace, ^worker, :call,
                        {:ets, :select_count, [^expected_table, _match_spec]}},
                       1_000

        assert_receive {:count_result, ^kind, 1}, 1_000
        refute_receive {:trace, ^worker, :call, {:ets, :select_count, _args}}, 20
      end
    end

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

  describe "local_members/3" do
    test "returns local exact-key members and honors the limit", %{name: name} do
      key = "local_members/#{System.unique_integer([:positive])}"
      test_pid = self()

      pids =
        for id <- 1..4 do
          pid =
            spawn(fn ->
              :ok = Group.join(name, key, %{id: id})
              send(test_pid, {:joined, self()})
              Process.sleep(:infinity)
            end)

          on_exit(fn ->
            if Process.alive?(pid), do: Process.exit(pid, :kill)
          end)

          pid
        end

      for pid <- pids, do: assert_receive({:joined, ^pid}, 1000)

      members = Group.local_members(name, key, limit: 2)

      assert length(members) == 2
      assert Enum.all?(members, fn {pid, %{id: id}} -> pid in pids and id in 1..4 end)
      assert Group.local_members(name, key, limit: 0) == []
      assert length(Group.local_members(name, key)) == 4
    end

    test "supports prefix queries and metadata extraction", %{name: name} do
      prefix = "local_prefix/#{System.unique_integer([:positive])}/"

      for id <- 1..5 do
        :ok = Group.join(name, prefix <> Integer.to_string(id), %{public: id, private: :drop})
      end

      members =
        Group.local_members(name, prefix,
          limit: 2,
          extract_meta: {GroupTest.ExtractMeta, :strip, []}
        )

      assert length(members) == 2

      assert Enum.all?(members, fn {pid, meta} ->
               pid == self() and Map.keys(meta) == [:public]
             end)
    end

    test "rejects invalid limits", %{name: name} do
      assert_raise ArgumentError, ~r/expected :limit to be a non-negative integer/, fn ->
        Group.local_members(name, "local_members", limit: nil)
      end
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

    test "rejects invalid extract_meta configuration at startup" do
      for bad <- ["nope", {Map, :take}, fn a, b -> {a, b} end] do
        name = :"invalid_extract_meta_#{System.unique_integer([:positive])}"

        error =
          assert_raise ArgumentError, fn ->
            Group.Supervisor.init(name: name, extract_meta: bad, log: false)
          end

        assert error.message =~ ":extract_meta"
      end
    end

    test "applies a configured extract_meta function to reads and events" do
      name = :"test_group_extract_fun_#{System.unique_integer([:positive])}"
      extract_meta = fn meta -> Map.take(meta, [:public]) end
      start_supervised!({Group, name: name, shards: 1, log: false, extract_meta: extract_meta})

      registry_key = "users/function"
      pg_key = "rooms/function"
      :ok = Group.monitor(name, :all)

      :ok = Group.register(name, registry_key, %{public: :registry, private: :drop})
      :ok = Group.join(name, pg_key, %{public: :pg, private: :drop})

      assert_receive {:group,
                      [%Group.Event{type: :registered, key: ^registry_key} = registry_event], _},
                     1_000

      assert registry_event.meta == %{public: :registry}

      assert_receive {:group, [%Group.Event{type: :joined, key: ^pg_key} = pg_event], _}, 1_000
      assert pg_event.meta == %{public: :pg}

      assert Group.lookup(name, registry_key) == {self(), %{public: :registry}}
      assert Group.members(name, pg_key) == [{self(), %{public: :pg}}]

      assert Enum.sort(Group.local_entries(name)) ==
               Enum.sort([
                 {:registry, nil, registry_key, self(), %{public: :registry}},
                 {:pg, nil, pg_key, self(), %{public: :pg}}
               ])
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

    test "process death replication uses the non-suspending remote send path", %{name: name} do
      key = "batch/nonblocking/#{System.unique_integer([:positive])}"
      parent = self()

      owner =
        spawn(fn ->
          :ok = Group.register(name, key, %{})
          send(parent, {:nonblocking_owner_ready, self()})
          Process.sleep(:infinity)
        end)

      on_exit(fn -> kill_if_alive(owner) end)
      assert_receive {:nonblocking_owner_ready, ^owner}, 1_000

      shard_name = Group.Replica.shard_for(name, nil, key)
      shard_pid = Process.whereis(shard_name)

      # Flush the registration sender buffer before tracing the DOWN path.
      flush_replicated_registry_barrier(shard_name)
      assert_receive {:replicated_registry_buffer_flushed, ^shard_name}, 1_000

      :sys.replace_state(shard_pid, fn state ->
        %{
          state
          | remote_shards: Map.put(state.remote_shards, node(), self()),
            peer_last_seen:
              Map.put(state.peer_last_seen, node(), System.monotonic_time(:millisecond))
        }
      end)

      :erlang.trace(shard_pid, true, [:call])
      :erlang.trace_pattern({:erlang, :send_nosuspend, 3}, true, [:local])

      on_exit(fn ->
        if Process.alive?(shard_pid), do: :erlang.trace(shard_pid, false, [:call])
        :erlang.trace_pattern({:erlang, :send_nosuspend, 3}, false, [:local])
      end)

      Process.exit(owner, :kill)
      local_node = node()

      assert_receive {:trace, ^shard_pid, :call,
                      {:erlang, :send_nosuspend,
                       [
                         {^shard_name, ^local_node},
                         {:group_replica_frame, ^local_node, {:delta_batch, _version, runs}},
                         [:noconnect]
                       ]}},
                     1_000

      assert Enum.any?(runs, fn {_stream_id, _first_seq, records, _head} ->
               Enum.any?(records, fn {_seq, mutations} ->
                 {:unregister, nil, key, owner, %{}, :killed} in mutations
               end)
             end)
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
    test "legacy unsequenced ingress cannot materialize or delete rows" do
      name = start_single_shard_group()
      shard = Group.Replica.shard_name(name, 0)
      registry_key = "legacy-ingress/registry/#{System.unique_integer([:positive])}"
      pg_key = "legacy-ingress/pg/#{System.unique_integer([:positive])}"
      cluster_state_key = "legacy-ingress/cluster-state/#{System.unique_integer([:positive])}"
      retained_key = "legacy-ingress/retained/#{System.unique_integer([:positive])}"
      owner = spawn_forever()

      on_exit(fn -> kill_if_alive(owner) end)

      send(shard, replicated_register(nil, registry_key, owner, %{legacy: true}, :register))
      send(shard, replicated_pg_join(nil, pg_key, owner, %{legacy: true}, :join))
      send(shard, {:cluster_state, nil, [{cluster_state_key, owner, %{}, 1}], []})

      :ok = Group.register(name, retained_key, %{retained: true})

      # Pre-AE cluster lifecycle messages were unsequenced and unfenced. A
      # delayed copy must not purge current rows while leaving their stream
      # cursor advanced.
      send(shard, {:cluster_disconnect, [nil], self()})

      send(
        shard,
        {:replicate_process_down_batch,
         [{self(), nil, retained_key, %{retained: true}, :legacy_delete}], []}
      )

      _state = :sys.get_state(shard)

      assert Group.lookup(name, registry_key) == nil
      assert Group.members(name, pg_key) == []
      assert Group.lookup(name, cluster_state_key) == nil
      assert Group.lookup(name, retained_key) == {self(), %{retained: true}}
      assert :ok = Group.TestCluster.assert_replica_consistent(name)
    end
  end

  describe "replica write-ahead journal" do
    test "concurrent shards retain independent append order", %{name: name} do
      named_cluster = "journal/append-order"
      operations_per_shard = 100
      :ok = Group.connect(name, named_cluster)

      parent = self()

      owners =
        for shard <- 0..3 do
          nil_keys =
            keys_for_shard(nil, "journal/append-order/nil/#{shard}", 4, shard, 50)

          named_keys =
            keys_for_shard(
              named_cluster,
              "journal/append-order/named/#{shard}",
              4,
              shard,
              50
            )

          spawn(fn ->
            nil_keys
            |> Enum.zip(named_keys)
            |> Enum.each(fn {nil_key, named_key} ->
              :ok = Group.register(name, nil_key, %{})
              :ok = Group.register(name, named_key, %{}, cluster: named_cluster)
            end)

            send(parent, {:append_order_complete, shard, self()})
            Process.sleep(:infinity)
          end)
        end

      on_exit(fn -> Enum.each(owners, &kill_if_alive/1) end)

      Enum.with_index(owners)
      |> Enum.each(fn {owner, shard} ->
        assert_receive {:append_order_complete, ^shard, ^owner}, 10_000
      end)

      metadata = Group.Replica.Data.replication_meta_table(name)
      assert :ets.info(metadata, :write_concurrency) == :auto

      for shard <- 0..3 do
        assert [{{:append_counter, shard}, operations_per_shard}] ==
                 :ets.lookup(metadata, {:append_counter, shard})

        order_rows =
          name
          |> Group.Replica.Data.replica_oplog_order_table(shard)
          |> :ets.tab2list()
          |> Enum.sort()

        assert Enum.map(order_rows, &elem(&1, 0)) ==
                 Enum.to_list(1..operations_per_shard)

        assert Enum.all?(order_rows, fn {_append_id, stream_id, _seq} ->
                 Group.Replica.WireProtocol.stream_shard(stream_id) == shard
               end)

        oplog_rows =
          name
          |> Group.Replica.Data.replica_oplog_table(shard)
          |> :ets.tab2list()
          |> MapSet.new(fn {{stream_id, seq}, append_id, _mutations} ->
            {append_id, stream_id, seq}
          end)

        assert MapSet.new(order_rows) == oplog_rows

        order_rows
        |> Enum.group_by(fn {_append_id, stream_id, _seq} -> stream_id end)
        |> Enum.each(fn {_stream_id, rows} ->
          assert Enum.map(rows, &elem(&1, 2)) == Enum.to_list(1..50)
        end)
      end
    end

    test "a shard restart replays an appended mixed record and later cleans its owner" do
      name = start_single_shard_group(replicated_oplog_max_entries: 16)
      key = "journal/replay/#{System.unique_integer([:positive])}"
      owner = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> kill_if_alive(owner) end)

      stream_id = Group.Replica.Data.local_stream_id(name, 0, nil)
      time = System.system_time()

      {seq, _mutations} =
        Group.Replica.Data.append_replica_record(name, 0, stream_id, [
          {:register, nil, key, owner, %{kind: :registry}, time, node()},
          {:join, nil, key, owner, %{kind: :pg}, time, :join, node()}
        ])

      old_shard = Process.whereis(Group.Replica.shard_name(name, 0))
      Process.exit(old_shard, :kill)

      Group.TestCluster.assert_eventually(fn ->
        new_shard = Process.whereis(Group.Replica.shard_name(name, 0))

        is_pid(new_shard) and new_shard != old_shard and
          Group.lookup(name, key) == {owner, %{kind: :registry}} and
          Group.members(name, key) == [{owner, %{kind: :pg}}]
      end)

      assert {_floor, ^seq, ^seq} =
               Group.Replica.Data.replica_stream_head(name, 0, stream_id)

      Process.exit(owner, :kill)

      Group.TestCluster.assert_eventually(fn ->
        Group.lookup(name, key) == nil and Group.members(name, key) == []
      end)
    end

    test "a shard restart repairs an interrupted append tail and interrupted prune floor" do
      name = start_single_shard_group(replicated_oplog_max_entries: 16)
      stream_id = Group.Replica.Data.local_stream_id(name, 0, nil)
      first_key = "journal/crash-window/first/#{System.unique_integer([:positive])}"
      second_key = "journal/crash-window/second/#{System.unique_integer([:positive])}"
      third_key = "journal/crash-window/third/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, first_key, %{seq: 1})
      :ok = Group.register(name, second_key, %{seq: 2})

      oplog = Group.Replica.Data.replica_oplog_table(name, 0)
      order = Group.Replica.Data.replica_oplog_order_table(name, 0)
      stream_meta = Group.Replica.Data.replica_stream_meta_table(name, 0)

      [{{^stream_id, 1}, first_append_id, _mutations}] = :ets.lookup(oplog, {stream_id, 1})

      # Pruning removes order -> record -> advances floor. Model a kill after
      # the first two writes but before the floor update.
      :ets.delete(order, first_append_id)
      :ets.delete(oplog, {stream_id, 1})

      # Appending advances head -> append counter -> record -> order. Model a
      # kill after the head update but before the record exists.
      assert 3 = :ets.update_counter(stream_meta, stream_id, {2, 1})

      old_shard = Process.whereis(Group.Replica.shard_name(name, 0))
      Process.exit(old_shard, :kill)

      Group.TestCluster.assert_eventually(fn ->
        new_shard = Process.whereis(Group.Replica.shard_name(name, 0))
        is_pid(new_shard) and new_shard != old_shard
      end)

      :sys.get_state(Group.Replica.shard_name(name, 0))
      assert {2, 2, 2} = Group.Replica.Data.replica_stream_head(name, 0, stream_id)
      assert :ok = Group.TestCluster.assert_replica_consistent(name)

      :ok = Group.register(name, third_key, %{seq: 3})
      assert {2, 3, 3} = Group.Replica.Data.replica_stream_head(name, 0, stream_id)

      assert Group.lookup(name, first_key) == {self(), %{seq: 1}}
      assert Group.lookup(name, second_key) == {self(), %{seq: 2}}
      assert Group.lookup(name, third_key) == {self(), %{seq: 3}}
      assert :ok = Group.TestCluster.assert_replica_consistent(name)
    end

    test "a shard restart rebuilds every one-sided materialized index" do
      name = start_single_shard_group(replicated_oplog_max_entries: 16)
      reg_key = "indexes/crash-window/reg/#{System.unique_integer([:positive])}"
      pg_key = "indexes/crash-window/pg/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, reg_key, %{kind: :registry})
      :ok = Group.join(name, pg_key, %{kind: :pg})

      stream_id = Group.Replica.Data.local_stream_id(name, 0, nil)
      generation = Group.Replica.WireProtocol.stream_generation(stream_id)
      epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)

      :ets.delete(Group.Replica.Data.reg_by_pid_table(name, 0), {self(), nil, reg_key})
      :ets.delete(Group.Replica.Data.pg_by_pid_table(name, 0), {self(), nil, pg_key})

      :ets.delete(
        Group.Replica.Data.reg_claim_by_pid_table(name, 0),
        {self(), nil, reg_key, node(), generation, epoch}
      )

      orphan = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> kill_if_alive(orphan) end)

      :ets.insert(
        Group.Replica.Data.reg_by_pid_table(name, 0),
        {{orphan, nil, "indexes/orphan/reg"}, %{}, 0, node()}
      )

      :ets.insert(
        Group.Replica.Data.pg_by_pid_table(name, 0),
        {{orphan, nil, "indexes/orphan/pg"}, %{}, 0, node()}
      )

      :ets.insert(
        Group.Replica.Data.reg_claim_by_pid_table(name, 0),
        {{orphan, nil, "indexes/orphan/claim", node(), generation, epoch}, %{}, 0, 1}
      )

      old_shard = Process.whereis(Group.Replica.shard_name(name, 0))
      Process.exit(old_shard, :kill)

      Group.TestCluster.assert_eventually(fn ->
        new_shard = Process.whereis(Group.Replica.shard_name(name, 0))
        is_pid(new_shard) and new_shard != old_shard
      end)

      :sys.get_state(Group.Replica.shard_name(name, 0))
      assert Group.lookup(name, reg_key) == {self(), %{kind: :registry}}
      assert Group.members(name, pg_key) == [{self(), %{kind: :pg}}]
      assert Group.Replica.Data.registry_lookup_by_pid(name, 0, orphan) == []
      assert Group.Replica.Data.entries_by_pid(name, 0, orphan) == []
      assert :ok = Group.TestCluster.assert_replica_consistent(name)
    end

    test "a shard restart completes an interrupted named-cluster close without retained rows" do
      name = start_single_shard_group(replicated_oplog_max_entries: 16)
      cluster = "close/crash-window/#{System.unique_integer([:positive])}"
      reg_key = "close/crash-window/reg/#{System.unique_integer([:positive])}"
      pg_key = "close/crash-window/pg/#{System.unique_integer([:positive])}"
      remote_route = :"close-crash-window@remote"

      :ok = Group.connect(name, cluster)
      :ok = Group.register(name, reg_key, %{kind: :registry}, cluster: cluster)
      :ok = Group.join(name, pg_key, %{kind: :pg}, cluster: cluster)
      :ok = Group.Replica.Data.add_cluster_node(name, [cluster], remote_route)

      stream_id = Group.Replica.Data.local_stream_id(name, 0, cluster)
      old_epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)
      old_shard = Process.whereis(Group.Replica.shard_name(name, 0))
      :ok = :sys.suspend(old_shard)

      # Group.disconnect/3 closes authority and routing before its request
      # reaches every shard. Hold the durable cleanup message in the shard's
      # mailbox, then model a kill in that exact window.
      assert [{^cluster, ^old_epoch}] =
               Group.Replica.Data.deactivate_local_clusters(name, [cluster])

      assert Group.Replica.Data.closed_local_clusters(name) == [cluster]

      Process.exit(old_shard, :kill)

      Group.TestCluster.assert_eventually(fn ->
        new_shard = Process.whereis(Group.Replica.shard_name(name, 0))

        is_pid(new_shard) and new_shard != old_shard and
          Group.lookup(name, reg_key, cluster: cluster) == nil and
          Group.members(name, pg_key, cluster: cluster) == [] and
          Group.Replica.Data.closed_local_clusters(name) == [] and
          Group.Replica.Data.cluster_nodes(name, cluster) == []
      end)

      assert :ets.lookup(Group.Replica.Data.replica_stream_meta_table(name, 0), stream_id) == []
      assert :ok = Group.TestCluster.assert_replica_consistent(name)
    end

    test "the last close acknowledgement atomically removes all cluster routing" do
      name = start_single_shard_group()
      cluster = "close/terminal-ack/#{System.unique_integer([:positive])}"
      remote_route = :"close-terminal-ack@remote"

      :ok = Group.connect(name, cluster)
      :ok = Group.Replica.Data.add_cluster_node(name, [cluster], remote_route)

      shard = Process.whereis(Group.Replica.shard_name(name, 0))
      :ok = :sys.suspend(shard)

      on_exit(fn -> Group.TestCluster.resume_if_alive(shard) end)

      # Model the durable deactivation caller and the final shard both dying
      # immediately after the final acknowledgement. Once the close marker is
      # gone there is no later recovery hook, so routing must already be gone.
      assert [{^cluster, epoch}] =
               Group.Replica.Data.deactivate_local_clusters_durable(name, [cluster])

      assert is_reference(epoch)

      assert [] =
               Group.Replica.Data.mark_closed_cluster_shard(name, [{cluster, make_ref()}], 0)

      assert Group.Replica.Data.closed_local_clusters(name) == [cluster]

      assert [^cluster] =
               Group.Replica.Data.mark_closed_cluster_shard(name, [{cluster, epoch}], 0)

      assert Group.Replica.Data.closed_local_clusters(name) == []
      assert Group.Replica.Data.cluster_nodes(name, cluster) == []
      assert Group.Replica.Data.clusters_for_node(name, remote_route) == []
    end

    test "delayed restart cleanup cannot remove a reactivated cluster's routes" do
      name = start_single_shard_group()
      cluster = "close/reactivated-cleanup/#{System.unique_integer([:positive])}"
      remote_route = :"close-reactivated-cleanup@remote"

      :ok = Group.connect(name, cluster)
      :ok = Group.Replica.Data.add_cluster_node(name, [cluster], remote_route)

      # Model repair_primary_replica_rows/2 observing this cluster while it was
      # inactive, followed by a concurrent reconnect completing before repair's
      # serialized routing cleanup runs.
      :ok = Group.Replica.Data.remove_clusters(name, [cluster])

      assert Enum.sort(Group.Replica.Data.cluster_nodes(name, cluster)) ==
               Enum.sort([node(), remote_route])

      assert cluster in Group.Replica.Data.clusters_for_node(name, node())
      assert cluster in Group.Replica.Data.clusters_for_node(name, remote_route)
    end

    test "the last expired replica lane atomically removes peer routing" do
      name = start_single_shard_group()
      remote_node = :"terminal-peer-retirement@remote"
      generation = Group.Replica.WireProtocol.new_generation()

      assert {nil, []} =
               Group.Replica.Data.put_remote_replica_info(
                 name,
                 0,
                 remote_node,
                 generation,
                 0,
                 [{nil, generation}]
               )

      :ok =
        Group.Replica.Data.put_remote_view_info(
          name,
          0,
          remote_node,
          generation,
          0,
          0
        )

      assert remote_node in Group.Replica.Data.cluster_nodes(name, nil)
      assert [nil] = Group.Replica.Data.clusters_for_node(name, remote_node)

      # Once this call removes the last persisted lane view, no shard restart
      # can reconstruct an expiry obligation for the peer. Its routing must be
      # gone before the terminal retirement is acknowledged.
      assert :node_retired =
               Group.Replica.Data.expire_remote_replica_lane(name, 0, remote_node)

      assert Group.Replica.Data.cluster_nodes(name, nil) |> Enum.member?(remote_node) == false
      assert Group.Replica.Data.clusters_for_node(name, remote_node) == []
      assert Group.Replica.Data.remote_generation(name, remote_node) == nil
    end

    test "nodedown removes a retired peer's pending authority repair" do
      name = start_single_shard_group()
      remote_node = :"retired-authority-repair@remote"
      shard = Group.Replica.shard_name(name, 0)

      send(shard, {:replica_authority_dirty_local, remote_node})

      assert %{cluster_control_dirty: %{^remote_node => _timestamp}} = :sys.get_state(shard)

      send(shard, {:nodedown, remote_node})

      refute Map.has_key?(:sys.get_state(shard).cluster_control_dirty, remote_node)
    end

    test "delayed peer cleanup cannot remove a rediscovered generation's routes" do
      name = start_single_shard_group()
      remote_node = :"rediscovered-peer-route@remote"
      old_generation = Group.Replica.WireProtocol.new_generation()

      assert {nil, []} =
               Group.Replica.Data.put_remote_replica_info(
                 name,
                 0,
                 remote_node,
                 old_generation,
                 0,
                 [{nil, old_generation}]
               )

      :ok =
        Group.Replica.Data.put_remote_view_info(
          name,
          0,
          remote_node,
          old_generation,
          0,
          0
        )

      assert :node_retired =
               Group.Replica.Data.expire_remote_replica_lane(name, 0, remote_node)

      new_generation = Group.Replica.WireProtocol.new_generation()

      assert {nil, []} =
               Group.Replica.Data.put_remote_replica_info(
                 name,
                 0,
                 remote_node,
                 new_generation,
                 0,
                 [{nil, new_generation}]
               )

      # Model the old expiry/nodedown caller resuming only after rediscovery.
      :ok = Group.Replica.Data.purge_cluster_node(name, remote_node)

      assert remote_node in Group.Replica.Data.cluster_nodes(name, nil)
      assert [nil] = Group.Replica.Data.clusters_for_node(name, remote_node)
      assert Group.Replica.Data.remote_generation(name, remote_node) == new_generation
    end

    test "a lane restart reconstructs retirement from a persisted authority hint" do
      name = :"group_hint_restart_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Group,
         name: name,
         shards: 2,
         log: false,
         replicated_anti_entropy_interval: 25,
         replicated_peer_lease_timeout: 150}
      )

      remote_node = :"hint-restart-retirement@remote"
      old_generation = Group.Replica.WireProtocol.new_generation()

      assert {nil, []} =
               Group.Replica.Data.put_remote_replica_info(
                 name,
                 0,
                 remote_node,
                 old_generation,
                 0,
                 [{nil, old_generation}]
               )

      generation = Group.Replica.WireProtocol.new_generation()

      assert Group.Replica.Data.observe_remote_replica_hint(
               name,
               remote_node,
               generation,
               0
             )

      old_lane = Process.whereis(Group.Replica.shard_name(name, 1))
      monitor = Process.monitor(old_lane)
      Process.exit(old_lane, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^old_lane, :killed}, 5_000

      wait_until(fn ->
        case Process.whereis(Group.Replica.shard_name(name, 1)) do
          pid when is_pid(pid) -> pid != old_lane
          _ -> false
        end
      end)

      wait_until(
        fn ->
          Group.Replica.Data.remote_replica_authority_hint(name, remote_node) == nil
        end,
        2_000
      )

      refute Map.has_key?(
               :sys.get_state(Group.Replica.shard_name(name, 1)).peer_last_seen,
               remote_node
             )
    end
  end

  describe "replica authority snapshots" do
    test "a raced authority hint prevents a partial incremental view install", %{name: name} do
      remote_node = :"incremental-authority-race@remote"
      generation = Group.Replica.WireProtocol.new_generation()
      cluster = "incremental-authority-race/cluster"
      epoch = make_ref()

      assert {nil, []} =
               Group.Replica.Data.put_remote_replica_info(
                 name,
                 0,
                 remote_node,
                 generation,
                 0,
                 [{nil, generation}]
               )

      :ok =
        Group.Replica.Data.put_remote_view_info(
          name,
          0,
          remote_node,
          generation,
          0,
          0
        )

      assert Group.Replica.Data.observe_remote_replica_hint(
               name,
               remote_node,
               generation,
               2
             )

      assert :stale =
               Group.Replica.Data.put_remote_cluster_epochs(
                 name,
                 0,
                 remote_node,
                 generation,
                 0,
                 1,
                 [{cluster, epoch}]
               )

      assert Group.Replica.Data.remote_cluster_epoch(name, remote_node, cluster) == nil

      assert :stale =
               Group.Replica.Data.put_remote_view_info(
                 name,
                 0,
                 remote_node,
                 generation,
                 0,
                 2
               )

      refute Group.Replica.Data.remote_registry_claim_authoritative?(
               name,
               0,
               remote_node,
               generation,
               nil,
               generation
             )
    end

    test "a newer-generation hint atomically rejects an old lane view", %{name: name} do
      remote_node = :"lane-view-generation-race@remote"
      old_generation = Group.Replica.WireProtocol.new_generation()

      assert {nil, []} =
               Group.Replica.Data.put_remote_replica_info(
                 name,
                 0,
                 remote_node,
                 old_generation,
                 0,
                 [{nil, old_generation}]
               )

      new_generation = Group.Replica.WireProtocol.new_generation()
      assert Group.Replica.WireProtocol.generation_newer?(new_generation, old_generation)

      assert Group.Replica.Data.observe_remote_replica_hint(
               name,
               remote_node,
               new_generation,
               0
             )

      assert :stale =
               Group.Replica.Data.put_remote_view_info(
                 name,
                 1,
                 remote_node,
                 old_generation,
                 0,
                 0
               )

      assert Group.Replica.Data.remote_view_generation(name, 1, remote_node) == nil
    end

    test "revision and epoch rows remain coherent during concurrent activation", %{name: name} do
      clusters = for i <- 1..1_000, do: "authority/#{i}"

      activators =
        clusters
        |> Enum.chunk_every(125)
        |> Enum.map(fn chunk ->
          Task.async(fn ->
            Enum.each(chunk, fn cluster ->
              [{^cluster, _epoch}] =
                Group.Replica.Data.activate_local_clusters(name, [cluster])
            end)
          end)
        end)

      for _ <- 1..200 do
        {generation, revision, epochs} =
          Group.Replica.Data.local_replica_authority(name)

        assert {nil, generation} in epochs
        assert revision == Enum.count(epochs, &(not is_nil(elem(&1, 0))))
      end

      Task.await_many(activators, 10_000)

      {generation, 1_000, epochs} = Group.Replica.Data.local_replica_authority(name)
      assert {nil, generation} in epochs
      assert length(epochs) == 1_001
      assert Map.new(epochs) |> map_size() == 1_001
    end
  end

  defp start_single_shard_group(opts \\ []) do
    name = :"test_timeout_group_#{System.unique_integer([:positive])}"
    opts = Keyword.merge([name: name, shards: 1, log: false], opts)
    start_supervised!({Group, opts})
    name
  end

  defp keys_for_shard(cluster, prefix, num_shards, shard, count) do
    1
    |> Stream.iterate(&(&1 + 1))
    |> Stream.map(&"#{prefix}/#{&1}")
    |> Stream.filter(&(Group.Replica.shard_index_for(cluster, &1, num_shards) == shard))
    |> Enum.take(count)
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

  defp replicated_register(cluster, key, pid, meta, _reason, time \\ System.system_time()) do
    {:replicate_registry_batch, [{:register, cluster, key, pid, meta, time, node(pid)}]}
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

  defp replica_ingress_fairness_owner(parent) do
    receive do
      {:write, shard, request} ->
        ref = make_ref()
        send(shard, {:group_local_request, self(), ref, request})
        {reply, calls} = receive_local_write_with_trace(shard, ref, 0)
        send(parent, {:local_write_finished, self(), reply, calls})
        Process.sleep(:infinity)
    end
  end

  defp receive_local_write_with_trace(shard, ref, calls) do
    receive do
      {:trace, ^shard, :call,
       {Group.Replica, :handle_replica_message, [_state, _source_node, _message]}} ->
        receive_local_write_with_trace(shard, ref, calls + 1)

      {:group_local_reply, ^ref, reply} ->
        {reply, calls}
    end
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
