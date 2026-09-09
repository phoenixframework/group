defmodule Group.ClusterLeasePropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Group.PropertyFixture
  alias Group.Replica.Data
  alias Group.TestCluster

  @name :lease_property
  @target "leased"
  @other "other"
  @key "interest/key"

  property "expiry follows remaining cluster-local interest, not connected connect calls" do
    check all(
            shards <- member_of([1, 2, 4]),
            ttl <- integer(60_000..120_000),
            repeated_ttls <- list_of(integer(1..240_000), min_length: 1, max_length: 5),
            interests <-
              map(
                list_of(
                  tuple(
                    {member_of([@target, @other, nil]), member_of([:registry, :pg, :monitor])}
                  ),
                  max_length: 18
                ),
                &Enum.uniq/1
              ),
            max_runs: 100
          ) do
      with_group(@name, [shards: shards, log: false], fn actors ->
        owner = actors[0]
        assert :ok = Group.connect(@name, @target, ttl: ttl)
        assert :ok = Group.connect(@name, @other)
        manager = Group.ClusterLease.lease_name(@name)
        :sys.get_state(manager)

        assert {^ttl, deadline} = Data.cluster_lease(@name, @target)
        # Make any refresh observable even when multiple calls share a clock tick.
        sentinel = deadline + 3_600_000
        Data.put_cluster_lease(@name, @target, ttl, sentinel)

        for repeated_ttl <- [ttl | repeated_ttls] do
          assert :ok = Group.connect(@name, @target, ttl: repeated_ttl)
          assert Data.cluster_lease(@name, @target) == {ttl, sentinel}
          assert :ok = Group.connect(@name, @other, ttl: repeated_ttl)
          assert Data.cluster_lease(@name, @other) == nil
        end

        for interest <- interests, do: add_interest(owner, interest)
        sweep_and_assert(interests, ttl, owner)

        # Generated list order also varies removal order. Sweep after each
        # removal, checking combinations rather than one interest type at a time.
        Enum.reduce(interests, interests, fn interest, remaining ->
          remove_interest(owner, interest)
          remaining = List.delete(remaining, interest)

          if Group.connected?(@name, @target) do
            sweep_and_assert(remaining, ttl, owner)
          else
            assert Data.cluster_lease(@name, @target) == nil
            assert_interests(remaining, owner)
          end

          remaining
        end)

        refute Group.connected?(@name, @target)
        assert Data.cluster_lease(@name, @target) == nil

        # A disconnected cluster may acquire a new lease; a still-connected
        # plain cluster above may not. Reconnect must not resurrect old interest.
        new_ttl = List.last(repeated_ttls) + 60_000
        assert :ok = Group.connect(@name, @target, ttl: new_ttl)
        assert {^new_ttl, _} = Data.cluster_lease(@name, @target)
        :sys.get_state(manager)
        sweep_and_assert([], new_ttl, owner)
      end)
    end
  end

  defp sweep_and_assert(interests, ttl, owner) do
    before_sweep = System.monotonic_time(:millisecond)
    TestCluster.do_expire_cluster_lease_and_force_sweep(@name, @target)
    after_sweep = System.monotonic_time(:millisecond)
    active? = Enum.any?(interests, fn {cluster, _kind} -> cluster == @target end)

    assert Group.connected?(@name, @target) == active?

    if active? do
      assert {^ttl, deadline} = Data.cluster_lease(@name, @target)
      assert deadline >= before_sweep + ttl
      assert deadline <= after_sweep + ttl
    else
      assert Data.cluster_lease(@name, @target) == nil
    end

    assert_interests(interests, owner)
    assert :ok = TestCluster.assert_ets_consistent(@name)
  end

  defp assert_interests(interests, owner) do
    # Interest in nil/other must neither keep the target alive nor be purged.
    assert Group.connected?(@name, @other)

    for cluster <- [nil, @target, @other] do
      registry = if {cluster, :registry} in interests, do: {owner, %{}}, else: nil
      pg = if {cluster, :pg} in interests, do: [{owner, %{}}], else: []
      assert Group.lookup(@name, @key, cluster: cluster) == registry
      assert Group.members(@name, @key, cluster: cluster) == pg

      subscriptions = Registry.keys(Group.registry_name(@name), owner)
      assert {@name, cluster, :all} in subscriptions == {cluster, :monitor} in interests
    end
  end

  defp add_interest(owner, {cluster, kind}) do
    assert :ok =
             in_process(owner, fn ->
               case kind do
                 :registry -> Group.register(@name, @key, %{}, cluster: cluster)
                 :pg -> Group.join(@name, @key, %{}, cluster: cluster)
                 :monitor -> Group.monitor(@name, :all, cluster: cluster)
               end
             end)
  end

  defp remove_interest(owner, {cluster, kind}) do
    assert :ok =
             in_process(owner, fn ->
               case kind do
                 :registry -> Group.unregister(@name, @key, cluster: cluster)
                 :pg -> Group.leave(@name, @key, cluster: cluster)
                 :monitor -> Group.demonitor(@name, :all, cluster: cluster)
               end
             end)
  end
end
