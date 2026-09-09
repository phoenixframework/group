defmodule GroupBench.HelpersTest do
  use ExUnit.Case, async: true

  alias GroupBench.Helpers

  @moduletag :capture_log

  setup do
    %{name: :"bench_test_#{System.unique_integer([:positive])}"}
  end

  test "workers retain valid entries during a scenario and all stop before it returns", %{
    name: name
  } do
    {pool, pids} =
      Helpers.with_group([name: name, shards: 2], fn pool ->
        :ok = Group.monitor(name, :all)

        {us, pids} =
          Helpers.run_workers(
            pool,
            3,
            fn i -> Group.register(name, "key-#{i}", %{i: i}) end,
            fn pids ->
              Helpers.await_registered_events(name, nil, entries(pids))
            end
          )

        assert is_integer(us)
        assert Enum.all?(pids, &Process.alive?/1)
        assert :ok = Helpers.verify_registry(name, entries(pids))
        {pool, pids}
      end)

    refute Process.alive?(pool)
    refute Enum.any?(pids, &Process.alive?/1)
  end

  test "registry verification uses the selected cluster", %{name: name} do
    Helpers.with_group([name: name, shards: 1], fn pool ->
      :ok = Group.connect(name, "game")
      :ok = Group.register(name, "default", %{})
      :ok = Group.monitor(name, :all, cluster: "game")

      {_, pids} =
        Helpers.run_workers(
          pool,
          3,
          fn i -> Group.register(name, "key-#{i}", %{i: i}, cluster: "game") end,
          fn pids -> Helpers.await_registered_events(name, "game", entries(pids)) end
        )

      assert :ok = Helpers.verify_registry(name, entries(pids), cluster: "game")
      assert :ok = Helpers.verify_registry(name, [{"default", self(), %{}}])
    end)
  end

  test "a failed scenario also stops its workers", %{name: name} do
    parent = self()

    assert_raise RuntimeError, "scenario failed", fn ->
      Helpers.with_group([name: name, shards: 1], fn pool ->
        {_, pids} = Helpers.run_workers(pool, 3, fn _ -> :ok end)
        send(parent, {:resources, pool, pids})
        raise "scenario failed"
      end)
    end

    assert_receive {:resources, pool, pids}
    refute Process.alive?(pool)
    refute Enum.any?(pids, &Process.alive?/1)
  end

  test "unsuccessful operations abort the scenario instead of counting as completed work", %{
    name: name
  } do
    parent = self()

    assert_raise RuntimeError, ~r/returned \{:error, :rejected\}, expected :ok/, fn ->
      Helpers.with_group([name: name, shards: 1], fn pool ->
        Helpers.run_workers(pool, 1, fn _ ->
          send(parent, {:worker, self()})
          {:error, :rejected}
        end)
      end)
    end

    assert_receive {:worker, pid}
    refute Process.alive?(pid)
  end

  test "crashed operations fail without waiting for the completion timeout", %{name: name} do
    assert_raise RuntimeError, ~r/exited: :broken/, fn ->
      Helpers.with_group([name: name, shards: 1], fn pool ->
        Helpers.run_workers(pool, 1, fn _ -> exit(:broken) end)
      end)
    end
  end

  test "timed-out workers are stopped", %{name: name} do
    parent = self()

    assert_raise RuntimeError, "timed out waiting for 1 benchmark workers", fn ->
      Helpers.with_group([name: name, shards: 1], fn pool ->
        send(parent, {:pool, pool})

        Helpers.run_workers(
          pool,
          1,
          fn _ -> Process.sleep(:infinity) end,
          fn _ -> :ok end,
          1
        )
      end)
    end

    assert_receive {:pool, pool}
    refute Process.alive?(pool)
  end

  test "exact membership verification detects wrong metadata", %{name: name} do
    Helpers.with_group([name: name, shards: 1], fn pool ->
      {_, [pid]} = Helpers.run_workers(pool, 1, fn _ -> Group.join(name, "room", %{v: 1}) end)
      assert :ok = Helpers.verify_members(name, [{"room", pid, %{v: 1}}])

      assert_raise RuntimeError, ~r/unexpected memberships/, fn ->
        Helpers.verify_members(name, [{"room", pid, %{v: 2}}])
      end
    end)
  end

  test "exact registry verification rejects a wrong owner even when the count matches", %{
    name: name
  } do
    Helpers.with_group([name: name, shards: 1], fn pool ->
      {_, [_pid]} = Helpers.run_workers(pool, 1, fn _ -> Group.register(name, "key", %{}) end)

      assert_raise RuntimeError, ~r/unexpected registration/, fn ->
        Helpers.verify_registry(name, [{"key", self(), %{}}])
      end
    end)
  end

  test "every timed sample must pass result validation" do
    assert_raise RuntimeError, "invalid sample", fn ->
      Helpers.collect_samples(1, fn -> :wrong end, fn
        :ok -> :ok
        _ -> raise "invalid sample"
      end)
    end

    assert [sample] = Helpers.collect_samples(1, fn -> :ok end)
    assert is_integer(sample)
  end

  test "missing registration events fail rather than returning a partial result", %{name: name} do
    assert_raise RuntimeError, "timed out waiting for 1 registration events", fn ->
      Helpers.await_registered_events(name, nil, [{"key", self(), %{}}], 1)
    end
  end

  test "duplicate events cannot substitute for missing events", %{name: name} do
    event = event(name)
    send(self(), {:group, [event, event], %{name: name}})

    assert_raise RuntimeError, ~r/duplicate registration event/, fn ->
      Helpers.await_registered_events(name, nil, [{"key", self(), %{}}, {"other", self(), %{}}])
    end
  end

  test "extra event batches are rejected even after every expected event arrived", %{name: name} do
    event = event(name)
    send(self(), {:group, [event], %{name: name}})
    send(self(), {:group, [event], %{name: name}})

    assert_raise RuntimeError, ~r/duplicate registration event/, fn ->
      Helpers.await_registered_events(name, nil, [{"key", self(), %{}}])
    end
  end

  for {field, value} <- [
        key: "wrong",
        pid: :wrong,
        meta: %{wrong: true},
        cluster: "wrong",
        supervisor: :wrong,
        type: :unregistered,
        reason: :wrong,
        previous_meta: %{}
      ] do
    @tag field: field, value: value
    test "wrong #{field} invalidates registration events", %{
      name: name,
      field: field,
      value: value
    } do
      send(self(), {:group, [Map.put(event(name), field, value)], %{name: name}})

      assert_raise RuntimeError, ~r/unexpected/, fn ->
        Helpers.await_registered_events(name, nil, [{"key", self(), %{}}])
      end
    end
  end

  test "named-cluster events may arrive in any batch order", %{name: name} do
    first = %{event(name) | cluster: "game"}
    second = %{first | key: "other"}
    send(self(), {:group, [second], %{name: name}})
    send(self(), {:group, [first], %{name: name}})

    assert :ok =
             Helpers.await_registered_events(
               name,
               "game",
               [{"key", self(), %{}}, {"other", self(), %{}}]
             )
  end

  defp entries(pids) do
    pids
    |> Enum.with_index(1)
    |> Enum.map(fn {pid, i} -> {"key-#{i}", pid, %{i: i}} end)
  end

  defp event(name) do
    %Group.Event{supervisor: name, type: :registered, key: "key", pid: self(), meta: %{}}
  end
end
