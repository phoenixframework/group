defmodule Group.JepsenCleanupDeathsTest do
  use ExUnit.Case, async: false
  @moduletag :local
  @moduletag :tmp_dir
  Code.require_file("jepsen/harness_modules.exs", __DIR__)

  setup %{tmp_dir: tmp_dir} do
    for variable <- ["GROUP_JEPSEN_UNEXPECTED_DEATH_LOG", "GROUP_JEPSEN_PERSISTENT_EVENT_LOG"] do
      previous = System.get_env(variable)
      System.put_env(variable, Path.join(tmp_dir, variable))

      on_exit(fn ->
        if previous, do: System.put_env(variable, previous), else: System.delete_env(variable)
      end)
    end

    start_supervised!({Group, name: :jepsen_group, shards: 1, log: false})

    driver =
      start_supervised!({Group.Jepsen.Driver, [index: 0, node_id: "n1", boot_id: "boot"]})

    for logical <- ["loser", "survivor"] do
      assert %{status: :ok} =
               GenServer.call(driver, {:mutate, :join, logical, nil, 0, 1})
    end

    owners = :sys.get_state(driver).owners
    {loser, _, _, _} = owners["loser"]
    {survivor, survivor_token, _, _} = owners["survivor"]

    on_exit(fn ->
      for pid <- [loser, survivor], Process.alive?(pid), do: Process.exit(pid, :kill)
    end)

    %{driver: driver, loser: loser, survivor: survivor, token: survivor_token}
  end

  for request <- [{:drop_cluster, "red"}, :owner_snapshots] do
    @request request
    test "monitored conflict death during #{inspect(request)} preserves other owners", ctx do
      :ok = :sys.suspend(ctx.loser)
      task = Task.async(fn -> GenServer.call(ctx.driver, @request, 15_000) end)
      expected = if @request == :owner_snapshots, do: :snapshot, else: @request

      Group.LocalCase.wait_until(fn ->
        {:messages, messages} = Process.info(ctx.loser, :messages)
        Enum.any?(messages, &match?({:"$gen_call", _, ^expected}, &1))
      end)

      Process.exit(
        ctx.loser,
        {:group_registry_conflict, "jepsen/registry/0", %{token: "winner", revision: 2}}
      )

      response = Task.await(task, 15_000)

      if @request == :owner_snapshots,
        do: assert(match?({:ok, [_]}, response)),
        else: assert(response == :ok)

      assert Process.alive?(ctx.driver)
      assert Process.alive?(ctx.survivor)

      assert {:ok, [%{token: token, memberships: [_]}]} =
               GenServer.call(ctx.driver, :owner_snapshots)

      assert token == ctx.token
      state = :sys.get_state(ctx.driver)
      assert Map.keys(state.owners) == ["survivor"]
      assert map_size(state.monitors) == 1
      assert state.unexpected_deaths == []

      Group.LocalCase.wait_until(fn ->
        Group.members(:jepsen_group, "jepsen/pg/0") ==
          [{ctx.survivor, %{token: ctx.token, revision: 1}}]
      end)
    end
  end

  test "death queued before snapshot is consumed without replacing the Driver", ctx do
    :ok = :sys.suspend(ctx.driver)
    task = Task.async(fn -> GenServer.call(ctx.driver, :owner_snapshots) end)

    Group.LocalCase.wait_until(fn ->
      {:messages, messages} = Process.info(ctx.driver, :messages)
      Enum.any?(messages, &match?({:"$gen_call", _, :owner_snapshots}, &1))
    end)

    ref = Process.monitor(ctx.loser)

    Process.exit(
      ctx.loser,
      {:group_registry_conflict, "jepsen/registry/0", %{token: "winner", revision: 2}}
    )

    assert_receive {:DOWN, ^ref, :process, _, _}
    :ok = :sys.resume(ctx.driver)
    assert {:ok, [%{token: token}]} = Task.await(task)
    assert token == ctx.token
    assert Process.alive?(ctx.driver)
  end

  test "unexpected owner death still leaves failure evidence", ctx do
    :ok = :sys.suspend(ctx.loser)
    task = Task.async(fn -> GenServer.call(ctx.driver, {:drop_cluster, "red"}, 15_000) end)

    Group.LocalCase.wait_until(fn ->
      {:messages, messages} = Process.info(ctx.loser, :messages)
      Enum.any?(messages, &match?({:"$gen_call", _, {:drop_cluster, "red"}}, &1))
    end)

    Process.exit(ctx.loser, :implementation_bug)
    assert :ok = Task.await(task, 15_000)

    assert [%{reason: ":implementation_bug"}] =
             GenServer.call(ctx.driver, :unexpected_deaths)

    assert Process.alive?(ctx.survivor)
    assert Process.alive?(ctx.driver)
  end

  test "a live owner's timeout is reported without deleting any owner state", ctx do
    :ok = :sys.suspend(ctx.loser)

    assert {:error, {"loser", _, {:timeout, _}, _}} =
             GenServer.call(ctx.driver, {:drop_cluster, "red"}, 15_000)

    assert Process.alive?(ctx.driver)
    assert Process.alive?(ctx.loser)
    assert Process.alive?(ctx.survivor)
    state = :sys.get_state(ctx.driver)
    assert map_size(state.owners) == 2
    assert map_size(state.monitors) == 2
    assert state.unexpected_deaths == []
    :ok = :sys.resume(ctx.loser)
    assert {:ok, snapshots} = GenServer.call(ctx.driver, :owner_snapshots)
    assert length(snapshots) == 2
  end
end
