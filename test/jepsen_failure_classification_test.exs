defmodule Group.JepsenFailureClassificationTest do
  use ExUnit.Case, async: false
  @moduletag :local
  Code.require_file("jepsen/harness_modules.exs", __DIR__)

  defmodule API do
    def register(_, _, %{revision: revision}, _) do
      case revision do
        0 -> {:error, :taken}
        1 -> raise "implementation bug"
        2 -> throw(:implementation_bug)
        3 -> :erlang.error(:implementation_bug)
        4 -> exit({:timeout, {GenServer, :call, [:replica, :write, 5_000]}})
        5 -> {:error, :invented_error}
      end
    end
  end

  test "actual Group admission and ownership faults retain stable codes" do
    start_supervised!({Group, name: :jepsen_group, shards: 1, log: false})

    driver =
      start_supervised!({Group.Jepsen.Driver, [index: 0, node_id: "n1", boot_id: "boot"]})

    assert %{status: :fail, code: :not_connected} =
             GenServer.call(driver, {:mutate, :register, "one", "closed", 0, 0})

    assert %{status: :fail, code: :not_owned} =
             GenServer.call(driver, {:mutate, :unregister, "one", nil, 0, 0})

    assert %{status: :ok} =
             GenServer.call(driver, {:mutate, :register, "one", nil, 0, 0})

    assert %{status: :fail, code: :taken} =
             GenServer.call(driver, {:mutate, :register, "two", nil, 0, 0})

    for owner <- ["one", "two"], do: GenServer.call(driver, {:kill, owner})
  end

  @tag :tmp_dir
  test "actual Owner/Driver boundary preserves unexpected failures independently of owner life",
       %{tmp_dir: tmp_dir} do
    old = System.get_env("GROUP_JEPSEN_UNEXPECTED_DEATH_LOG")
    path = Path.join(tmp_dir, "evidence")
    System.put_env("GROUP_JEPSEN_UNEXPECTED_DEATH_LOG", path)

    on_exit(fn ->
      if old,
        do: System.put_env("GROUP_JEPSEN_UNEXPECTED_DEATH_LOG", old),
        else: System.delete_env("GROUP_JEPSEN_UNEXPECTED_DEATH_LOG")
    end)

    driver =
      start_supervised!(
        {Group.Jepsen.Driver, [index: 0, node_id: "n1", boot_id: "boot", api: API]}
      )

    mutate = fn revision ->
      GenServer.call(driver, {:mutate, :register, "owner", nil, 0, revision})
    end

    assert %{status: :fail, code: :taken} = mutate.(0)
    assert %{status: :unknown, code: :indeterminate} = mutate.(4)
    refute File.exists?(path)

    for revision <- [1, 2, 3, 5] do
      assert %{status: :fail, code: :unexpected, error: %{reason: reason}} = mutate.(revision)
      assert reason != ""
    end

    assert :ok =
             GenServer.call(driver, {:kill, "owner"})
             |> then(fn
               %{status: :ok} -> :ok
             end)

    assert Process.alive?(driver)
    assert length(File.read!(path) |> String.split("\n", trim: true)) == 4
    assert {:ok, []} = GenServer.call(driver, :owner_snapshots)
  end
end
