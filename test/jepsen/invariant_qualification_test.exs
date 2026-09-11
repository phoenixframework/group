# MIX_ENV=test mix run --no-start test/jepsen/invariant_qualification_test.exs
# Compile only the actual oracle modules, not the node entrypoint. Redirect
# the injection marker into the workspace so this probe needs no Docker or
# machine-global files.
ExUnit.start()

defmodule Group.Jepsen.InvariantQualificationTest do
  use ExUnit.Case, async: false

  alias Group.Jepsen.Invariant
  alias Group.Replica.Data

  @compile {:no_warn_undefined, [Group.Jepsen.Invariant, Group.Jepsen.EDN]}

  setup_all do
    work =
      Path.expand(
        "../../tmp/invariant-qualification-#{System.unique_integer([:positive])}",
        __DIR__
      )

    File.mkdir_p!(work)
    marker = Path.join(work, "cursor-marker")
    on_exit(fn -> File.rm_rf!(work) end)
    {:ok, _} = Application.ensure_all_started(:group)

    {:__block__, _, expressions} =
      __DIR__
      |> Path.join("node.exs")
      |> File.read!()
      |> Code.string_to_quoted!()

    modules =
      Enum.filter(expressions, fn
        {:defmodule, _, [{:__aliases__, _, [:Group, :Jepsen, name]}, _]} ->
          name in [:InvariantViolation, :Invariant, :ConflictResolver, :EDN]

        _ ->
          false
      end)

    ast =
      Macro.prewalk({:__block__, [], modules}, fn
        "/tmp/group-jepsen-cursor-marker-corruption" -> marker
        node -> node
      end)

    Code.compile_quoted(ast)
    {:ok, marker: marker}
  end

  setup %{marker: marker} do
    File.rm(marker)
    start_supervised!({Group, name: :jepsen_group, shards: 1, log: false})
    :ok
  end

  test "arming with no remote cursor reports no injection or invariant evidence", %{
    marker: marker
  } do
    File.write!(marker, "enabled\n")
    snapshot = Invariant.snapshot([])
    refute snapshot.healthy
    assert snapshot.snapshot_staging_count == -1
    assert snapshot.injected_corruptions == []
    assert snapshot.failed_invariants == []
  end

  test "a real marker insertion reports its exact invariant", %{marker: marker} do
    File.write!(marker, "enabled\n")
    :ets.insert(Data.replica_cursor_table(:jepsen_group, 0), {:probe_stream, 1})
    snapshot = Invariant.snapshot([])
    refute snapshot.healthy
    assert snapshot.injected_corruptions == [:"cursor-marker"]
    assert snapshot.failed_invariants == [:cursor_snapshot_marker]
    assert Group.Jepsen.EDN.encode(snapshot) =~ ":cursor-snapshot-marker"
  end

  test "an unrelated index failure cannot masquerade as a cursor marker" do
    :ets.insert(
      Data.reg_by_pid_table(:jepsen_group, 0),
      {{self(), nil, "qualification-probe"}, %{}, 0, node()}
    )

    snapshot = Invariant.snapshot([])
    refute snapshot.healthy
    assert snapshot.injected_corruptions == []
    assert snapshot.failed_invariants == [:registry_dual_indexes]
  end
end
