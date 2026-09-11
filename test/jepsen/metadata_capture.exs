System.put_env("GROUP_JEPSEN_LIBRARY", "1")
Code.require_file("node.exs", __DIR__)

alias Group.Jepsen.{Driver, EDN, Snapshot}
alias Group.Replica.Data

{:ok, _} = Group.Jepsen.Transport.Stats.start_link([])

{:ok, _} =
  Group.start_link(
    name: :jepsen_group,
    shards: 1,
    log: false,
    resolve_registry_conflict: {Group.Jepsen.ConflictResolver, :resolve, []}
  )

{:ok, _} = Group.Jepsen.Driver.Supervisor.start_link(node_id: "n1", boot_id: "capture")

for revision <- 1..2, operation <- [:register, :join] do
  %{status: :ok} = Driver.mutate(operation, "owner", nil, 0, revision)
end

capture = fn -> Snapshot.capture("n1", "capture", 1, [], []).snapshot end
healthy = capture.()
[owner] = healthy.owners
expected = %{token: owner.token, revision: 2}
^expected = healthy.registry["root"][0]
[^expected] = healthy.pg["root"][0]
[%{revision: 2}] = owner.registrations
[%{revision: 2}] = owner.memberships

# Corrupt all materialized indexes coherently: the old internal projection
# invariant remains healthy, so only the independent owner/public comparison
# can catch lost, missing or wrong acknowledged metadata.
tables = [
  Data.reg_by_key_table(:jepsen_group, 0),
  Data.reg_by_pid_table(:jepsen_group, 0),
  Data.reg_claim_by_key_table(:jepsen_group, 0),
  Data.reg_claim_by_pid_table(:jepsen_group, 0),
  Data.pg_by_key_table(:jepsen_group, 0),
  Data.pg_by_pid_table(:jepsen_group, 0)
]

originals = Map.new(tables, &{&1, :ets.tab2list(&1)})

corruptions =
  for meta <- [
        %{token: owner.token, revision: 1},
        %{token: owner.token},
        %{token: owner.token, revision: "2"},
        %{token: "wrong-owner", revision: 2},
        %{token: owner.token, revision: 2, extra: true}
      ] do
    Enum.each(originals, fn {table, rows} ->
      changed =
        Enum.map(rows, fn row ->
          row
          |> Tuple.to_list()
          |> Enum.map(fn value -> if value == expected, do: meta, else: value end)
          |> List.to_tuple()
        end)

      :ets.insert(table, changed)
    end)

    result = capture.()
    true = result.internal.healthy
    result
  end

Enum.each(originals, fn {table, rows} -> :ets.insert(table, rows) end)
%{status: :ok} = Driver.kill("owner")
%{status: :ok, owner: reincarnated} = Driver.mutate(:join, "owner", nil, 0, 2)
false = reincarnated.token == owner.token

File.write!(hd(System.argv()), EDN.encode(%{healthy: healthy, corruptions: corruptions}))
