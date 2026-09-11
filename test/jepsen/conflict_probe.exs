# Invoked by the Clojure checker qualification. Load the actual harness without
# starting its TCP server; no copied Owner/Driver logic lives in this probe.
System.put_env("GROUP_JEPSEN_LIBRARY_ONLY", "1")
Code.require_file("node.exs", __DIR__)
Application.ensure_all_started(:group)
Logger.configure(level: :emergency)

defmodule Group.Jepsen.ConflictProbe do
  alias Group.Jepsen.{ConflictEvidence, Driver, EDN}

  def run do
    directory = Path.join(__DIR__, ".cache/conflict-probe-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)

    try do
      {winner, rejected, winner_events} = winner(directory)

      cases = [
        {"historical winner subsequently unregistered and died", true, 1, "jepsen/registry/0",
         winner, false},
        {"victim killed during register", true, 1, "jepsen/registry/0", winner, true},
        {"forged key and rank", false, 99, "nonexistent", %{token: "invented", revision: -100},
         false},
        {"rightful winner killed", false, 99, "jepsen/registry/0", winner, false},
        {"nonexistent winning claim", false, 1, "jepsen/registry/0",
         %{token: "invented", revision: 100}, false},
        {"definitively rejected winning claim", false, 1, "jepsen/registry/0", rejected, false}
      ]

      scenarios =
        Enum.with_index(cases, fn {label, valid, revision, key, meta, pending}, index ->
          events = victim(directory, index, revision, key, meta, pending)

          %{
            label: label,
            valid: valid,
            snapshots: %{
              "n1" => %{conflict_evidence: events},
              "n2" => %{conflict_evidence: winner_events}
            }
          }
        end)

      IO.puts("CONFLICT-PROBE " <> EDN.encode(scenarios))
    after
      File.rm_rf!(directory)
    end
  end

  defp start(directory, label, node_id) do
    {:ok, group} = Group.start_link(name: :jepsen_group, shards: 1, log: false)
    path = Path.join(directory, label)
    {:ok, evidence} = ConflictEvidence.start_link(conflict_evidence_path: path)
    {:ok, driver} = Driver.start_link(index: 0, node_id: node_id, boot_id: label)
    {group, evidence, driver, path}
  end

  defp mutate(driver, operation, revision) do
    GenServer.call(driver, {:mutate, operation, "owner", nil, 0, revision})
  end

  defp winner(directory) do
    {group, evidence, driver, _path} = start(directory, "winner", "n2")
    %{status: :ok, owner: %{token: token}} = mutate(driver, :register, 10)

    %{status: :fail, owner: %{token: rejected}} =
      GenServer.call(driver, {:mutate, :register, "rejected", nil, 0, 100})

    %{status: :ok} = mutate(driver, :unregister, 0)
    %{status: :ok} = GenServer.call(driver, {:kill, "owner"})
    %{status: :ok} = GenServer.call(driver, {:kill, "rejected"})
    events = ConflictEvidence.snapshot()
    GenServer.stop(driver)
    GenServer.stop(evidence)
    Supervisor.stop(group)
    {%{token: token, revision: 10}, %{token: rejected, revision: 100}, events}
  end

  defp victim(directory, index, revision, key, winner, pending) do
    {group, evidence, driver, path} = start(directory, "victim-#{index}", "n1")
    %{status: :ok} = mutate(driver, :join, 0)
    {pid, _token, _monitor, _cached} = :sys.get_state(driver).owners["owner"]
    shard = Group.Replica.shard_for(:jepsen_group, nil, "jepsen/registry/0")

    task =
      if pending do
        :ok = :sys.suspend(shard)
        task = Task.async(fn -> mutate(driver, :register, revision) end)
        wait(fn -> Enum.any?(ConflictEvidence.snapshot(), &(&1.kind == :register)) end)
        task
      else
        %{status: :ok} = mutate(driver, :register, revision)
        nil
      end

    Process.exit(pid, {:group_registry_conflict, key, winner})
    if task, do: Task.await(task)
    wait(fn -> Enum.any?(ConflictEvidence.snapshot(), &(&1.kind == :death)) end)
    if pending, do: :sys.resume(shard)
    events = ConflictEvidence.snapshot()
    [] = GenServer.call(driver, :unexpected_deaths)
    {:ok, []} = GenServer.call(driver, :owner_snapshots)
    GenServer.stop(driver)
    GenServer.stop(evidence)

    # The exact registration/death obligations must survive a recorder restart.
    {:ok, evidence} = ConflictEvidence.start_link(conflict_evidence_path: path)
    ^events = ConflictEvidence.snapshot()
    GenServer.stop(evidence)
    Supervisor.stop(group)
    events
  end

  defp wait(fun, remaining \\ 200)
  defp wait(_fun, 0), do: raise("probe timed out")

  defp wait(fun, remaining) do
    unless fun.() do
      Process.sleep(5)
      wait(fun, remaining - 1)
    end
  end
end

Group.Jepsen.ConflictProbe.run()
