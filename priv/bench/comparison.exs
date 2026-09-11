# Use the same harness and Benchee version for both revisions. Only the Group
# path changes; this also works when the baseline predates this harness.
root = System.get_env("GROUP_BENCH_ROOT") || Path.expand("../..", __DIR__)
Mix.install([{:group, path: root}, {:benchee, "== 1.5.1"}])

{:ok, supervisor} = Group.start_link(name: :comparison, shards: 8, log: false)
parent = self()

members =
  for _ <- 1..128 do
    spawn_link(fn ->
      :ok = Group.join(:comparison, "members", %{})
      send(parent, {:ready, self()})

      receive do
        :stop -> :ok
      end
    end)
  end

for pid <- members do
  receive do
    {:ready, ^pid} -> :ok
  after
    5_000 -> raise "benchmark member did not become ready"
  end
end

:ok = Group.register(:comparison, "lookup", %{value: 1})
output = System.get_env("GROUP_BENCH_OUTPUT", "comparison.benchee")
baseline = System.get_env("GROUP_BENCH_BASELINE")

try do
  Benchee.run(
    %{
      "lookup" => fn -> Group.lookup(:comparison, "lookup") end,
      "members/128" => fn -> Group.members(:comparison, "members") end,
      "register/unregister" => fn ->
        :ok = Group.register(:comparison, "registry-cycle", %{})
        :ok = Group.unregister(:comparison, "registry-cycle")
      end,
      "join/leave" => fn ->
        :ok = Group.join(:comparison, "pg-cycle", %{})
        :ok = Group.leave(:comparison, "pg-cycle")
      end
    },
    time: 3,
    warmup: 1,
    memory_time: 1,
    save: [path: output, tag: System.get_env("GROUP_BENCH_TAG", "candidate")],
    load: if(baseline, do: [baseline], else: [])
  )
after
  for pid <- members, do: send(pid, :stop)
  Supervisor.stop(supervisor)
end
