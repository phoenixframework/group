defmodule Group.JepsenQualificationRunnerTest do
  use ExUnit.Case, async: true

  @moduletag :local
  @moduletag tmp_dir: System.pid()

  @corruptions ~w(none unexpected-death internal-index cursor-marker registry-projection terminal-unavailable)

  setup %{tmp_dir: directory} do
    repo = Path.expand("checkout with spaces", directory)
    scripts = Path.join(repo, "test/jepsen")
    bin = Path.expand("bin", directory)
    probes = Path.expand("probes", directory)

    for path <- [scripts, bin, probes], do: File.mkdir_p!(path)
    script = Path.join(scripts, "qualify.exs")
    File.cp!("test/jepsen/qualify.exs", script)

    # Exercise the actual executable runner, replacing only the external
    # mutation/Jepsen commands. Neither Docker nor the real mutation campaign runs.
    for command <- ["mix", "timeout"] do
      path = Path.join(bin, command)
      File.write!(path, command_stub())
      File.chmod!(path, 0o755)
    end

    {:ok, repo: repo, scripts: scripts, script: script, bin: bin, probes: probes}
  end

  test "qualifies the healthy baseline and all five corruptions with bounded commands", context do
    {output, status} = run_qualification(context)
    assert status == 0, output
    assert calls(context) == ["mutations" | @corruptions]
    assert [artifacts] = artifact_directories(context)

    for mode <- @corruptions do
      [cwd, skip_checker, result | args] = invocation(context, mode)
      assert cwd == context.repo
      assert skip_checker == "1"
      assert result == Path.join(artifacts, "#{mode}.result")

      assert args == [
               "--signal=TERM",
               "--kill-after=30",
               "180",
               Path.join(context.scripts, "run.sh"),
               "test",
               "--no-ssh",
               "--nodes",
               "n1,n2,n3",
               "--concurrency",
               "2n",
               "--time-limit",
               "6",
               "--fault-interval",
               "1",
               "--recovery-time",
               "5",
               "--transport",
               "distribution",
               "--scenario",
               "mixed",
               "--corruption",
               mode
             ]

      assert File.read!(Path.join(artifacts, "#{mode}.log")) == "probe log #{mode}\n"
      valid? = mode == "none"
      assert File.read!(result) == "group-qualification-v1\t#{mode}\t#{valid?}\ttrue\n"
    end
  end

  test "a failed mutation baseline propagates without running live qualification", context do
    assert {_, 42} = run_qualification(context, baseline_status: 42)
    assert calls(context) == ["mutations"]
  end

  test "a failing healthy history stops before any corruption", context do
    assert {_, 1} = run_qualification(context, mode: "none", status: 1)
    assert calls(context) == ["mutations", "none"]
  end

  for {label, status, record} <- [
        {"unexpected acceptance", 0, "wrong-validity"},
        {"unqualified rejection", 1, "unqualified"},
        {"wrong corruption", 1, "wrong-mode"},
        {"wrong schema", 1, "wrong-version"},
        {"malformed record", 1, "malformed"},
        {"missing result", 1, "missing"},
        {"timeout with completed evidence", 124, "valid"},
        {"timeout invocation failure", 125, "valid"},
        {"killed process", 137, "missing"},
        {"missing executable", 127, "missing"}
      ] do
    @tag status: status, record: record
    test "rejects #{label} and does not proceed to the next corruption", context do
      assert {_, 1} = run_qualification(context, status: context.status, record: context.record)
      assert calls(context) == ["mutations", "none", "unexpected-death"]
    end
  end

  test "a prior successful run cannot supply a missing result for the next run", context do
    assert {_, 0} = run_qualification(context)
    [first] = artifact_directories(context)
    assert {_, 1} = run_qualification(context, record: "missing")
    assert length(artifact_directories(context)) == 2
    [_, _, result | _] = invocation(context, "unexpected-death")
    refute Path.dirname(result) == first
    refute File.exists?(result)
    assert File.exists?(Path.join(first, "unexpected-death.result"))
  end

  defp run_qualification(context, opts \\ []) do
    System.cmd("elixir", [context.script],
      env: [
        {"PATH", context.bin <> ":" <> System.fetch_env!("PATH")},
        {"ERL_FLAGS", "+S 1:1"},
        {"QUALIFICATION_PROBE_DIR", context.probes},
        {"QUALIFICATION_PROBE_MODE", Keyword.get(opts, :mode, "unexpected-death")},
        {"QUALIFICATION_PROBE_RECORD", Keyword.get(opts, :record, "valid")},
        {"QUALIFICATION_PROBE_STATUS", opts[:status] && to_string(opts[:status])},
        {"QUALIFICATION_BASELINE_STATUS", to_string(Keyword.get(opts, :baseline_status, 0))}
      ],
      stderr_to_stdout: true
    )
  end

  defp calls(context) do
    context.probes |> Path.join("calls") |> File.read!() |> String.split("\n", trim: true)
  end

  defp invocation(context, mode) do
    context.probes |> Path.join("#{mode}.txt") |> File.read!() |> String.split("\n")
  end

  defp artifact_directories(context) do
    Path.wildcard(Path.join(context.scripts, ".cache/qualification.*"))
  end

  defp command_stub do
    ~S"""
    #!/usr/bin/env elixir
    args = System.argv()
    probes = System.fetch_env!("QUALIFICATION_PROBE_DIR")
    mode = if args == ["run", "test/mutation/run.exs"], do: "mutations", else: List.last(args)
    File.write!(Path.join(probes, "calls"), mode <> "\n", [:append])
    result = System.get_env("GROUP_JEPSEN_QUALIFICATION_RESULT", "absent")
    skip = System.get_env("GROUP_JEPSEN_SKIP_CHECKER", "unset")
    File.write!(Path.join(probes, "#{mode}.txt"), Enum.join([File.cwd!(), skip, result | args], "\n"))
    IO.puts("probe log #{mode}")

    if mode == "mutations" do
      System.halt(String.to_integer(System.fetch_env!("QUALIFICATION_BASELINE_STATUS")))
    end

    targeted? = mode == System.fetch_env!("QUALIFICATION_PROBE_MODE")
    status = if mode == "none", do: "0", else: "1"
    status = if targeted?, do: System.get_env("QUALIFICATION_PROBE_STATUS") || status, else: status
    kind = if targeted?, do: System.fetch_env!("QUALIFICATION_PROBE_RECORD"), else: "valid"
    valid? = mode == "none"
    valid? = if kind == "wrong-validity", do: not valid?, else: valid?
    record_mode = if kind == "wrong-mode", do: "different-corruption", else: mode
    version = if kind == "wrong-version", do: "group-qualification-v2", else: "group-qualification-v1"
    record = "#{version}\t#{record_mode}\t#{valid?}\t#{kind != "unqualified"}\n"
    record = if kind == "malformed", do: "not a qualification record\n", else: record
    unless kind == "missing", do: File.write!(result, record)
    System.halt(String.to_integer(status))
    """
  end
end
