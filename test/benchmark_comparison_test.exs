defmodule Group.BenchmarkComparisonTest do
  use ExUnit.Case, async: true

  @moduletag :local
  @moduletag :tmp_dir

  setup %{tmp_dir: directory} do
    baseline = Path.expand("old library", directory)
    candidate = Path.expand("new library", directory)
    harness = Path.join(candidate, "priv/bench")
    bin = Path.join(directory, "bin")
    calls = Path.expand("calls", directory)
    reports = Path.expand("reports", directory)
    File.mkdir_p!(Path.join(baseline, "priv/bench"))
    File.mkdir_p!(harness)
    File.mkdir_p!(bin)

    script = Path.join(harness, "compare_distributed.sh")
    File.cp!("priv/bench/compare_distributed.sh", script)

    File.write!(Path.join(baseline, "priv/bench/run_distributed.sh"), "exit 99\n")

    File.write!(Path.join(harness, "run_distributed.sh"), """
    set -eu
    printf '%s|%s|%s|%s\\n' "$0" "$GROUP_BENCH_GROUP_PATH" "$MIX_BUILD_PATH" "$*" >> "$BENCH_CALL_LOG"
    mkdir -p "$MIX_BUILD_PATH"
    test ! -e "$MIX_BUILD_PATH/library"
    printf '%s\\n' "$GROUP_BENCH_GROUP_PATH" > "$MIX_BUILD_PATH/library"
    if [ "$GROUP_BENCH_GROUP_PATH" = "${BENCH_FAIL_LIBRARY:-}" ]; then exit 23; fi
    printf 'measured %s\\n' "$GROUP_BENCH_GROUP_PATH"
    """)

    timeout = Path.join(bin, "timeout")

    File.write!(timeout, """
    #!/bin/sh
    test "$1" = 20m || exit 98
    shift
    exec "$@"
    """)

    File.chmod!(timeout, 0o755)

    {:ok,
     baseline: baseline,
     candidate: candidate,
     harness: harness,
     script: script,
     reports: reports,
     calls: calls,
     env: [
       {"PATH", Path.expand(bin) <> ":" <> System.fetch_env!("PATH")},
       {"BENCH_CALL_LOG", calls},
       {"BENCH_FAIL_LIBRARY", nil}
     ]}
  end

  test "both libraries use the candidate harness and separate clean builds", context do
    assert {_, 0} = compare(context)
    assert [baseline, candidate] = read_calls(context)
    [baseline_harness, baseline_library, baseline_build, baseline_args] = baseline
    [candidate_harness, candidate_library, candidate_build, candidate_args] = candidate
    expected_harness = Path.join(context.harness, "run_distributed.sh")
    assert baseline_harness == expected_harness
    assert candidate_harness == expected_harness
    assert baseline_library == context.baseline
    assert candidate_library == context.candidate
    assert baseline_args == "--shards 4"
    assert candidate_args == baseline_args
    refute baseline_build == candidate_build

    assert File.read!(Path.join(baseline_build, "library")) == context.baseline <> "\n"
    assert File.read!(Path.join(candidate_build, "library")) == context.candidate <> "\n"

    for revision <- [:baseline, :candidate] do
      log = File.read!(Path.join(context.reports, "distributed-#{revision}.log"))
      assert log == "measured #{context[revision]}\n"
    end

    # A second comparison must not reuse stale artifacts from either revision.
    assert {_, 0} = compare(context)
    builds = Enum.map(read_calls(context), &Enum.at(&1, 2))
    assert length(Enum.uniq(builds)) == 4
  end

  for failure <- [:baseline, :candidate] do
    @tag failure: failure
    test "a failing #{failure} propagates through tee", context do
      env =
        List.keyreplace(
          context.env,
          "BENCH_FAIL_LIBRARY",
          0,
          {"BENCH_FAIL_LIBRARY", context[context.failure]}
        )

      context = %{context | env: env}
      assert {_, 23} = compare(context)
      assert length(read_calls(context)) == if(context.failure == :baseline, do: 1, else: 2)
    end
  end

  defp compare(context) do
    System.cmd(
      "bash",
      [context.script, context.baseline, context.candidate, context.reports, "--shards", "4"],
      env: context.env,
      stderr_to_stdout: true
    )
  end

  defp read_calls(context) do
    context.calls
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(&String.split(&1, "|"))
  end
end
