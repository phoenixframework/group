defmodule Group.Jepsen.Qualification do
  @moduledoc false

  # An intentionally corrupted history must be invalid, but still demonstrate
  # that its intended corruption was injected and detected by the checker.
  @checks [
    {"none", true},
    {"unexpected-death", false},
    {"internal-index", false},
    {"cursor-marker", false},
    {"registry-projection", false},
    {"terminal-unavailable", false}
  ]

  def run do
    repo = Path.expand("../..", __DIR__)
    cache = Path.join(__DIR__, ".cache")
    File.mkdir_p!(cache)
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
    artifacts = Path.join(cache, "qualification.#{suffix}")
    File.mkdir!(artifacts)
    File.chmod!(artifacts, 0o700)
    IO.puts("qualification artifacts: #{artifacts}")

    {_output, status} =
      System.cmd("mix", ["run", "test/mutation/run.exs"],
        cd: repo,
        into: IO.stream(),
        stderr_to_stdout: true
      )

    if status != 0, do: System.halt(status)

    Enum.each(@checks, fn {corruption, expected_valid?} ->
      qualify!(repo, artifacts, corruption, expected_valid?)
    end)

    IO.puts("mutation and live checker qualification passed")
  end

  defp qualify!(repo, artifacts, corruption, expected_valid?) do
    log = Path.join(artifacts, "#{corruption}.log")
    result = Path.join(artifacts, "#{corruption}.result")

    # Keep the existing process-tree deadline and TERM/KILL grace period.
    # Streaming output to disk avoids retaining a live history on this VM's heap.
    {_output, status} =
      System.cmd(
        "timeout",
        [
          "--signal=TERM",
          "--kill-after=30",
          "180",
          Path.join(__DIR__, "run.sh"),
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
          corruption
        ],
        cd: repo,
        env: [
          {"GROUP_JEPSEN_SKIP_CHECKER", "1"},
          {"GROUP_JEPSEN_QUALIFICATION_RESULT", result}
        ],
        into: File.stream!(log, [:write, :binary]),
        stderr_to_stdout: true
      )

    expected_status = if expected_valid?, do: 0, else: 1

    unless status == expected_status do
      raise "#{corruption}: expected exit #{expected_status}, got #{status}; see #{log}"
    end

    expected_record = "group-qualification-v1\t#{corruption}\t#{expected_valid?}\ttrue\n"

    unless File.read!(result) == expected_record do
      raise "#{corruption}: missing or mismatched checker evidence; see #{result} and #{log}"
    end

    IO.puts("qualified #{corruption} (#{log})")
  end
end

Group.Jepsen.Qualification.run()
