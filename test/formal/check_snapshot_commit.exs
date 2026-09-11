defmodule Group.Formal.SnapshotCommitQualification do
  @moduledoc false

  def run do
    directory = __DIR__
    spec = Path.join(directory, "SnapshotAssembly.tla")
    config = Path.join(directory, "SnapshotAssembly.cfg")
    check = Path.join(directory, "check.sh")

    run_check!(check, spec, config, 0)

    source = File.read!(spec)
    guard = "IN IF stagedCommitted /\\ nextChunks = Chunks"

    unless length(:binary.matches(source, guard)) == 1 do
      raise "snapshot commit mutation must match exactly one installation guard"
    end

    artifacts =
      Path.expand(
        "../../tmp/formal/snapshot-commit-#{System.pid()}-#{System.unique_integer([:positive])}",
        directory
      )

    File.mkdir_p!(artifacts)
    mutant = Path.join(artifacts, "SnapshotAssembly.tla")
    mutant_config = Path.join(artifacts, "SnapshotAssembly.cfg")
    File.write!(mutant, String.replace(source, guard, "IN IF nextChunks = Chunks"))

    # Isolate this obligation: another invariant must not receive credit for
    # detecting the missing commit, nor may a parse/runtime failure count as a kill.
    File.write!(mutant_config, "SPECIFICATION Spec\nINVARIANT NoCommitMeansNoInstall\n")

    # TLC's stable VIOLATION_SAFETY exit status, not an error-message match.
    run_check!(check, mutant, mutant_config, 12)
    IO.puts("snapshot commit invariant rejects installation without a terminal commit")
    IO.puts("qualification artifacts: #{artifacts}")
  end

  defp run_check!(check, spec, config, expected_status) do
    {_output, status} =
      System.cmd("bash", [check],
        env: [{"TLA_SPEC", spec}, {"TLA_CONFIG", config}],
        into: IO.stream(),
        stderr_to_stdout: true
      )

    unless status == expected_status do
      raise "TLC exited #{status}, expected #{expected_status} for #{spec}"
    end
  end
end

Group.Formal.SnapshotCommitQualification.run()
