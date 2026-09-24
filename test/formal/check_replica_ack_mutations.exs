defmodule Group.Formal.ReplicaAckQualification do
  @moduledoc false

  @mutations [
    {"stale ACK token", "  /\\ m.tok = s.sendToken\n", "", "INVARIANT",
     "QuietHasCurrentAck", 12, "Invariant QuietHasCurrentAck is violated"},
    {"stale need full send", "  s.pending /\\ m.aux = s.head /\\ m.n <= s.head\n",
     "  m.aux = s.head /\\ m.n <= s.head\n", "INVARIANT", "NoUnjustifiedFullSend", 12,
     "Invariant NoUnjustifiedFullSend is violated"},
    {"gratuitous full hello", "  /\\ s.helloDue\n", "", "INVARIANT",
     "NoUnjustifiedFullSend", 12, "Invariant NoUnjustifiedFullSend is violated"},
    {"lost recovery probe after a stale hello",
     "ProbeAllowed == ~s.connected \\/ s.probePending\n",
     "ProbeAllowed == ~s.connected\n", "PROPERTY", "HealedConvergence", 13,
     "Temporal property HealedConvergence was violated"},
    {"premature ACK for an uninstalled receiver PID",
     "m.probe = s.probe /\\ m.aux = 1 /\\ s.connected",
     "m.probe = s.probe /\\ s.connected", "PROPERTY", "HealedConvergence", 13,
     "Temporal property HealedConvergence was violated"},
    {"premature ACK for stale receiver authority",
     "m.epoch = s.knownEpoch\n                    THEN 1 ELSE 0",
     "TRUE\n                    THEN 1 ELSE 0", "PROPERTY", "HealedConvergence", 13,
     "Temporal property HealedConvergence was violated"}
  ]

  def run do
    directory = __DIR__
    source = File.read!(Path.join(directory, "ReplicaAck.tla"))
    check = Path.join(directory, "check.sh")

    Enum.each(@mutations, fn {label, old, replacement, check_kind, check_name, expected_status,
                              violation} ->
      unless length(:binary.matches(source, old)) == 1 do
        raise "#{label} mutation must match exactly one guard"
      end

      artifacts =
        Path.join(
          System.tmp_dir!(),
          "group-replica-ack-#{System.pid()}-#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(artifacts)
      spec = Path.join(artifacts, "ReplicaAck.tla")
      config = Path.join(artifacts, "ReplicaAck.cfg")
      File.write!(spec, String.replace(source, old, replacement))

      File.write!(
        config,
        """
        SPECIFICATION Spec
        CHECK_DEADLOCK FALSE
        CONSTANTS
          MaxSeq = 2
          MaxMessages = 2
          MaxProbe = 2
          MaxPid = 2
          MaxEpoch = 2
        #{check_kind} #{check_name}
        """
      )

      {output, status} =
        System.cmd("bash", [check],
          env: [
            {"TLA_SPEC", spec},
            {"TLA_CONFIG", config},
            {"TLA_METADIR", Path.join(artifacts, "tlc")}
          ],
          stderr_to_stdout: true
        )

      unless status == expected_status and String.contains?(output, violation) do
        raise "#{label} mutation was not rejected by #{check_name} (TLC status #{status}):\n#{output}"
      end

      IO.puts("#{check_name} rejects #{label}")
      File.rm_rf!(artifacts)
    end)
  end
end

Group.Formal.ReplicaAckQualification.run()
