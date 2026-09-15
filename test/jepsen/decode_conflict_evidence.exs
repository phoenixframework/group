System.put_env("GROUP_JEPSEN_LIBRARY_ONLY", "1")
Code.require_file("node.exs", __DIR__)

[path] = System.argv()
events = path |> File.read!() |> Group.Jepsen.ConflictEvidence.decode()
IO.puts("CONFLICT-EVIDENCE " <> Group.Jepsen.EDN.encode(events))
