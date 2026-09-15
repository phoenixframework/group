# Require once so independent harness regressions share the same real modules.
# Keep top-level executable startup, transport adapters, and unrelated modules
# out of the test VM. Log paths are configured at runtime, never rewritten here.
path = Path.join(__DIR__, "node.exs")
{:__block__, metadata, forms} = path |> File.read!() |> Code.string_to_quoted!()
modules = [Group.Jepsen.Transport.Stats, Group.Jepsen.Owner, Group.Jepsen.Driver]

forms =
  Enum.filter(forms, fn
    {:defmodule, _, [{:__aliases__, _, parts}, _]} -> Module.concat(parts) in modules
    _ -> false
  end)

Code.compile_quoted({:__block__, metadata, forms}, path)
