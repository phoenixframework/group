# Start epmd and distribution if they are not already running (needed for distributed tests)
epmd = System.find_executable("epmd") || raise "epmd executable not found"
epmd_running? = fn -> match?({_, 0}, System.cmd(epmd, ["-names"], stderr_to_stdout: true)) end

unless epmd_running?.() do
  case System.cmd(epmd, ["-daemon"], stderr_to_stdout: true) do
    {_, 0} -> :ok
    {output, status} -> raise "failed to start epmd (status #{status}): #{output}"
  end

  unless epmd_running?.() do
    raise "epmd did not become available after starting it"
  end
end

unless Node.alive?() do
  {:ok, _} = Node.start(:"test_#{System.unique_integer([:positive])}@127.0.0.1", :longnames)
  Node.set_cookie(:group_test)
end

# Disable :global's partition prevention to allow peer-to-peer disconnects
# in distributed tests without the test node also being disconnected.
:application.set_env(:kernel, :prevent_overlapping_partitions, false)

ExUnit.start()
