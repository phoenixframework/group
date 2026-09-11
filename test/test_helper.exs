ExUnit.start()

if System.get_env("GROUP_TEST_DIAGNOSTICS") do
  ExUnit.configure(formatters: [ExUnit.CLIFormatter, Group.TestDiagnostics])
end
