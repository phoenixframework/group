# Run with: elixir test/mutation/isolation_test.exs
# Exercises the actual campaign against a tiny dependency-free fixture, with
# the real Mix aliases and one real mutation definition. No peers/JVM/Docker.
ExUnit.start()

defmodule Group.MutationIsolationTest do
  use ExUnit.Case, async: false

  @repo Path.expand("../..", __DIR__)
  @mutation "drain_oversized_ingress_batch_without_yield"

  setup do
    work = Path.join(@repo, "tmp/mutation-isolation-#{System.unique_integer([:positive])}")
    File.mkdir_p!(work)
    on_exit(fn -> File.rm_rf!(work) end)

    # Keep aliases and preferred environments identical to the real project,
    # but remove application code/dependencies unrelated to this harness probe.
    ast = @repo |> Path.join("mix.exs") |> File.read!() |> Code.string_to_quoted!()

    {_, functions} =
      Macro.prewalk(ast, [], fn
        {kind, _, [{name, _, _}, _]} = node, acc
        when kind in [:def, :defp] and name in [:aliases, :cli] ->
          {node, [Macro.to_string(node) | acc]}

        node, acc ->
          {node, acc}
      end)

    write(work, "mix.exs", """
    defmodule Isolation.MixProject do
      use Mix.Project
      def project, do: [app: :isolation, version: "0.0.0", aliases: aliases()]
      #{Enum.join(functions, "\n")}
    end
    """)

    File.mkdir_p!(Path.join(work, "deps"))
    File.cp!(Path.join(@repo, "test/mutation/run.exs"), write(work, "test/mutation/run.exs", ""))
    write(work, "test/test_helper.exs", "ExUnit.start()\n")

    checker =
      write(work, "test/jepsen/checker.sh", """
      #!/bin/sh
      echo invoked >> "#{work}/checker-invocations"
      exit 73
      """)

    File.chmod!(checker, 0o755)

    write(work, "lib/group/replica.ex", """
    defmodule Isolation.Target do
      @incoming_batch_quota 1
      def split(messages) do
        {turn, remaining} = Enum.split(messages, @incoming_batch_quota)
        {turn, remaining}
      end
    end
    """)

    {:ok, work: work}
  end

  test "a passing target survives a broken checker; ordinary mix test still runs it", %{
    work: work
  } do
    target(work, "assert is_tuple(Isolation.Target.split([1, 2]))")
    {output, status} = campaign(work)
    assert status == 1, output
    assert output =~ "#{@mutation}: SURVIVED"
    refute File.exists?(Path.join(work, "checker-invocations"))

    {output, status} = command(work, ["mix", "test", "test/group_test.exs:34"])
    assert status != 0, output
    assert File.read!(Path.join(work, "checker-invocations")) == "invoked\n"
  end

  test "a regression assertion kills the mutant", %{work: work} do
    target(work, "assert Isolation.Target.split([1, 2]) == {[1], [2]}")
    {output, status} = campaign(work)
    assert status == 0, output
    assert output =~ "#{@mutation}: killed"
    refute File.exists?(Path.join(work, "checker-invocations"))
  end

  test "a failing baseline fails the campaign before mutation", %{work: work} do
    target(work, "assert false")
    {output, status} = campaign(work)
    assert status != 0, output
    assert output =~ "baseline failed"
    refute output =~ "#{@mutation}: killed"
  end

  test "a noncompiling mutant remains invalid, not killed", %{work: work} do
    target(work, "assert is_tuple(Isolation.Target.split([1, 2]))")
    path = Path.join(work, "test/mutation/run.exs")
    source = File.read!(path)

    # Only the fixture's chosen replacement is made syntactically invalid.
    old = ~S("    _ = @incoming_batch_quota\n    turn = messages\n    remaining = []")
    assert length(:binary.matches(source, old)) == 1
    File.write!(path, String.replace(source, old, ~S("    this will not compile(")))

    {output, status} = campaign(work)
    assert status != 0, output
    assert output =~ "#{@mutation}: INVALID (does not compile)"
    refute output =~ "#{@mutation}: killed"
  end

  defp target(work, assertion) do
    write(
      work,
      "test/group_test.exs",
      "defmodule Isolation.TargetTest do\n  use ExUnit.Case\n" <>
        String.duplicate("\n", 31) <>
        "  test \"target\" do\n    #{assertion}\n  end\nend\n"
    )
  end

  defp campaign(work), do: command(work, ["elixir", "test/mutation/run.exs", @mutation])

  defp command(work, args) do
    timeout = System.find_executable("timeout") || raise "timeout is required"

    System.cmd(timeout, ["45" | args],
      cd: work,
      env: [{"ERL_FLAGS", "+S 2:2"}, {"MIX_ENV", nil}, {"ERL_AFLAGS", nil}],
      stderr_to_stdout: true
    )
  end

  defp write(work, relative, content) do
    path = Path.join(work, relative)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
    path
  end
end
