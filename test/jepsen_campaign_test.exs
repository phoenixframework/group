defmodule Group.JepsenCampaignTest do
  use ExUnit.Case, async: true

  @moduletag :local
  @moduletag :tmp_dir

  setup %{tmp_dir: directory} do
    script_dir = Path.join(directory, "checkout/test/jepsen")
    bin = Path.join(directory, "bin")
    File.mkdir_p!(script_dir)
    File.mkdir_p!(bin)
    File.cp!("test/jepsen/campaign.sh", Path.join(script_dir, "campaign.sh"))

    # Match a minimal CI runner: no ripgrep and no real Docker/campaign work.
    for command <- ~w(bash dirname mkdir mktemp awk tail) do
      File.ln_s!(System.find_executable(command), Path.join(bin, command))
    end

    write_executable(Path.join(bin, "timeout"), """
    #!/bin/bash
    shift 3
    exec "$@"
    """)

    write_executable(Path.join(script_dir, "run.sh"), """
    #!/bin/bash
    printf '%s\\n' "$CAMPAIGN_OUTPUT"
    exit "$CAMPAIGN_STATUS"
    """)

    {:ok, script: Path.join(script_dir, "campaign.sh"), bin: bin}
  end

  test "counts every successful history without ripgrep", context do
    {output, status} = run_campaign(context, 2)
    assert status == 0, output
    assert output =~ "Passed distribution/mixed: 2/2 valid histories"
  end

  for count <- [0, 1, 3] do
    @tag count: count
    test "rejects #{count} successful histories when two are required", context do
      {output, status} = run_campaign(context, context.count)
      assert status == 1, output
      assert output =~ "expected 2 valid histories, found #{context.count}"
    end
  end

  test "preserves a failed campaign status even after successful histories", context do
    {output, status} = run_campaign(context, 2, 42)
    assert status == 42, output
    refute output =~ "Jepsen campaign passed"
  end

  test "does not disguise a counter failure as missing histories", context do
    awk = Path.join(context.bin, "awk")
    File.rm!(awk)
    write_executable(awk, "#!/bin/bash\nexit 42\n")

    {output, status} = run_campaign(context, 2)
    assert status == 42, output
    refute output =~ "valid histories, found"
  end

  defp run_campaign(context, count, status \\ 0) do
    System.cmd(System.find_executable("bash"), [context.script],
      env: [
        {"PATH", context.bin},
        {"GROUP_JEPSEN_SKIP_CHECKER", "1"},
        {"GROUP_JEPSEN_CAMPAIGN_TRANSPORT", "distribution"},
        {"GROUP_JEPSEN_CAMPAIGN_SCENARIO", "mixed"},
        {"GROUP_JEPSEN_CAMPAIGN_COUNT", "2"},
        {"GROUP_JEPSEN_CAMPAIGN_ARTIFACT_DIR", ""},
        {"CAMPAIGN_OUTPUT", String.duplicate("Everything looks good!\n", count)},
        {"CAMPAIGN_STATUS", Integer.to_string(status)}
      ],
      stderr_to_stdout: true
    )
  end

  defp write_executable(path, contents) do
    File.write!(path, contents)
    File.chmod!(path, 0o755)
  end
end
