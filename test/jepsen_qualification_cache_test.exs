defmodule Group.JepsenQualificationCacheTest do
  use ExUnit.Case, async: true

  @moduletag :local
  @moduletag :tmp_dir

  for existing_cache? <- [false, true] do
    @tag existing_cache?: existing_cache?
    test "standalone qualification prepares artifacts with existing cache: #{existing_cache?}",
         %{tmp_dir: directory, existing_cache?: existing_cache?} do
      repo = Path.join(directory, "checkout")
      script_dir = Path.join(repo, "test/jepsen")
      cache = Path.join(script_dir, ".cache")
      bin = Path.join(directory, "bin")
      File.mkdir_p!(script_dir)
      File.mkdir_p!(bin)
      File.cp!("test/jepsen/qualify.sh", Path.join(script_dir, "qualify.sh"))

      if existing_cache? do
        File.mkdir_p!(cache)
        File.write!(Path.join(cache, "retained-artifact"), "previous run")
      end

      # Stop at the first external phase, before mutation or Docker work. This
      # also verifies that a baseline failure still propagates out of the script.
      mix = Path.join(bin, "mix")

      File.write!(mix, """
      #!/bin/sh
      printf '%s\\n' "$PWD" "$@" > baseline-invocation
      exit 42
      """)

      File.chmod!(mix, 0o755)

      {output, status} =
        System.cmd("bash", [Path.join(script_dir, "qualify.sh")],
          env: [{"PATH", bin <> ":" <> System.fetch_env!("PATH")}],
          stderr_to_stdout: true
        )

      assert status == 42, output

      assert File.read!(Path.join(repo, "baseline-invocation")) ==
               "#{Path.expand(repo)}\nrun\ntest/mutation/run.exs\n"

      assert [artifact] = Path.wildcard(Path.join(cache, "qualification.*"))
      assert File.dir?(artifact)

      if existing_cache? do
        assert File.read!(Path.join(cache, "retained-artifact")) == "previous run"
      end
    end
  end
end
