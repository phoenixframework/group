defmodule Group.MutationCampaignTest do
  use ExUnit.Case, async: true

  @moduletag :local

  test "every mutation has a current regression target and production fragment" do
    {output, status} =
      System.cmd("elixir", ["test/mutation/run.exs", "--list"],
        cd: Path.expand("..", __DIR__),
        env: [{"ERL_FLAGS", "+S 2:2"}, {"ERL_AFLAGS", nil}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "accept_old_generation"
    assert output =~ "drain_oversized_ingress_batch_without_yield"
  end
end
