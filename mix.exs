defmodule Group.MixProject do
  use Mix.Project

  @version "0.2.1"
  @source_url "https://github.com/phoenixframework/group"

  def project do
    [
      app: :group,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_ignore_filters: [~r"^test/jepsen/", ~r"^test/mutation/"],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      package: package(),
      docs: docs(),
      name: "Group",
      homepage_url: @source_url,
      description: """
      Distributed process groups, registry, lifecycle monitoring, and isolated subclusters.
      """
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {Group.Application, []}
    ]
  end

  def cli do
    [preferred_envs: ["test.soak": :test]]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.30", only: :dev, runtime: false},
      {:stream_data, "~> 1.4", only: :test}
    ]
  end

  defp package do
    [
      maintainers: ["Chris McCord"],
      licenses: ["MIT"],
      links: %{GitHub: @source_url},
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE.md CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "Group",
      source_url: @source_url,
      source_ref: "v#{@version}"
    ]
  end

  defp aliases do
    [
      test: ["test", "cmd test/jepsen/checker.sh"],
      "test.soak": [
        # Run the PR gate in a child VM. test_helper starts distribution, and
        # keeping that VM alive for the following `cmd` phases can retain a
        # fixed ERL_AFLAGS distribution port and make qualification fail with
        # eaddrinuse.
        "cmd mix test",
        "cmd env GROUP_JEPSEN_SKIP_CHECKER=1 test/jepsen/qualify.sh",
        "cmd env GROUP_JEPSEN_SKIP_CHECKER=1 test/jepsen/campaign.sh"
      ]
    ]
  end
end
