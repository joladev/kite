defmodule Kite.MixProject do
  use Mix.Project

  def project do
    [
      app: :kite,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: "An Elixir atproto Jetstream V2 subscriber library",
      package: package(),
      docs: docs(),
      source_url: "https://github.com/joladev/kite"
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:mimic, "~> 2.3", only: :test},
      {:ex_doc, "~> 0.34", only: :dev},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:mint_web_socket, "~> 1.0"}
    ]
  end

  defp aliases do
    [
      precommit: [
        "compile --warnings-as-errors",
        "deps.unlock --unused",
        "format",
        "credo --strict",
        "test"
      ]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "Tangled" => "https://tangled.org/jola.dev/kite",
        "GitHub" => "https://github.com/joladev/kite"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
  end
end
