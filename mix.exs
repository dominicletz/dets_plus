defmodule DetsPlus.MixProject do
  use Mix.Project

  @version "2.4.2"
  @name "DetsPlus"
  @url "https://github.com/dominicletz/dets_plus"
  @maintainers ["Dominic Letz"]

  def project do
    [
      app: :dets_plus,
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: @name,
      version: @version,
      docs: docs(),
      package: package(),
      homepage_url: @url,
      aliases: aliases(),
      description: """
      Pure Elixir disk backed key-value store.
      """,
      elixirc_paths: if(Mix.env() == :dev, do: ["lib", "bench"], else: ["lib"])
    ]
  end

  defp docs do
    [
      main: @name,
      source_ref: "v#{@version}",
      source_url: @url,
      authors: @maintainers
    ]
  end

  defp package do
    [
      maintainers: @maintainers,
      licenses: ["MIT"],
      links: %{github: @url},
      files: ~w(lib LICENSE.md mix.exs README.md)
    ]
  end

  defp aliases() do
    [
      lint: [
        "compile",
        "format --check-formatted",
        "credo",
        "dialyzer"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.28", only: :dev, runtime: false},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.2", only: [:dev], runtime: false},
      {:cubdb, "~> 2.0", only: [:dev], runtime: false},
      {:benchee, "~> 1.1", only: :dev, runtime: false},
      {:paged_file, "~> 1.1"}
      # {:paged_file, path: "../paged_file"}
    ]
  end
end
