defmodule Zookeeper.Mixfile do
  use Mix.Project

  def project do
    [
      app: :zookeeper,
      version: "0.1.1",
      elixir: "~>1.10",
      deps: deps(),
      package: package(),
      description: "Zookeeper client for Elixir with common recipes."
    ]
  end

  def application do
    [
      applications: [:erlzk, :uuid, :logger],
    ]
  end

  defp deps do
    [
      {:erlzk, "~> 0.6.4"},
      {:dialyze, "~> 0.2.1",  only: :dev},
      {:uuid, "~> 1.1"}
    ]
  end

  defp package do
    [
      contributors: ["Stanislav Vishnevskiy"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/vishnevskiy/zookeeper-elixir"}
    ]
  end
end
