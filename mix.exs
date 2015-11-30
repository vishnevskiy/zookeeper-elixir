defmodule Zookeeper.Mixfile do
  use Mix.Project

  def project do
    [
      app: :zookeeper,
      version: "0.0.1",
      elixir: ">= 1.0.0 and < 1.2.0",
      deps: deps,
      package: package,
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
      {:erlzk, github: "huaban/erlzk", ref: "a005aab956e88686d8d9e719cb3937ae91c98b68"},
      {:uuid, "~> 0.1.5"},
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
