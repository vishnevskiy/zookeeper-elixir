defmodule Zookeeper.Mixfile do
  use Mix.Project

  def project do
    [
      app: :zookeeper,
      version: "0.0.1",
      elixir: "~> 0.15.1",
      deps: deps,
      package: package,
      description: "Zookeeper client for Elixir with common recipes."
    ]
  end

  def application do
    [
      applications: [:erlzk],
    ]
  end

  defp deps do
    [
      {:erlzk, github: "huaban/erlzk"}
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
