zookeeper-elixir
============

# Getting Started

This is fork, Original repo: [Zookeeper Elixir](https://github.com/vishnevskiy/zookeeper-elixir)

# Usage

The code is fairly documented and has tests and typespecs so just reading it should provide all the information needed to use the library effectively.


```elixir
{:ok, pid} = Zookeeper.Client.start
{:ok, path} = Zookeeper.Client.create(pid, "/testing", "some data")
{:ok, {data, stat}} = Zookeeper.Client.get(pid, path, self())
{:ok, stat} = Zookeeper.Client.set(pid, path, "some new data")
receive do
  {Zookeeper.Client, ^path, :data} -> IO.puts("data changed in #{path}")
end
```