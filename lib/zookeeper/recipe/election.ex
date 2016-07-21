# TODO: handle zk session change

defmodule Zookeeper.Election do
  use GenServer

  require Logger

  @node_name "__election__"

  @moduledoc """
  ZooKeeper Leader Elections
  """

  ## Client

  @doc """
  Create election for a path.
  """
  def start(client, path, ma, identifier \\ nil) do
    GenServer.start(__MODULE__, {client, ma, path, identifier})
  end

  @doc """
  Create election for a path.
  """
  def start_link(client, ma, path, identifier \\ nil) do
    GenServer.start_link(__MODULE__, {client, ma, path, identifier})
  end

  @doc """
  Cancel participation in the election.
  """
  def cancel(pid) do
    GenServer.call(pid, :cancel)
  end

  @doc """
  Get identifier of all contenders.
  """
  def contenders(pid) do
    GenServer.call(pid, :contenders)
  end

  ## Server

  def init({client, path, ma, identifier}) do
    # props to Netflix Curator for this trick. It is possible for our
    # create request to succeed on the server, but for a failure to
    # prevent us from getting back the full path name. We prefix our
    # lock name with a uuid and can check for its presence on retry.
    prefix = "#{UUID.uuid4(:hex)}#{@node_name}"

    {:ok, pid} = Zookeeper.ChildrenWatch.start_link(client, path)

    state = %{
      client: client,
      prefix: prefix,
      path: path,
      ma: ma,
      data: (if identifier == nil do "" else identifier end),
      pid: nil,
      node: find_node(client, path, prefix),
      children: pid,
    } |> ensure_node

    {:ok, state}
  end

  def terminate(_reason, %{client: zk, path: path, node: node}) when node != nil do
    Zookeeper.Client.delete(zk, "#{path}/#{node}")
  end
  def terminate(_reason, _state), do: :ok

  def handle_call(:cancel, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(:contenders, _from, %{client: zk, path: path}=state) do
    contenders = sorted_children(state)
      |> Enum.map(
        fn node ->
          case Zookeeper.Client.get(zk, "#{path}/#{node}") do
            {:ok, {data, _stat}} -> data
            {:error, :no_node} -> nil
          end
        end
      )
      |> Enum.reject(&is_nil(&1))
    {:reply, contenders, state}
  end

  def handle_info({Zookeeper.ChildrenWatch, _pid, _path, :children, _}, %{pid: nil, node: node, ma: {module, args}}=state) do
    children = sorted_children(state)
    if List.first(children) == node do
      {:ok, pid} = apply(module, :start_link, args)
      state = %{state | pid: pid}
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  ## Private

  defp ensure_node(%{client: zk, path: path, prefix: prefix, data: data, node: nil}=state) do
    {:ok, created_path} = Zookeeper.Client.create(zk, "#{path}/#{prefix}", data, makepath: true, create_mode: :ephemeral_sequential)
    %{state | node: Path.basename(created_path)}
  end
  defp ensure_node(state), do: state

  defp find_node(zk, path, prefix) do
    case Zookeeper.Client.get_children(zk, path) do
      {:ok, children} -> children |> Enum.filter(&String.starts_with?(&1, prefix)) |> List.first
      {:error, :no_node} -> nil
    end
  end

  defp sorted_children(%{children: cw}) do
    Zookeeper.ChildrenWatch.children(cw)
      |> Enum.sort_by(fn (<<_ :: size(256), @node_name, seq :: binary>>) -> String.to_integer(seq) end)
  end
end
