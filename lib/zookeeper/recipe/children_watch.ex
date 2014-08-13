# TODO: handle zk session change

defmodule Zookeeper.ChildrenWatch do
  use GenServer

  @moduledoc """
  Watches a node for children updates and sends an event to the specified
  pid each time it changes

  The event will also be sent the very first time its
  registered to get children.
  """

  ## Client

  @doc """
  Create a children watcher for a path.
  """
  def start(client, path) do
    GenServer.start(__MODULE__, {client, path, self()})
  end


  @doc """
  Create a children watcher for a path.
  """
  def start_link(client, path) do
    GenServer.start_link(__MODULE__, {client, path, self()})
  end

  @doc """
  Get current list of children.
  """
  def children(pid) do
    GenServer.call(pid, :children)
  end

  @doc """
  Stop a children watcher.
  """
  def stop(pid) do
    GenServer.call(pid, :stop)
  end

  ## Server

  def init({client, path, watcher}) do
    state = %{
      client: client,
      path: path,
      watcher: watcher,
      prior_children: nil,
    }
    case maybe_get_children(state) do
      {:noreply, state} -> {:ok, state}
      {:stop, reason, _state} -> {:stop, reason}
    end
  end

  def handle_call(:children, _from, state) do
    {:reply, state.prior_children, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def handle_info({Zookeeper.Client, _path, :exists}, state) do
    maybe_get_children(state)
  end

  def handle_info({Zookeeper.Client, _path, :children}, state) do
    maybe_get_children(state)
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  ## Private

  def maybe_get_children(state) do
    if Process.alive?(state.watcher) do
      state |> get_children
    else
      {:stop, :normal, state}
    end
  end

  def get_children(state) do
    case Zookeeper.Client.get_children(state.client, state.path, self()) do
      {:ok, children} -> {:noreply, maybe_send_children(children, state)}
      {:error, :no_node} ->
        case Zookeeper.Client.exists(state.client, state.path, self()) do
          {:ok, _stat} -> get_children(state)
          {:error, :no_node} -> {:noreply, state}
          {:error, reason} -> {:stop, reason, state}
        end
      {:error, reason} -> {:stop, reason, state}
    end
  end

  defp maybe_send_children(children, state) do
    unless state.prior_children == children do
      send state.watcher, {__MODULE__, state.client, state.path, :children, children}
    end
    %{state | prior_children: children}
  end
end