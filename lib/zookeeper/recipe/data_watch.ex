# TODO: handle zk session change

defmodule Zookeeper.DataWatch do
  use GenServer

  @moduledoc """
  Watches a node for data updates and sends an event to the specified
  watcher each time it changes

  The event will also be sent the very first time its
  registered to get the data.

  If the node does not exist, then the event will be sent with
  `nil` for all values.
  """

  ## Client

  @doc """
  Create a data watcher for a path.
  """
  def start(client, path, watcher \\ self()) do
    GenServer.start(__MODULE__, {client, path, watcher})
  end

  @doc """
  Create a data watcher for a path.
  """
  def start_link(client, path, watcher \\ self()) do
    GenServer.start_link(__MODULE__, {client, path, watcher})
  end

  @doc """
  Get current data.
  """
  def data(pid) do
    GenServer.call(pid, :data)
  end

  @doc """
  Stop a data watcher.
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
      prior_data: nil,
    }
    Process.monitor(client)
    case maybe_get_data(state) do
      {:noreply, state} -> {:ok, state}
      {:stop, reason, _state} -> {:stop, reason}
    end
  end

  def handle_call(:data, _from, state) do
    {:reply, state.prior_data, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def handle_info({Zookeeper.Client, _path, :exists}, state) do
    maybe_get_data(state)
  end

  def handle_info({Zookeeper.Client, _path, :data}, state) do
    maybe_get_data(state)
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, %{client: pid}=state) do
    {:stop, reason, state}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  ## Private

  defp maybe_get_data(state) do
    if Process.alive?(state.watcher) do
      state |> get_data
    else
      {:stop, :normal, state}
    end
  end

  defp get_data(state) do
    case Zookeeper.Client.get(state.client, state.path, self()) do
      {:ok, {data, stat}} -> {:noreply, maybe_send_data({data, stat}, state)}
      {:error, :no_node} ->
        case Zookeeper.Client.exists(state.client, state.path, self()) do
          {:ok, _stat} -> get_data(state)
          {:error, :no_node} -> {:noreply, maybe_send_data({nil, nil}, state)}
          {:error, reason} -> {:stop, reason, state}
        end
      {:error, reason} -> {:stop, reason, state}
    end
  end

  defp maybe_send_data(data, state) do
    unless state.prior_data == data do
      send state.watcher, {__MODULE__, state.client, state.path, :data, data}
    end
    %{state | prior_data: data}
  end
end