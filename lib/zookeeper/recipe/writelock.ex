defmodule Zookeeper.WriteLock do
  @moduledoc """
  Zookeeper lock recipe
  """

  @node_name "__lock__"

  ## Client

  @doc """
  Obtain a lock using the indicated path and then call passed in function.

  Returns the result of the call or an error. It's strongly recommended to have
  the current process linked with the ZK client process and spawn it with
  stop_on_disconnect set to true so everything is immediately killed off in
  case the ZK connection is lost.

  Note that you _must_ pass a timeout. I think it's good practice to make
  specifying timeouts explicit.
  """
  def lock(zk, path, timeout, fun) do
    {:ok, pid} = GenServer.start_link(__MODULE__, {zk, path, fun})
    try do
      GenServer.call(pid, :lock, timeout)
    after
      spawn fn -> GenServer.stop(pid, :normal) end
    end
  end

  ## Server

  def init({zk, path, fun}) do
    prefix = "#{UUID.uuid4(:hex)}#{@node_name}"
    # Step 1
    {:ok, created_path} = Zookeeper.Client.create(zk, "#{path}/#{prefix}", "",
      makepath: true, create_mode: :ephemeral_sequential)
    node = Path.basename(created_path)

    {:ok, %{zk: zk, path: path, fun: fun, node: node, return_pid: nil}}
  end

  def handle_call(:lock, from, state) do
    try_lock(%{state | return_pid: from})
  end

  def terminate(_reason, %{zk: zk, path: path, node: node}) do
    Zookeeper.Client.delete(zk, "#{path}/#{node}")
  end

  # Callback from Client signaling that the watch triggered
  def handle_info({Zookeeper.Client, _path, _type}, state) do
    GenServer.cast(self(), :try_lock)
    {:noreply, state}
  end

  def handle_cast(:try_lock, %{return_pid: return_pid}=state) do
    case try_lock(state) do
      {:reply, value, _state} -> GenServer.reply(return_pid, value)
      _ -> nil
    end
  end

  ## Private

  defp try_lock(%{zk: zk, path: path, fun: fun, node: node}=state) do
    # Find children
    {:ok, children} = Zookeeper.Client.get_children(zk, path)
    sorted_children = children
      |> Enum.sort_by(fn (<<_ :: size(256), @node_name, seq :: binary>>) -> String.to_integer(seq) end)
    # If we're the first we have the lowest sequence number and therefore the lock
    if List.first(sorted_children) == node do
      {:reply, fun.(), state}
    else
      # If not, watch our predecessor. If it changes, we retry.
      my_pos = Enum.find_index(sorted_children, fn(v) -> v == node end)
      next_lower = Enum.at(sorted_children, my_pos - 1)
      case Zookeeper.Client.exists(zk, "#{path}/#{next_lower}", self()) do
        {:ok, _stat} ->
          {:noreply, state} # Wait until the watch fires, then try again
        {:error, :no_node} ->
          try_lock(state)
      end
    end
  end
end
