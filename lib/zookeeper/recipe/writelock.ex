defmodule WriteLock do
  @moduledoc """
  Zookeeper lock. This module has one call which does all the work of locking, running some code,
  and then unlocking.

  """

  @algorithm """
  Locks
  Fully distributed locks that are globally synchronous, meaning at any snapshot in time no two clients think they hold the same lock. These can be implemented using ZooKeeeper. As with priority queues, first define a lock node.

  Clients wishing to obtain a lock do the following:

  1. Call create( ) with a pathname of "_locknode_/guid-lock-" and the sequence and ephemeral flags set.
  The guid is needed in case the create() result is missed. See the note below.
  2. Call getChildren( ) on the lock node without setting the watch flag (this is important to avoid
  the herd effect).
  3. If the pathname created in step 1 has the lowest sequence number suffix, the client has the lock
  and the client exits the protocol.
  4. The client calls exists( ) with the watch flag set on the path in the lock directory with the
  next lowest sequence number.
  5. if exists( ) returns false, go to step 2. Otherwise, wait for a notification for the pathname
  from the previous step before going to step 2.

  The unlock protocol is very simple: clients wishing to release a lock simply delete the node they created in step 1.

  Here are a few things to notice:

  The removal of a node will only cause one client to wake up since each node is watched by exactly one client. In this way, you avoid the herd effect.
  There is no polling or timeouts.
  Because of the way you implement locking, it is easy to see the amount of lock contention, break locks, debug locking problems, etc.
  Recoverable Errors and the GUID
  If a recoverable error occurs calling create() the client should call getChildren() and check for a node containing the guid used in the path name. This handles the case (noted above) of the create() succeeding on the server but the server crashing before returning the name of the new node.
  """

  @node_name "__lock__"

  ## Client

  @doc """
  Obtain a lock using the indicated path and then call pid with the args. Returns the result
  of the call or {:error, "Reason"}. The pid will be linked for the duration of the lock with the
  current process; it's strongly recommended to have the current process linked with the ZK
  client process and spawn it with stop_on_disconnect set to true so everything is immediately
  killed off in case the ZK connection is lost.
  """
  def lock(zk, path, pid, args \\ []) do
    {:ok, pid} = GenServer.start(__MODULE__, {zk, path, pid, args})
    result = GenServer.call(pid, :lock)
    GenServer.stop(pid, :normal)
    result
  end

  ## Server

  def init({zk, path, pid, args}) do
    prefix = "#{UUID.uuid4(:hex)}#{@node_name}"
    # Step 1
    {:ok, created_path} = Zookeeper.Client.create(zk, "#{path}/#{prefix}", "",
      makepath: true, create_mode: :ephemeral_sequential)
    node = Path.basename(created_path)

    {:ok, %{zk: zk, path: path, pid: pid, args: args, node: node}}
  end

  def handle_call(:lock, from, state) do
    try_lock(%{state | return_pid: from})
  end

  def handle_cast(:try_lock, _from, %{return_pid: return_pid}=state) do
    case try_lock(state) do
      {:reply, value, state} -> GenServer.reply(return_pid, value)
      _ -> nil
    end
  end

  def terminate(_reason, %{zk: zk, path: path, pid: pid, node: node}) do
    Process.unlink(pid)
    Zookeeper.Client.delete(zk, "#{path}/#{node}")
  end

  # Callback from Client signaling that the watch triggered
  def handle_info({Zookeeper.Client, _path, _type}, state) do
    GenServer.cast(self(), :try_lock)
    {:noreply, state}
  end

  ## Private

  defp try_lock(%{zk: zk, path: path, pid: pid, args: args, node: node}=state) do
    # Step 2
    children = Zookeeper.Client.get_children(zk, path)
    # TODO shared code wth Election
    sorted_children = children
      |> Enum.sort_by(fn (<<_ :: size(25), @node_name, seq :: binary>>) -> String.to_integer(seq) end)
    if List.first(sorted_children) == node do
      # Step 3
      Process.link(pid)
      {:reply, GenServer.call(pid, args), state}
    else
      # 4
      my_pos = Enum.find_index(sorted_children, node)
      next_lower = Enum.at(sorted_children, my_pos - 1)
      case Zookeeper.exists(zk, next_lower, self()) do
        {:ok, _stat} -> {:noreply, state, :hibernate} # Wait until the watch fires, then try again
        {:error, :no_node} -> try_lock(state) # Stuff already changed, try again immediately
      end
    end
  end

end
