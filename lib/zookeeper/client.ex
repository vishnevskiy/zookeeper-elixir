defmodule Zookeeper.Client do 
  use GenServer

  alias Zookeeper.ZnodeStat

  @type host :: {char_list, integer}
  @type hosts :: String.t | [host]
  @type options :: [timeout: integer,
                    chroot: char_list,
                    disable_watch_auto_reset: boolean,
                    auth_data: [{char_list, binary}],
                    stop_on_disconnect: boolean]
  @type acl :: {[perms], scheme, id}
  @type perms :: :r | :w | :c | :d | :a
  @type scheme :: char_list
  @type id :: char_list
  @type create_mode :: :persistent | :ephemeral | :persistent_sequential | :ephemeral_sequential
  @type create_error :: :node_exists | :no_node | :no_auth | :invalid_acl | :no_children_for_ephemerals | :closed
  @type create_options :: [create_mode: create_mode,
                           acl: [acl],
                           makepath: boolean]

  # This Id represents anyone.
  @zk_id_anyone_id_unsafe ['world', 'anyone']
  # This Id is only usable to set ACLs. It will get substituted with the Id's the client authenticated with.
  @zk_id_auth_ids ['auth', '']
  
  # This is a completely open ACL.
  @zk_acl_open_acl_unsafe [:rwcda|@zk_id_anyone_id_unsafe] |> List.to_tuple
  # This ACL gives the creators authentication id's all permissions.
  @zk_acl_creator_all_acl [:rwcda|@zk_id_auth_ids] |> List.to_tuple
  # This ACL gives the world the ability to read.
  @zk_acl_read_acl_unsafe [:r|@zk_id_anyone_id_unsafe] |> List.to_tuple

  @doc """
  Connect to ZooKeeper.
  """
  @spec start(hosts, options) :: {:ok, pid} | {:error, atom}
  def start(hosts \\ "127.0.0.1:2181", options \\ []) do
    {timeout, options} = Keyword.pop(options, :timeout, 10000)
    {stop_on_disconnect, options} = Keyword.pop(options, :stop_on_disconnect, false)
    GenServer.start(__MODULE__, {hosts, timeout, stop_on_disconnect}, options)
  end

  @doc """
  Connect to ZooKeeper.
  """
  @spec start_link(hosts, options) :: {:ok, pid} | {:error, atom}
  def start_link(hosts \\ "127.0.0.1:2181", options \\ []) do
    {timeout, options} = Keyword.pop(options, :timeout, 10000)
    {stop_on_disconnect, options} = Keyword.pop(options, :stop_on_disconnect, false)
    GenServer.start_link(__MODULE__, {hosts, timeout, stop_on_disconnect}, options)
  end

  @doc """
  Disconnect from ZooKeeper.
  """
  @spec close(pid) :: :ok
  def close(pid) do
    GenServer.call(pid, :close)
  end

  @doc """
  Create a node with all possible options.
  """
  @spec create(pid, String.t, binary, create_options) :: {:ok, String.t} | {:error, create_error}
  def create(pid, path, value \\ "", opts \\ []) do
    GenServer.call(pid, {:create, path, value, opts})
  end

  @spec create!(pid, String.t, binary, create_options) :: {:ok, String.t} | {:error, create_error}
  def create!(pid, path, value \\ "", opts \\ []), do: apply!(&create/4, [pid, path, value, opts])

  @doc """
  Recursively create a path if it doesn’t exist.
  """
  @spec ensure_path(pid, String.t, [acl]) :: :ok | {:error, :no_auth | :invalid_acl | :no_children_for_ephemerals | :closed}
  def ensure_path(pid, path, acl \\ nil) do
    GenServer.call(pid, {:ensure_path, path, acl})
  end

  @doc """
  Check if a node exists.
  """
  @spec exists(pid, String.t, pid) :: {:ok, %ZnodeStat{}} | {:error, :no_node | :closed}
  def exists(pid, path, watcher \\ nil) do
    GenServer.call(pid, {:exists, path, watcher})
  end

  @spec exists!(pid, String.t, pid) :: %ZnodeStat{}
  def exists!(pid, path, watch \\ nil), do: apply!(&exists/3, [pid, path, watch])

  @doc """
  Get the value of a node.
  """
  @spec get(pid, String.t, pid) :: {:ok, {binary, %ZnodeStat{}}} | {:error, :no_node | :no_auth | :closed}
  def get(pid, path, watcher \\ nil) do
    GenServer.call(pid, {:get, path, watcher})
  end

  @spec get!(pid, String.t, pid) :: {binary, %ZnodeStat{}}
  def get!(pid, path, watcher \\ nil), do: apply!(&get/3, [pid, path, watcher])

  @doc """
  Get a list of child nodes of a path.
  """
  @spec get_children(pid, String.t, pid) :: {:ok, [String.t]} | {:error, :no_node | :no_auth | :closed}
  def get_children(pid, path, watcher \\ nil) do
    GenServer.call(pid, {:get_children, path, watcher})
  end

  @spec get_children!(pid, String.t, pid) :: [String.t]
  def get_children!(pid, path, watcher \\ nil), do: apply!(&get_children/3, [pid, path, watcher])
  
  @doc """
  Return the ACL and stat of the node of the given path.
  """
  @spec get_acls(pid, String.t) :: {:ok, {[acl], %ZnodeStat{}}} | {:error, :no_node | :no_auth | :closed}
  def get_acls(pid, path) do
    GenServer.call(pid, {:pid, path})
  end

  @doc """
  Set the ACL for the node of the given path.
  """
  @spec set_acls(pid, String.t, [acl], integer) :: {:ok, %ZnodeStat{}} | {:error, :no_node | :bad_version | :no_auth | :closed | :invalid_acl}
  def set_acls(pid, path, acls, version \\ -1) do
    GenServer.call(pid, {:set_acls, path, acls, version})
  end

  @doc """
  Set the value of a node.
  """
  @spec set(pid, String.t, binary, integer) :: {:ok, %ZnodeStat{}} | {:error, :no_node | :bad_version | :no_auth | :closed}
  def set(pid, path, value, version \\ -1) do
    GenServer.call(pid, {:set, path, value, version})
  end

  @spec set!(pid, String.t, binary, integer) :: %ZnodeStat{}
  def set!(pid, path, value, version \\ -1), do: apply!(&set/4, [pid, path, value, version])
 
  @doc """
  Delete a node.
  """
  @spec delete(pid, String.t, integer, boolean) :: :ok | {:error, :no_node | :bad_version | :no_auth | :closed | :not_empty}
  def delete(pid, path, version \\ -1, recursive \\ false) do
    GenServer.call(pid, {:delete, path, version, recursive})
  end

  @spec delete!(pid, String.t, integer, boolean) :: :ok
  def delete!(pid, path, version \\ -1, recursive \\ false), do: apply!(&delete/4, [pid, path, version, recursive])

  ## Server

  def init({hosts, timeout, stop_on_disconnect}) do
    {:ok, pid} = hosts 
      |> parse_hosts 
      |> :erlzk_conn.start_link(timeout, monitor: self)
    {:ok, %{zk: pid, watchers: HashDict.new, stop_on_disconnect: stop_on_disconnect}}
  end

  def terminate(_reason, %{zk: zk}) do
    :erlzk_conn.stop(zk)
  end

  def handle_call(:close, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call({:create, path, value, opts}, _from, %{zk: zk}=state) do
    {:reply, do_create(zk, path, value, opts), state}
  end

  def handle_call({:ensure_path, path, acl}, _from, %{zk: zk}=state) do
    {:reply, do_ensure_path(zk, path, acl), state}
  end

  def handle_call({:exists, path, watcher}, _from, %{zk: zk}=state) do
    if watcher == nil do
      {:reply, do_exists(zk, path, false), state}
    else
      reply = do_exists(zk, path, true)
      watchers = state.watchers |> update_watchers({:exists, path}, watcher)
      {:reply, reply, %{state | watchers: watchers}}
    end
  end

  def handle_call({:get, path, watcher}, _from, %{zk: zk, watchers: watchers}=state) do
    if watcher == nil do
      {:reply, do_get(zk, path, false), state}
    else
      reply = do_get(zk, path, true)
      watchers = state.watchers |> update_watchers({:data, path}, watcher)
      {:reply, reply, %{state | watchers: watchers}}
    end
  end

  def handle_call({:get_children, path, watcher}, _from, %{zk: zk}=state) do
    if watcher == nil do
      {:reply, do_get_children(zk, path, false), state}
    else
      reply = do_get_children(zk, path, true)
      watchers = state.watchers |> update_watchers({:children, path}, watcher)
      {:reply, reply, %{state | watchers: watchers}}
    end
  end

  def handle_call({:get_acls, path}, _from, %{zk: zk}=state) do
    {:reply, do_get_acls(zk, path), state}
  end

  def handle_call({:set_acls, path, acl, version}, _from, %{zk: zk}=state) do
    {:reply, do_set_acls(zk, path, acl, version), state}
  end

  def handle_call({:set, path, value, version}, _from, %{zk: zk}=state) do
    {:reply, do_set(zk, path, value, version), state}
  end

  def handle_call({:delete, path, version, recursive}, _from, %{zk: zk}=state) do
    {:reply, do_delete(zk, path, version, recursive), state}
  end

  def handle_info({:exists, path, _event}, state) do
    {:noreply, %{state | watchers: notify_watchers(state.watchers, :exists, path)}}
  end 

  def handle_info({:get_data, path, _event}, state) do
    {:noreply, %{state | watchers: notify_watchers(state.watchers, :data, path)}}
  end 

  def handle_info({:get_children, path, _event}, state) do
    {:noreply, %{state | watchers: notify_watchers(state.watchers, :children, path)}}
  end 

  def handle_info({:connected, _host, _port}, state) do
    {:noreply, state}
  end 

  def handle_info({:disconnected, _host, _port}, %{stop_on_disconnect: true}=state) do
    {:stop, :disconnected, state}
  end 

  def handle_info({:expired, _host, _port}, state) do
    {:noreply, state}
  end 

  def handle_info(_message, state) do
    {:noreply, state}
  end 

  ## Private ZK

  def do_create(pid, path, value \\ "", opts \\ []) do
    acl = 
      case Keyword.get(opts, :acl) do
        nil -> [@zk_acl_open_acl_unsafe]
        acl -> acl
      end
    create_mode = Keyword.get(opts, :create_mode, :persistent)
    makepath = Keyword.get(opts, :makepath, false)

    if makepath do 
      do_ensure_path(pid, Path.dirname(path), acl)
    end

    case :erlzk_conn.create(pid, normalize_path(path), value, acl, create_mode) do
      {:ok, path} -> {:ok, to_string(path)} 
      error -> error
    end
  end

  def do_ensure_path(pid, path, acl \\ nil) do
    case do_create(pid, path, "", acl: acl) do
      {:ok, _} -> :ok
      {:error, :node_exists} -> :ok
      {:error, :no_node} -> 
        case do_ensure_path(pid, Path.dirname(path), acl) do
          :ok -> do_ensure_path(pid, path, acl)
          error -> error
        end
      error -> error
    end
  end

  def do_exists(pid, path, watch) do
    reply = 
      if watch do
        :erlzk_conn.exists(pid, normalize_path(path), true, self)
      else
        :erlzk_conn.exists(pid, normalize_path(path), false)
      end
    case reply do
      {:ok, stat} -> {:ok, ZnodeStat.new(stat)} 
      error -> error
    end
  end

  def do_get(pid, path, watch) do
    reply = 
      if watch do
        :erlzk_conn.get_data(pid, normalize_path(path), true, self)
      else
        :erlzk_conn.get_data(pid, normalize_path(path), false)
      end
    case reply do
      {:ok, {value, stat}} -> {:ok, {value, ZnodeStat.new(stat)}} 
      error -> error
    end
  end

  def do_get_children(pid, path, watch) do
    reply = 
      if watch do
        :erlzk_conn.get_children(pid, normalize_path(path), true, self)
      else
        :erlzk_conn.get_children(pid, normalize_path(path), false)
      end
    case reply do
      {:ok, children} -> {:ok, children |> Enum.map(&to_string/1)}
      error -> error
    end
  end

  def do_get_acls(pid, path) do
    case :erlzk_conn.get_acl(pid, normalize_path(path)) do
      {:ok, {acls, {stat, _}}} -> {:ok, {acls, ZnodeStat.new(stat)}} 
      error -> error
    end
  end

  def do_set_acls(pid, path, acls, version \\ -1) do
     case :erlzk_conn.set_acl(pid, normalize_path(path), acls, version) do
      {:ok, stat} -> {:ok, ZnodeStat.new(stat)}
      error -> error
    end
  end

  def do_set(pid, path, value, version \\ -1) do
    case :erlzk_conn.set_data(pid, normalize_path(path), value, version) do
      {:ok, stat} -> {:ok, ZnodeStat.new(stat)}
      error -> error
    end
  end

  def do_delete(pid, path, version \\ -1, recursive \\ false) do
    if recursive do
      case do_get_children(pid, path, false) do
        {:ok, children} ->
          children |> Enum.each(&do_delete(pid, Path.join(path, &1), -1, true))
          do_delete(pid, path, version)
        {:error, :no_node} -> :ok
        error -> error
      end
    else
      :erlzk_conn.delete(pid, normalize_path(path), version)
    end
  end

  ## Private

  @spec normalize_path(String.t) :: char_list
  defp normalize_path(path) when is_bitstring(path), do: path |> String.to_char_list |> normalize_path

  @spec normalize_path(char_list) :: char_list
  defp normalize_path([?/|_]=path), do: path

  @spec normalize_path(char_list) :: char_list
  defp normalize_path(path), do: [?/|path]

  @spec parse_hosts(String.t) :: [host]
  defp parse_hosts(hosts) when is_bitstring(hosts) do
    hosts 
    |> String.split(",") 
    |> Enum.map(&String.split(&1, ":"))
    |> Enum.map(fn([host, port]) -> {String.to_char_list(host), String.to_integer(port)} end)
  end

  @spec parse_hosts([host]) :: [host]
  defp parse_hosts(hosts), do: hosts

  @spec apply!((... -> any), [any]) :: any
  defp apply!(function, args) do
    case apply(function, args) do
      :ok -> :ok
      {:ok, result} -> result
      {:error, reason} -> raise Zookeeper.Error, reason: reason
    end
  end

  @spec notify_watchers(Dict.t, :exists | :data | :children, char_list) :: Dict.t
  defp notify_watchers(watchers, type, path) do
    path = path |> to_string
    case Dict.pop(watchers, {type, path}) do
      {nil, watchers} ->
        watchers
      {receivers, watchers} ->
        receivers |> Enum.each(&send(&1, {__MODULE__, path, type}))
        watchers
    end
  end

  @spec notify_watchers(Dict.t, {:exists | :data | :children, char_list}, pid) :: Dict.t
  defp update_watchers(watchers, key, watcher) do
    if Dict.has_key?(watchers, key) do
      watchers |> Dict.put(key, [watcher|Dict.get(watchers, key)])
    else
      watchers |> Dict.put_new(key, [watcher])
    end
  end
end
