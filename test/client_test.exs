defmodule Zookeeper.ClientTest do
  use ExUnit.Case

  alias Zookeeper.Client, as: ZK
  alias Zookeeper.ZnodeStat

  # TODO: test ACL

  setup_all do
    {:ok, pid} = ZK.start_link()
    pid |> cleanup
    {:ok, pid: pid}
  end

  setup %{pid: pid} = context do
    on_exit(context, fn -> cleanup(pid) end)
    :ok
  end

  test "simple create", %{pid: pid} do
    path = "/exunit"
    assert {:ok, ^path} = ZK.create(pid, path)
    assert {:ok, {"", _stat}} = ZK.get(pid, path)
  end

  test "create with data", %{pid: pid} do
    path = "/exunit"
    data = "data"
    assert {:ok, path} == ZK.create(pid, path, data)
    assert {:ok, {^data, _stat}} = ZK.get(pid, "/exunit")
  end

  test "create makepath", %{pid: pid} do
    path = "/exunit/a/b/c"
    data = "makepath"
    assert {:error, :no_node} == ZK.create(pid, path)
    assert {:ok, path} == ZK.create(pid, path, data, makepath: true)
    assert {:ok, {^data, _stat}} = ZK.get(pid, path)
  end

  test "create ephemeral", %{pid: pid} do
    path = "/exunit"
    assert {:ok, path} == ZK.create(pid, path, "", create_mode: :ephemeral)
    assert {:ok, {"", %ZnodeStat{owner_session_id: session_id}}} = ZK.get(pid, path)
    assert session_id != 0
    assert {:error, :no_children_for_ephemerals} == ZK.create(pid, "#{path}/a")
  end

  test "create sequential", %{pid: pid} do
    assert {:ok, "/exunit/s" <> seq} =
             ZK.create(pid, "/exunit/s", "", create_mode: :persistent_sequential, makepath: true)

    assert String.length(seq) > 0
  end

  test "get watch", %{pid: pid} do
    path = "/exunit"
    assert {:ok, path} == ZK.create(pid, path)
    assert {:ok, {"", _stat}} = ZK.get(pid, path, self())
    assert {:ok, _stat} = ZK.set(pid, path, "^.^")
    assert_receive {ZK, ^path, :data}
  end

  test "exists", %{pid: pid} do
    path = "/exunit"
    assert {:error, :no_node} == ZK.exists(pid, path)
    assert {:ok, path} == ZK.create(pid, path)
    assert {:ok, %ZnodeStat{}} = ZK.exists(pid, path)
  end

  test "exists watch", %{pid: pid} do
    path = "/exunit"
    assert {:error, :no_node} == ZK.exists(pid, path, self())
    assert {:ok, path} == ZK.create(pid, path)
    assert_receive {ZK, ^path, :exists}
  end

  test "ensure path", %{pid: pid} do
    path = "/exunit/a/b/c"
    assert {:error, :no_node} == ZK.exists(pid, path)
    assert :ok == ZK.ensure_path(pid, path)
    assert {:ok, _stat} = ZK.exists(pid, path)
  end

  test "get children", %{pid: pid} do
    path = "/exunit"
    assert {:error, :no_node} = ZK.get_children(pid, path)

    for i <- 0..5 do
      assert {:ok, _path} = ZK.create(pid, "#{path}/#{i}", "", makepath: true)
    end

    assert {:ok, children} = ZK.get_children(pid, path)
    assert Enum.map(0..5, &to_string/1) == Enum.sort(children)
  end

  test "get children watch", %{pid: pid} do
    path = "/exunit"
    assert {:ok, path} == ZK.create(pid, path)
    assert {:ok, []} = ZK.get_children(pid, path, self())
    assert {:ok, _path} = ZK.create(pid, "#{path}/a")
    assert_receive {ZK, ^path, :children}
  end

  test "set", %{pid: pid} do
    path = "/exunit"
    assert {:error, :no_node} == ZK.set(pid, path, "")
    assert {:ok, path} == ZK.create(pid, path)
    assert {:ok, {"", %ZnodeStat{version: version}}} = ZK.get(pid, path)
    assert {:ok, %ZnodeStat{version: version = 1}} = ZK.set(pid, path, "a")
    assert {:error, :bad_version} == ZK.set(pid, path, "b", 0)
    assert {:ok, %ZnodeStat{version: 2}} = ZK.set(pid, path, "b", version)
  end

  test "test delete", %{pid: pid} do
    path = "/exunit"
    assert {:ok, path} == ZK.create(pid, path)
    assert {:ok, _stat} = ZK.set(pid, path, "b")
    assert {:error, :bad_version} == ZK.delete(pid, path, 0)
    assert :ok == ZK.delete(pid, path, 1)
    assert {:error, :no_node} == ZK.get(pid, path)
  end

  test "test recursive delete", %{pid: pid} do
    path = "/exunit"
    assert {:ok, _path} = ZK.create(pid, "#{path}/a/b/c", "", makepath: true)
    assert {:error, :not_empty} == ZK.delete(pid, path)
    assert :ok == ZK.delete(pid, path, -1, true)
  end

  defp cleanup(pid) do
    pid |> ZK.delete("/exunit", -1, true)
  end
end
