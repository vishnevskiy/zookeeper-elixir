defmodule Zookeeper.DataWatchTest do
  use ExUnit.Case

  alias Zookeeper.Client, as: ZK
  alias Zookeeper.ZnodeStat
  alias Zookeeper.DataWatch, as: DW

  # TODO: test session lost

  setup_all do
    {:ok, pid} = ZK.start_link()
    pid |> cleanup
    {:ok, pid: pid}
  end

  setup %{pid: pid} = context do
    on_exit(context, fn -> cleanup(pid) end)
    :ok
  end

  test "data watch", %{pid: pid} do
    path = "/exunit"

    {:ok, _pid} = DW.start(pid, path)

    # Ensure nils are sent if node does not exist
    assert_receive {DW, _, ^path, :data, {nil, nil}}

    # Create Node
    assert {:ok, path} == ZK.create(pid, path, "1")
    assert_receive {DW, _, ^path, :data, {"1", %ZnodeStat{version: version = 0}}}

    # Update Node
    assert {:ok, %ZnodeStat{version: 1}} = ZK.set(pid, path, "2", version)
    assert_receive {DW, _, ^path, :data, {"2", %ZnodeStat{version: 1}}}

    # Delete Node
    assert :ok == ZK.delete(pid, path)
    assert_receive {DW, _, ^path, :data, {nil, nil}}

    # Recreate Node
    assert {:ok, path} == ZK.create(pid, path, "3")
    assert_receive {DW, _, ^path, :data, {"3", %ZnodeStat{version: 0}}}
  end

  test "watcher should die if zookeeper dies" do
    {:ok, zk} = ZK.start()
    {:ok, dw} = DW.start(zk, "/test")
    Process.exit(zk, :shutdown)
    :timer.sleep(1)
    refute Process.alive?(dw)
  end

  defp cleanup(pid) do
    pid |> ZK.delete("/exunit", -1, true)
  end
end
