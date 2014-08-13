defmodule Zookeeper.ChildrenWatchTest do
  use ExUnit.Case

  alias Zookeeper.Client, as: ZK
  alias Zookeeper.ChildrenWatch, as: CW

  # TODO: test session lost

  setup_all do
    {:ok, pid} = ZK.connect
    pid |> cleanup
    {:ok, pid: pid}
  end

  setup %{pid: pid}=context do
    on_exit context, fn -> cleanup(pid) end
    :ok
  end

  test "children watch", %{pid: pid} do
    path = "/exunit"

    {:ok, _pid} = CW.start(pid, path)

    # Ensure no children are sent if node does not exist.
    refute_receive {_, ^path, :children, _}

    # Create Node
    assert {:ok, path} == ZK.create(pid, path)
    assert_receive {CW, _, ^path, :children, []}

    # Add First Child
    assert {:ok, _} = ZK.create(pid, "#{path}/a")
    assert_receive {CW, _, ^path, :children, ["a"]}

    # Add Children
    assert {:ok, _} = ZK.create(pid, "#{path}/b")
    assert {:ok, _} = ZK.create(pid, "#{path}/c")
    assert_receive {CW, _, ^path, :children, children}
    assert Enum.sort(children) == ["a", "b"]
    assert_receive {CW, _, ^path, :children, children}
    assert Enum.sort(children) == ["a", "b", "c"]

    # Delete Child
    assert :ok = ZK.delete(pid, "#{path}/c")
    assert_receive {CW, _, ^path, :children, children}
    assert Enum.sort(children) == ["a", "b"]

    # Delete Node
    assert :ok = ZK.delete(pid, path, -1, true)
    assert_receive {CW, _, ^path, :children, []}

    # Recreate Node
    assert {:ok, _} = ZK.create(pid, "#{path}/a", "", makepath: true)
    assert_receive {CW, _, ^path, :children, ["a"]}
  end

  defp cleanup(pid) do
    pid |> ZK.delete("/exunit", -1, true)
  end
end
