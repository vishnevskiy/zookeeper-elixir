defmodule WriteLockTest do
  use ExUnit.Case

   alias Zookeeper.Client, as: ZK
   alias Zookeeper.WriteLock, as: WL

   setup_all do
     {:ok, pid} = ZK.start
     pid |> cleanup
     {:ok, pid: pid}
   end

   setup %{pid: pid}=context do
     on_exit context, fn -> cleanup(pid) end
     :ok
   end

   test "writelock", %{pid: pid} do
     # TODO replace with a less bogus test
     for _ <- 0..2, into: Map.new, do: spawn_contender(pid)
   end

   defp cleanup(pid) do
     pid |> ZK.delete("/exunit-writelock", -1, true)
   end

   defp spawn_contender(zk) do
     identifier = UUID.uuid4(:hex)
     pid = spawn_contender(zk, identifier)
     {identifier, pid}
   end

   defp spawn_contender(zk, identifier) do
     WL.lock(zk, "/exunit-writelock", 5000, fn -> IO.puts("In lock #{identifier}") end)
   end
 end
