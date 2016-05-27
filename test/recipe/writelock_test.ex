defmodule WriteLockTest do
 use ExUnit.Case

   alias Zookeeper.Client, as: ZK
   alias Zookeeper.WriteLock, as: WL

   defmodule TestServer do
     use GenServer

     require Logger

     def start_link(identifier, test_pid, opts \\ []) do
       GenServer.start_link(__MODULE__, {test_pid, identifier}, opts)
     end

     def init({test_pid, identifier}) do
       test_pid |> send({:leader, self, identifier})
       {:ok, :ok}
     end
   end

   setup_all do
     {:ok, pid} = ZK.start
     pid |> cleanup
     {:ok, pid: pid}
   end

   setup %{pid: pid}=context do
     on_exit context, fn -> cleanup(pid) end
     :ok
   end

   test "election", %{pid: pid} do
     lockers = for _ <- 0..2, into: Map.new, do: spawn_contender(pid)

     # wait for a leader to be elected
     assert_receive {:leader, _leader_pid, leader_identifier}

     election = spawn_contender(pid, "test")

     # first one in list should be leader
     contenders = E.contenders(election)
     assert List.first(contenders) == leader_identifier

     # tell second one to cancel election. should never get elected.
     elections[Enum.at(contenders, 1)] |> Process.unlink
     elections[Enum.at(contenders, 1)] |> E.cancel

     # make leader exit. third contender should be elected.
     elections[leader_identifier] |> Process.unlink
     elections[leader_identifier] |> E.cancel
     expected_leader_identifier = Enum.at(contenders, 2)
     assert_receive {:leader, _leader_pid, ^expected_leader_identifier}

     # make first contender re-enter the race
     spawn_contender(pid, leader_identifier)

     # contender set should now be the current leader plus the first leader
     contenders = E.contenders(election)
     assert contenders == [expected_leader_identifier, "test", leader_identifier]
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
     {:ok, pid} = E.start_link(zk, "/exunit-writelock", {TestServer, [identifier, self]}, identifier)
     pid
   endnd
