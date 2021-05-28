defmodule Zookeeper.WriteLockSleepTest do
  @doc """
  A simple module to help in manual testing of WriteLock. The unit test is too simple so
  far, this is a band-aid
  """
  def sleeper(sleep) do
    IO.puts("#{inspect(self())}: Locked, sleeping for #{sleep}")
    :timer.sleep(sleep)
    IO.puts("#{inspect(self())}: Slept for #{sleep}, Releasing lock")
  end

  def test_lock(sleep, wait) do
    {:ok, zk} = Zookeeper.Client.start_link()
    Zookeeper.WriteLock.lock(zk, "/wait-writelock", wait, fn -> sleeper(sleep) end)
  end
end
