defmodule Zookeeper.Error do
  defexception message: nil

  def exception(reason: reason) do
    %__MODULE__{message: reason |> to_string}
  end
end
