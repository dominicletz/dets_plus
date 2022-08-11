defmodule EntryWriter do
  @moduledoc """
    Helper module to accelerate ETS inserts.
  """
  defstruct [:map]

  def new(_opts \\ []) do
    # batch_size = Keyword.get(opts, :batch_size, 1024)

    %EntryWriter{
      map: %{}
    }
  end

  def insert(state = %EntryWriter{map: map}, {table_idx, hash, offset}) do
    item = {hash, offset}
    map = Map.update(map, table_idx, [item], fn items -> [item | items] end)
    %EntryWriter{state | map: map}
  end

  def sync(state = %EntryWriter{}) do
    state
  end

  def lookup(%EntryWriter{map: map}, table_idx) do
    Map.get(map, table_idx, [])
  end

  def info(%EntryWriter{map: map}) do
    %{size: map_size(map)}
  end

  def close(%EntryWriter{}) do
    :ok
  end
end
