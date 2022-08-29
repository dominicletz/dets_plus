defmodule DetsPlus.Bench.DetsWrap do
  @moduledoc false
  def start_link(filename, _args) do
    :dets.open_file(__MODULE__, file: String.to_charlist(filename))
  end

  def get(db, key) do
    case :dets.lookup(db, key) do
      [] -> nil
      [other] -> other
    end
  end

  def size(db) do
    :dets.info(db, :size)
  end

  def put(db, key, value) do
    :dets.insert(db, {key, value})
  end

  def stop(db) do
    :dets.close(db)
  end
end
