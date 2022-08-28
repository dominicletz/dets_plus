defmodule CubDBWrap do
  @moduledoc """
    Helper to make CubDB compatible with the `:dets` protocol
  """
  def open_file(filename, _args) do
    CubDB.start_link(data_dir: "#{filename}/", auto_compact: false, auto_file_sync: false)
  end

  def lookup(db, key) do
    case CubDB.get(db, key) do
      nil -> []
      other -> [other]
    end
  end

  def insert(db, values) when is_list(values) do
    values =
      Enum.map(values, fn value ->
        key = elem(value, 0)
        {key, value}
      end)

    CubDB.put_multi(db, values)
  end

  def insert(db, value) do
    key = elem(value, 0)
    CubDB.put(db, key, value)
  end

  def sync(db) do
    CubDB.compact(db)
    CubDB.file_sync(db)
  end

  def close(db) do
    CubDB.stop(db)
  end
end
