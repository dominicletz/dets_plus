defmodule DetsPlus.Bench.DetsPlusWrap do
  def start_link(filename, _args) do
    DetsPlus.open_file(__MODULE__, file: String.to_charlist(filename))
  end

  def get(db, key) do
    case DetsPlus.lookup(db, key) do
      [] -> nil
      [other] -> other
    end
  end

  def size(db) do
    DetsPlus.info(db, :size)
  end

  def put(db, key, value) do
    DetsPlus.insert(db, {key, value})
  end

  def stop(db) do
    DetsPlus.close(db)
  end
end
