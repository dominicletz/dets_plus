defmodule DetsPlus.HashLRU do
  @moduledoc """
  DetsPlus.HashLRU Hash based Least-Recently-Used cache based on DetsPlus persistent storage.

  In contrast to DetsPlus.LRU the DetsPlush.HashLRU keeps the LRU size constant
  by using a hash function to determine the key's position in the cache. This is a tradeoff
  between the performance of the cache.

  Pro: (FASTER) By not needing to store a relationship between the keys and the order of the cache, the cache
  can skip a read operation and also save on key space.

  Contra: (LESS ACCURATE) Because there will be hash collisions, the cache will not be able to store all the
  keys in the exact order they were inserted. E.g. it's not LRU but rather a random key that gets evicted.

  Example:

  ```elixir
  alias DetsPlus.HashLRU

  {:ok, dets} = DetsPlus.open_file(:example)
  filter = fn value -> value != nil end
  max_size = 2
  lru = HashLRU.new(dets, max_size, filter)
  HashLRU.put(lru, 1, "1")
  HashLRU.put(lru, 2, "2")
  HashLRU.put(lru, 3, "3")
  HashLRU.get(lru, 1) == nil

  DetsPlus.close(dets)
  ```
  """

  defstruct [:dets, :size, :max_size, :filter]

  @doc """
  Creates a new LRU cache.

  ## Parameters
  * `dets` - The DetsPlus storage to use.
  * `max_size` - The maximum number of items to store.
  * `filter` - A filter function that determines if a value should be stored or not.
  """
  def new(dets, max_size, filter \\ fn _ -> true end) when is_integer(max_size) do
    counter = :atomics.new(1, signed: false)
    lru = %DetsPlus.HashLRU{dets: dets, size: counter, max_size: max_size, filter: filter}
    size = _get(lru, :meta, 0)
    _put(lru, {:meta, size})
    :atomics.put(counter, 1, size)
    lru
  end

  @doc """
  Puts a value in the cache.

  ## Parameters
  * `lru` - The LRU cache.
  * `key` - The key to store the value under.
  * `value` - The value to store.
  """
  def put(lru, key, value) do
    filter_fun = filter(lru)

    if filter_fun.(value) do
      n = :atomics.add_get(lru.size, 1, 1)
      _put(lru, [{:meta, n}, {hashkey(lru, key), {key, value}}])
    else
      delete(lru, key)
    end

    value
  end

  @doc """
  Gets a value from the cache.

  ## Parameters
  * `lru` - The LRU cache.
  * `key` - The key to get the value for.
  * `default` - The default value to return if the key is not found.
  """
  def get(lru, key, default \\ nil) do
    case _get(lru, hashkey(lru, key)) do
      nil -> default
      {^key, value} -> value
      {_otherkey, _othervalue} -> default
    end
  end

  @doc """
  Deletes a value from the cache.

  ## Parameters
  * `lru` - The LRU cache.
  * `key` - The key to delete.
  """
  def delete(lru, key) do
    _delete(lru, hashkey(lru, key))
  end

  @doc """
  Fetches a value from the cache, or calculates it if it's not found.

  ## Parameters
  * `lru` - The LRU cache.
  * `key` - The key to fetch the value for.
  * `fun` - The function to calculate the value if it's not found.
  """
  def fetch(lru, key, fun) do
    get(lru, key) ||
      :global.trans({key, self()}, fn ->
        fetch_nolock(lru, key, fun)
      end)
  end

  @doc """
  Updates a value in the cache.

  ## Parameters
  * `lru` - The LRU cache.
  * `key` - The key to update.
  * `fun` - The function to calculate the new value.
  """
  def update(lru, key, fun) do
    :global.trans({key, self()}, fn ->
      put(lru, key, _eval(fun))
    end)
  end

  @doc """
  Fetches a value from the cache, or calculates it if it's not found.

  Same as `fetch/3`, but without locking.

  ## Parameters
  * `lru` - The LRU cache.
  * `key` - The key to fetch the value for.
  * `fun` - The function to calculate the value if it's not found.
  """
  def fetch_nolock(lru, key, fun) do
    get(lru, key) || put(lru, key, _eval(fun))
  end

  @doc """
  Returns the size of the cache.

  ## Parameters
  * `lru` - The LRU cache.
  """
  def size(lru) do
    min(:atomics.get(lru.size, 1), lru.max_size)
  end

  @doc """
  Returns the keys and values of the cache.

  ## Parameters
  * `lru` - The LRU cache.
  """
  def to_list(lru) do
    for {{:hash, _hash}, {key, value}} <- DetsPlus.to_list(lru.dets), do: {key, value}
  end

  @doc """
  Returns the filter function of the cache.

  ## Parameters
  * `lru` - The LRU cache.
  """
  def filter(lru) do
    lru.filter
  end

  @doc """
  Returns the maximum size of the cache.

  ## Parameters
  * `lru` - The LRU cache.
  """
  def max_size(lru) do
    lru.max_size
  end

  @doc """
  Flushes the cache deleting all objects.

  ## Parameters
  * `lru` - The LRU cache.
  """
  def flush(lru) do
    DetsPlus.delete_all_objects(lru.dets)
    :atomics.put(lru.size, 1, 0)
    lru
  end

  #
  # Private functions below
  #

  defp _eval(fun) when is_function(fun, 0) do
    fun.()
  end

  defp _eval({m, f, a}) do
    apply(m, f, a)
  end

  defp _delete(lru, key) do
    DetsPlus.delete(lru.dets, key)
  end

  defp _get(lru, key, default \\ nil) do
    case DetsPlus.lookup(lru.dets, key) do
      [{^key, value}] -> value
      [] -> default
      _other -> default
    end
  end

  defp _put(lru, tuple) do
    DetsPlus.insert_async_if_not_busy(lru.dets, tuple)
  end

  defp hashkey(lru, key) do
    {:hash, :erlang.phash2(key, lru.max_size)}
  end
end
