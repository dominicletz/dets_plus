defmodule KrakenDB do
  @moduledoc """
  Alpha version of a sharded DetsPlus
  """
  defstruct [:dets, :keypos, :keyfun, :keyhashfun, :hashfun]
  @type t :: %__MODULE__{}
  @shard_count 256

  @doc false
  def open_file(name, args \\ []) when is_atom(name) and is_list(args) do
    open_directory(Atom.to_string(name), args)
  end

  def open_directory(name, args \\ []) when is_list(args) do
    {directory, args} = Keyword.pop(args, :directory, name)

    File.mkdir_p(directory)
    meta = Path.join(directory, "meta")

    args =
      case File.read(meta) do
        {:ok, content} ->
          Keyword.merge(args, :erlang.binary_to_term(content))

        _ ->
          File.write!(meta, :erlang.term_to_binary(args))
          args
      end

    auto_save_memory = Keyword.get(args, :auto_save_memory, 1_000_000_000)
    page_cache_memory = Keyword.get(args, :page_cache_memory, 1_000_000_000)

    dets =
      Enum.map(1..@shard_count, fn index ->
        file = Path.join(directory, "partition_#{index}")
        name = String.to_atom(file)

        {:ok, det} =
          DetsPlus.open_file(
            name,
            Keyword.merge(args,
              file: file,
              page_cache_memory: div(page_cache_memory, @shard_count),
              auto_save_memory: div(auto_save_memory, @shard_count),
              auto_save: :never
            )
          )

        det
      end)
      |> List.to_tuple()

    keypos = DetsPlus.info(elem(dets, 0), :keypos)

    state =
      %__MODULE__{keypos: keypos, dets: dets}
      |> init_hashfuns()

    {:ok, state}
  end

  defp init_hashfuns(state = %__MODULE__{keypos: keypos}) when is_integer(keypos) do
    %__MODULE__{
      state
      | keyfun: fn tuple -> elem(tuple, keypos - 1) end,
        keyhashfun: &default_hash/1,
        hashfun: fn tuple -> default_hash(elem(tuple, keypos - 1)) end
    }
  end

  defp init_hashfuns(state = %__MODULE__{keypos: keypos}) when is_atom(keypos) do
    %__MODULE__{
      state
      | keyfun: fn map -> Map.get(map, keypos) end,
        keyhashfun: &default_hash/1,
        hashfun: fn map -> default_hash(Map.get(map, keypos)) end
    }
  end

  @hash_size 8
  defp default_hash(key) do
    <<hash::binary-size(@hash_size), _::binary>> =
      :crypto.hash(:sha256, :erlang.term_to_binary(key))

    hash
  end

  @doc """
  Syncs pending writes to the persistent file and closes the table.
  """
  def close(kdb), do: all(kdb, :close)

  @doc """
  Deletes all objects from a table in almost constant time.
  """
  def delete_all_objects(kdb), do: all(kdb, :delete_all_objects)

  defp all(%__MODULE__{dets: dets}, method) when is_atom(method) do
    Tuple.to_list(dets)
    |> Enum.chunk_every(64)
    |> Task.async_stream(
      fn dets ->
        for det <- dets do
          apply(DetsPlus, method, [det])
        end
      end,
      ordered: false,
      timeout: :infinity
    )
    |> Stream.run()

    :ok
  end

  @prefix_size @hash_size - 1
  defp one(%__MODULE__{dets: dets, hashfun: hashfun}, method, object)
       when is_atom(method) and (is_tuple(object) or is_map(object)) do
    <<_::binary-size(@prefix_size), idx>> = hashfun.(object)
    det = elem(dets, idx)
    apply(DetsPlus, method, [det, object])
  end

  defp onekey(%__MODULE__{dets: dets, keyhashfun: keyhashfun}, method, key)
       when is_atom(method) do
    <<_::binary-size(@prefix_size), idx>> = keyhashfun.(key)
    det = elem(dets, idx)
    apply(DetsPlus, method, [det, key])
  end

  @doc """
  Deletes all instances of a specified object from a table.
  """
  def delete_object(kdb, object), do: one(kdb, :delete_object, object)

  @doc """
  Deletes all objects with key Key from table Name.
  """
  def delete(kdb, key), do: onekey(kdb, :delete, key)

  @doc """
    Inserts one or more objects into the table. If there already exists an object with a key matching the key of some of the given objects, the old object will be replaced.
  """
  def insert(kdb, objects) do
    List.wrap(objects)
    |> Enum.each(fn object ->
      one(kdb, :insert, object)
    end)
  end

  @doc """
  Inserts one object into the table if it doesn't already exists.
  """
  def insert_new(kdb, object), do: one(kdb, :insert_new, object)

  @doc """
  Returns the number of object in the table. This is an estimate.
  """
  def count(kdb), do: info(kdb, :size)

  @doc """
  Reducer function following the `Enum` protocol.
  """
  def reduce(kdb = %__MODULE__{}, acc, fun) do
    Enum.reduce(kdb, acc, fun)
  end

  @doc """
  Returns a list of all objects with key Key stored in the table.

  Example:

  ```
  2> KrakenDB.open_directory(:abc)
  {ok,:abc}
  3> KrakenDB.insert(:abc, {1,2,3})
  ok
  4> KrakenDB.insert(:abc, {1,3,4})
  ok
  5> KrakenDB.lookup(:abc, 1).
  [{1,3,4}]
  ```

  If the table type is set, the function returns either the empty list or a list with one object, as there cannot be more than one object with a given key. If the table type is bag or duplicate_bag, the function returns a list of arbitrary length.

  Notice that the order of objects returned is unspecified. In particular, the order in which objects were inserted is not reflected.
  """
  def lookup(kdb, key), do: onekey(kdb, :lookup, key)

  @doc """
  Works like `lookup/2`, but does not return the objects. Returns true if one or more table elements has the key `key`, otherwise false.
  """
  def member?(kdb, key), do: onekey(kdb, :member?, key)

  @doc """
  Ensures that all updates made to table are written to disk. While the sync is running the
  table can still be used for reads and writes, but writes issued after the `sync/1` call
  will not be part of the persistent file. These new changes will only be included in the
  next sync call.
  """
  def sync(kdb), do: all(kdb, :sync)

  @doc """
  Starts a sync of all changes to the disk. Same as `sync/1` but doesn't block
  """
  def start_sync(kdb), do: all(kdb, :start_sync)

  @doc """
  Returns information about table Name as a list of objects:

  - `{file_size, integer() >= 0}}` - The file size sum, in bytes.
  - `{directory, file:name()}` - The name of the directory where objects are stored.
  - `{keypos, keypos()}` - The key position.
  - `{size, integer() >= 0}` - The number of objects estimated in the table.
  - `{type, type()}` - The table type.
  """
  def info(%__MODULE__{dets: dets, keypos: keypos}) do
    Tuple.to_list(dets)
    |> Enum.flat_map(&DetsPlus.info/1)
    |> Enum.reduce(%{keypos: keypos}, fn {key, value}, info ->
      case {key, Map.get(info, key)} do
        {:keypos, _pos} -> info
        {:filename, name} -> Map.put(info, :directory, Path.dirname("#{name}"))
        {key, nil} -> Map.put(info, key, value)
        {key, num} when is_integer(num) -> Map.put(info, key, num + (value || 0))
        {key, atom} when is_atom(atom) -> Map.put(info, key, atom)
        _ -> info
      end
    end)
    |> Map.to_list()
  end

  @doc """
  Returns the information associated with `item` for the table. The following items are allowed:

  - `{file_size, integer() >= 0}}` - The file size, in bytes.
  - `{header_size, integer() >= 0}}` - The size of erlang term encoded header.
  - `{bloom_bytes, integer() >= 0}}` - The size of the in-memory and on-disk bloom filter, in bytes.
  - `{hashtable_bytes, integer() >= 0}}` - The size of the on-disk lookup hashtable, in bytes.
  - `{filename, file:name()}` - The name of the file where objects are stored.
  - `{keypos, keypos()}` - The key position.
  - `{size, integer() >= 0}` - The number of objects estimated in the table.
  - `{type, type()}` - The table type.
  """
  @spec info(
          KrakenDB.t(),
          :file_size
          | :header_size
          | :directory
          | :keypos
          | :size
          | :type
          | :creation_stats
          | :bloom_bytes
          | :hashtable_bytes
        ) ::
          any()
  def info(dets, item)
      when item == :file_size or item == :directory or item == :keypos or item == :size or
             item == :type or item == :creation_stats do
    Keyword.get(info(dets), item)
  end
end

defimpl Enumerable, for: KrakenDB do
  def count(_pid), do: {:error, __MODULE__}
  def member?(_pid, _key), do: {:error, __MODULE__}
  def slice(_pid), do: {:error, __MODULE__}

  def reduce(%KrakenDB{dets: dets}, acc, fun) do
    Tuple.to_list(dets)
    |> Enum.reduce(acc, fn det, acc ->
      case acc do
        {:done, acc} -> Enumerable.reduce(det, {:cont, acc}, fun)
        {:cont, acc} -> Enumerable.reduce(det, {:cont, acc}, fun)
        {:halt, acc} -> {:halted, acc}
      end
    end)
  end
end
