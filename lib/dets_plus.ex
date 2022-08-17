defmodule DetsPlus do
  @moduledoc """
  DetsPlus persistent tuple/struct/map storage.

  [DetsPlus](https://github.com/dominicletz/dets_plus) has a similiar API as `dets` but without
  the 2GB file storage limit. Writes are buffered in an
  internal ETS table and synced every `auto_save` period
  to the persistent storage.

  While `sync()` or `auto_save` is in progress the database
  can still read and written.

  There is no commitlog so not synced writes are lost.
  Lookups are possible by key and non-matches are accelerated
  using a bloom filter. The persistent file concept follows
  DJ Bernsteins CDB database format, but uses an Elixir
  encoded header https://cr.yp.to/cdb.html

  Limits are:
  - Total file size: 18_446 Petabyte
  - Maximum entry size: 4 Gigabyte
  - Maximum entry count: :infinity
  """

  # This limits the total database size to 18_446 PB
  @slot_size 8
  @slot_size_bits @slot_size * 8

  # This limits the biggest entry size to 4 GB
  @entry_size_size 4
  @entry_size_size_bits @entry_size_size * 8

  # We're using sha256 as source - should not have conflicts ever
  @hash_size 8
  @hash_size_bits @hash_size * 8

  # Tuples for use in the hash tables
  @null_tuple {<<0::unsigned-size(64)>>, 0}
  @null_binary <<0::unsigned-size(128)>>

  @version 3

  alias DetsPlus.{Bloom, EntryWriter, FileReader, FileWriter}
  use GenServer
  require Logger

  defstruct [:pid, :ets, :keyfun, :keyhashfun, :hashfun]

  @type t :: %__MODULE__{pid: pid()}

  defmodule State do
    @moduledoc false
    @enforce_keys [:version]
    defstruct [
      :version,
      :bloom,
      :bloom_size,
      :filename,
      :fp,
      :name,
      :ets,
      :mode,
      :auto_save,
      :keypos,
      :keyfun,
      :keyhashfun,
      :hashfun,
      :type,
      :file_entries,
      :slot_counts,
      :table_offsets,
      :sync,
      :sync_waiters,
      :file_size,
      :sync_fallback,
      :creation_stats
    ]
  end

  @doc """
    Opens an existing table or creates a new table. If no
    `file` argument is provided the table name will be used.

    Arguments:

    - `auto_save` - The autosave interval. If the interval is an integer Time, the table is flushed to disk whenever it is not accessed for Time milliseconds. If the interval is the atom infinity, autosave is disabled. Defaults to `180_000` (3 minutes).
    - `keypos` - The position of the element of each object to be used as key. Defaults to 1. The ability to explicitly state the key position is most convenient when we want to store Erlang records in which the first position of the record is the name of the record type.
  """
  def open_file(name, args \\ []) when is_atom(name) do
    filename = Keyword.get(args, :file, name) |> do_string()

    state =
      with true <- File.exists?(filename),
           {:ok, %File.Stat{size: file_size}} when file_size > 0 <- File.stat(filename) do
        load_state(filename, file_size)
      else
        _ ->
          mode = Keyword.get(args, :access, :read_write)
          auto_save = Keyword.get(args, :auto_save, 180_000)
          keypos = Keyword.get(args, :keypos, 1)
          type = Keyword.get(args, :type, :set)

          # unused options from dets:
          # - ram_file
          # - max_no_slots
          # - min_no_slots
          # - repair

          %State{
            version: @version,
            bloom: "",
            bloom_size: 0,
            filename: filename,
            fp: nil,
            name: name,
            mode: mode,
            auto_save: auto_save,
            keypos: keypos,
            type: type,
            file_entries: 0,
            slot_counts: %{},
            file_size: 0,
            sync: nil,
            sync_waiters: [],
            sync_fallback: %{}
          }
      end
      |> init_hashfuns()

    case GenServer.start_link(__MODULE__, state, hibernate_after: 5_000, name: name) do
      {:ok, pid} -> {:ok, GenServer.call(pid, :get_handle)}
      err -> err
    end
  end

  defp init_hashfuns(state = %State{keypos: keypos}) when is_integer(keypos) do
    %State{
      state
      | keyfun: fn tuple -> elem(tuple, keypos - 1) end,
        keyhashfun: &default_hash/1,
        hashfun: fn tuple -> default_hash(elem(tuple, keypos - 1)) end
    }
  end

  defp init_hashfuns(state = %State{keypos: keypos}) when is_atom(keypos) do
    %State{
      state
      | keyfun: fn map -> Map.get(map, keypos) end,
        keyhashfun: &default_hash/1,
        hashfun: fn map -> default_hash(Map.get(map, keypos)) end
    }
  end

  defp load_state(filename, file_size) do
    fp = file_open(filename)

    {:ok, <<header_offset::unsigned-size(@slot_size_bits)>>} =
      PagedFile.pread(fp, file_size - @slot_size, @slot_size)

    {:ok, header} = PagedFile.pread(fp, header_offset, file_size - header_offset - @slot_size)
    %State{version: version, bloom: bloom} = state = :erlang.binary_to_term(header)

    if version != @version do
      raise("incompatible dets+ version #{version}")
    end

    state = %State{
      state
      | fp: fp,
        file_size: file_size
    }

    if is_binary(bloom) do
      state
    else
      {:ok, bloom} = PagedFile.pread(fp, bloom, header_offset - bloom)
      %State{state | bloom: bloom}
    end
  end

  @wfile PagedFile
  defp store_state(state = %State{fp: fp, bloom: bloom}) do
    bloom_offset = @wfile.size(fp)
    @wfile.pwrite(fp, bloom_offset, bloom)
    header_offset = @wfile.size(fp)

    bin =
      :erlang.term_to_binary(
        %State{
          state
          | version: @version,
            bloom: bloom_offset,
            fp: nil,
            sync: nil,
            sync_waiters: [],
            ets: nil
        },
        [:compressed]
      )

    @wfile.pwrite(fp, header_offset, bin <> <<header_offset::unsigned-size(@slot_size_bits)>>)
    %State{state | file_size: @wfile.size(fp)}
  end

  @impl true
  def init(state = %State{auto_save: auto_save}) do
    if is_integer(auto_save) do
      :timer.send_interval(auto_save, :auto_save)
    end

    {:ok, init_ets(state)}
  end

  defp init_ets(state = %State{name: name, type: type}) do
    %State{state | ets: :ets.new(name, [type])}
  end

  defp init_table_offsets(state = %State{slot_counts: slot_counts}, start_offset) do
    table_offsets =
      Enum.reduce(1..256, %{0 => start_offset}, fn table_idx, table_offsets ->
        offset =
          Map.get(table_offsets, table_idx - 1) +
            Map.get(slot_counts, table_idx - 1, 0) * (@slot_size + @hash_size)

        Map.put(table_offsets, table_idx, offset)
      end)

    %State{state | table_offsets: table_offsets}
  end

  @doc """
  Syncs pending writes to the persistent file and closes the table.
  """
  @spec close(DetsPlus.t()) :: :ok
  def close(%__MODULE__{pid: pid}), do: close(pid)

  def close(pid) when is_pid(pid) or is_atom(pid) do
    call(pid, :sync)
    GenServer.stop(pid)
  end

  @doc """
  Deletes all objects from a table in almost constant time.
  """
  @spec delete_all_objects(DetsPlus.t()) :: :ok | {:error, atom()}
  def delete_all_objects(%__MODULE__{pid: pid}), do: delete_all_objects(pid)

  def delete_all_objects(pid) when is_pid(pid) or is_atom(pid) do
    call(pid, :delete_all_objects)
  end

  @doc """
    Inserts one or more objects into the table. If there already exists an object with a key matching the key of some of the given objects, the old object will be replaced.
  """
  @spec insert(DetsPlus.t(), tuple() | map() | [tuple() | map()]) :: :ok | {:error, atom()}
  def insert(%__MODULE__{pid: pid}, objects), do: insert(pid, objects)

  def insert(pid, objects) do
    call(pid, {:insert, List.wrap(objects)})
  end

  @doc """
  Inserts one or more objects into the table. If there already exists an object with a key matching the key of some of the given objects, the old object will be replaced.
  """
  @spec insert_new(DetsPlus.t(), tuple() | map() | [tuple() | map()]) :: true | false
  def insert_new(pid, object) when is_pid(pid) or is_atom(pid) do
    call(pid, :get_handle)
    |> insert_new(object)
  end

  def insert_new(%__MODULE__{pid: pid, hashfun: hashfun}, object) do
    objects =
      List.wrap(object)
      |> Enum.map(fn object -> {object, hashfun.(object)} end)

    case call(pid, {:insert_new, objects}) do
      :ok -> true
      false -> false
    end
  end

  @doc """
  Returns the number of object in the table. This is an estimate and the same as `info(dets, :size)`.
  """
  @spec count(DetsPlus.t()) :: integer()
  def count(pid) when is_pid(pid) or is_atom(pid), do: count(call(pid, :get_handle))

  def count(dets = %__MODULE__{}) do
    info(dets, :size)
  end

  @doc """
  Reducer function following the `Enum` protocol.
  """
  @spec reduce(DetsPlus.t(), any(), fun()) :: any()
  def reduce(pid, acc, fun) when is_pid(pid) or is_atom(pid),
    do: reduce(call(pid, :get_handle), acc, fun)

  def reduce(dets = %__MODULE__{}, acc, fun) do
    Enum.reduce(dets, acc, fun)
  end

  @doc """
  Returns a list of all objects with key Key stored in the table.

  Example:

  ```
  2> State.open_file(:abc)
  {ok,:abc}
  3> State.insert(:abc, {1,2,3})
  ok
  4> State.insert(:abc, {1,3,4})
  ok
  5> State.lookup(:abc, 1).
  [{1,3,4}]
  ```

  If the table type is set, the function returns either the empty list or a list with one object, as there cannot be more than one object with a given key. If the table type is bag or duplicate_bag, the function returns a list of arbitrary length.

  Notice that the order of objects returned is unspecified. In particular, the order in which objects were inserted is not reflected.
  """
  @spec lookup(DetsPlus.t(), any) :: [tuple() | map()] | {:error, atom()}
  def lookup(pid, key) when is_pid(pid) or is_atom(pid), do: lookup(call(pid, :get_handle), key)

  def lookup(%__MODULE__{pid: pid, keyhashfun: keyhashfun}, key) do
    call(pid, {:lookup, key, keyhashfun.(key)})
  end

  @doc """
  Works like `lookup/2`, but does not return the objects. Returns true if one or more table elements has the key `key`, otherwise false.
  """
  @spec member?(DetsPlus.t(), any) :: false | true | {:error, atom}
  def member?(dets, key) do
    case lookup(dets, key) do
      [] -> false
      list when is_list(list) -> true
      error -> error
    end
  end

  @doc """
  Same as `member?/2`
  """
  @spec member(DetsPlus.t(), any) :: false | true | {:error, atom}
  def member(dets, key) do
    member?(dets, key)
  end

  @doc """
  Ensures that all updates made to table are written to disk. While the sync is running the
  table can still be used for reads and writes, but writes issed after the `sync/1` call
  will not be part of the persistent file. These new changes will only be included in the
  next sync call.
  """
  @spec sync(DetsPlus.t()) :: :ok
  def sync(%__MODULE__{pid: pid}), do: sync(pid)

  def sync(pid) when is_pid(pid) or is_atom(pid) do
    call(pid, :sync, :infinity)
  end

  @doc """
  Starts a sync of all changes to the disk. Same as `sync/1` but doesn't block
  """
  @spec start_sync(DetsPlus.t()) :: :ok
  def start_sync(%__MODULE__{pid: pid}), do: start_sync(pid)

  def start_sync(pid) when is_pid(pid) or is_atom(pid) do
    call(pid, :start_sync, :infinity)
  end

  @doc """
  Returns information about table Name as a list of objects:

  - `{file_size, integer() >= 0}}` - The file size, in bytes.
  - `{filename, file:name()}` - The name of the file where objects are stored.
  - `{keypos, keypos()}` - The key position.
  - `{size, integer() >= 0}` - The number of objects estimated in the table.
  - `{type, type()}` - The table type.
  """
  @spec info(DetsPlus.t()) :: [] | nil
  def info(%__MODULE__{pid: pid}), do: info(pid)

  def info(pid) when is_pid(pid) or is_atom(pid) do
    call(pid, :info)
  end

  @doc """
  Returns the information associated with `item` for the table. The following items are allowed:

  - `{file_size, integer() >= 0}}` - The file size, in bytes.
  - `{bloom_bytes, integer() >= 0}}` - The size of the in-memory and on-disk bloom filter, in bytes.
  - `{hashtable_bytes, integer() >= 0}}` - The size of the on-disk lookup hashtable, in bytes.
  - `{filename, file:name()}` - The name of the file where objects are stored.
  - `{keypos, keypos()}` - The key position.
  - `{size, integer() >= 0}` - The number of objects estimated in the table.
  - `{type, type()}` - The table type.
  """
  @spec info(
          DetsPlus.t(),
          :file_size
          | :filename
          | :keypos
          | :size
          | :type
          | :creation_stats
          | :bloom_bytes
          | :hashtable_bytes
        ) ::
          any()
  def info(dets, item)
      when item == :file_size or item == :filename or item == :keypos or item == :size or
             item == :type or item == :creation_stats do
    case info(dets) do
      nil -> nil
      list -> Keyword.get(list, item)
    end
  end

  defp call(pid, cmd, timeout \\ :infinity) do
    GenServer.call(pid, cmd, timeout)
  end

  @impl true
  def handle_call(
        :get_handle,
        _from,
        state = %State{ets: ets, keyfun: keyfun, keyhashfun: keyhashfun, hashfun: hashfun}
      ) do
    {:reply,
     %DetsPlus{pid: self(), ets: ets, keyfun: keyfun, keyhashfun: keyhashfun, hashfun: hashfun},
     state}
  end

  def handle_call(
        :delete_all_objects,
        _from,
        state = %State{fp: fp, ets: ets, sync: sync, sync_waiters: waiters, filename: filename}
      ) do
    :ets.delete_all_objects(ets)

    if is_pid(sync) do
      Process.unlink(sync)
      Process.exit(sync, :kill)
    end

    for w <- waiters do
      :ok = GenServer.reply(w, :ok)
    end

    if fp != nil do
      PagedFile.close(fp)
    end

    PagedFile.delete(filename)

    {:reply, :ok,
     %State{
       state
       | sync: nil,
         sync_waiters: [],
         sync_fallback: %{},
         fp: nil,
         bloom: "",
         bloom_size: 0,
         file_entries: 0,
         slot_counts: %{},
         file_size: 0
     }}
  end

  def handle_call(
        :get_reduce_state,
        _from,
        state = %State{sync_fallback: fallback, filename: filename, hashfun: hash_fun}
      ) do
    new_data2 = Map.to_list(fallback)
    {:reply, {new_data2, filename, hash_fun}, state}
  end

  def handle_call(
        {:insert, objects},
        from,
        state = %State{
          ets: ets,
          sync: sync,
          sync_fallback: fallback,
          sync_waiters: sync_waiters,
          keyfun: keyfun
        }
      ) do
    if sync == nil do
      :ets.insert(ets, Enum.map(objects, fn object -> {keyfun.(object), object} end))
      {:reply, :ok, check_auto_save(state)}
    else
      fallback =
        Enum.reduce(objects, fallback, fn object, fallback ->
          Map.put(fallback, keyfun.(object), object)
        end)

      if map_size(fallback) > 1_000_000 do
        # this pause exists to protect from out_of_memory situations when the writer can't
        # finish in time
        Logger.warn(
          "State flush slower than new inserts - pausing writes until flush is complete"
        )

        {:noreply, %State{state | sync_fallback: fallback, sync_waiters: [from | sync_waiters]}}
      else
        {:reply, :ok, %State{state | sync_fallback: fallback}}
      end
    end
  end

  def handle_call(
        {:insert_new, objects},
        from,
        state = %State{ets: ets, sync_fallback: fallback, keyfun: keyfun}
      ) do
    exists =
      Enum.any?(objects, fn {object, hash} ->
        key = keyfun.(object)

        Map.has_key?(fallback, key) || :ets.lookup(ets, key) != [] ||
          file_lookup(state, key, hash) != []
      end)

    if exists do
      {:reply, false, state}
    else
      handle_call({:insert, objects}, from, state)
    end
  end

  def handle_call(
        {:lookup, key, hash},
        _from,
        state = %State{ets: ets, sync_fallback: fallback}
      ) do
    case Map.get(fallback, key) do
      nil ->
        case :ets.lookup(ets, key) do
          [] -> {:reply, file_lookup(state, key, hash), state}
          [{^key, object}] -> {:reply, [object], state}
        end

      value ->
        {:reply, [value], state}
    end
  end

  def handle_call(
        :info,
        _from,
        state = %State{
          bloom: bloom,
          filename: filename,
          keypos: keypos,
          type: type,
          ets: ets,
          file_size: file_size,
          file_entries: file_entries,
          creation_stats: creation_stats,
          slot_counts: slot_counts
        }
      ) do
    size = :ets.info(ets, :size) + file_entries
    table_size = Enum.sum(Map.values(slot_counts)) * @slot_size + @hash_size

    info = [
      file_size: file_size,
      bloom_bytes: byte_size(bloom),
      hashtable_bytes: table_size,
      filename: filename,
      keypos: keypos,
      size: size,
      type: type,
      creation_stats: creation_stats
    ]

    {:reply, info, state}
  end

  def handle_call(:start_sync, _from, state = %State{sync: sync, ets: ets}) do
    sync =
      if sync == nil and :ets.info(ets, :size) > 0 do
        spawn_sync_worker(state)
      else
        sync
      end

    {:reply, :ok, %State{state | sync: sync}}
  end

  def handle_call(:sync, from, state = %State{sync: nil, ets: ets}) do
    if :ets.info(ets, :size) == 0 do
      {:reply, :ok, state}
    else
      sync = spawn_sync_worker(state)
      {:noreply, %State{state | sync: sync, sync_waiters: [from]}}
    end
  end

  def handle_call(:sync, from, state = %State{sync: sync, sync_waiters: sync_waiters})
      when is_pid(sync) do
    {:noreply, %State{state | sync_waiters: [from | sync_waiters]}}
  end

  @impl true
  def handle_cast(
        {:sync_complete, sync_pid, new_filename, new_state = %State{}},
        %State{
          fp: fp,
          filename: filename,
          ets: ets,
          sync: sync_pid,
          sync_waiters: waiters,
          sync_fallback: fallback
        }
      ) do
    if fp != nil do
      :ok = PagedFile.close(fp)
    end

    File.rename!(new_filename, filename)
    fp = file_open(filename)

    :ets.delete_all_objects(ets)
    :ets.insert(ets, Map.to_list(fallback))

    for w <- waiters do
      :ok = GenServer.reply(w, :ok)
    end

    {:noreply, %State{new_state | fp: fp, sync: nil, sync_fallback: %{}, sync_waiters: []}}
  end

  # this is pending sync finishing while a complete delete_all_objects has been executed on the state
  def handle_cast({:sync_complete, _sync_pid, _new_filename, _new_state}, state = %State{}) do
    {:noreply, state}
  end

  @impl true
  def handle_info(:auto_save, state = %State{sync: sync, ets: ets}) do
    sync =
      if sync == nil and :ets.info(ets, :size) > 0 do
        spawn_sync_worker(state)
      else
        sync
      end

    {:noreply, %State{state | sync: sync}}
  end

  defp check_auto_save(state) do
    state
  end

  defp add_stats({prev, stats}, label) do
    now = :erlang.timestamp()
    elapsed = div(:timer.now_diff(now, prev), 1000)
    # IO.puts("#{label} #{elapsed}ms")
    {now, [{label, elapsed} | stats]}
  end

  defp spawn_sync_worker(
         state = %State{
           ets: ets,
           fp: fp,
           filename: filename,
           file_entries: file_entries,
           hashfun: hash_fun
         }
       ) do
    # assumptions here
    # 1. ets data set is small enough to fit into memory
    # 2. fp entries are sorted by hash
    dets = self()

    worker =
      spawn_link(fn ->
        stats = {:erlang.timestamp(), []}
        new_dataset = :ets.tab2list(ets)
        stats = add_stats(stats, :ets_flush)
        send(dets, :continue)

        # Ensuring hash function sort order
        new_dataset =
          parallel_hash(hash_fun, new_dataset)
          |> Enum.sort_by(fn {hash, _object} -> hash end, :asc)

        stats = add_stats(stats, :ets_sort)

        old_file =
          if fp != nil do
            FileReader.new(fp, byte_size("DET+"), module: PagedFile, buffer_size: 512_000)
          else
            nil
          end

        new_filename = "#{filename}.tmp"
        @wfile.delete(new_filename)

        # ~5mb for this write buffer, it's mostly append only, higher values
        # didn't make an impact.
        # (the only non-append workflow on this fp is the hash overflow handler, but that is usually small)
        opts = [page_size: 512_000, max_pages: 10]
        {:ok, new_file} = @wfile.open(new_filename, opts)
        state = %State{state | fp: new_file}
        stats = add_stats(stats, :fopen)
        new_dataset_length = length(new_dataset)

        # setting the bloom size based of a size estimate
        future_bloom = Task.async(fn -> write_bloom(file_entries + new_dataset_length) end)
        future_entries = Task.async(fn -> write_entries(state, new_dataset_length) end)
        future_state = Task.async(fn -> write_data(state) end)

        # This starts of the file_reader sending entry data to above the future_* workers
        pids = [future_bloom.pid, future_entries.pid, future_state.pid]
        async_iterate_produce(hash_fun, new_dataset, old_file, pids)

        state = Task.await(future_state, :infinity)
        {bloom, bloom_size} = Task.await(future_bloom, :infinity)
        state = %State{state | bloom: bloom, bloom_size: bloom_size}

        entries = Task.await(future_entries, :infinity)

        stats = add_stats(stats, :write_entries)
        state = write_hashtable(state, entries)
        stats = add_stats(stats, :write_hashtable)
        state = store_state(state)
        stats = add_stats(stats, :header_store)

        @wfile.close(new_file)
        {_, stats} = add_stats(stats, :file_close)

        state = %State{state | creation_stats: stats}
        GenServer.cast(dets, {:sync_complete, self(), new_filename, state})
      end)

    # Profiler.fprof(worker)

    receive do
      :continue ->
        :erlang.garbage_collect()
        worker
    end
  end

  @min_chunk_size 10_000
  @max_tasks 4
  @doc false
  def parallel_hash(hashfun, new_dataset, tasks \\ 1) do
    len = length(new_dataset)

    if len > @min_chunk_size and tasks < @max_tasks do
      {a, b} = Enum.split(new_dataset, div(len, 2))
      task = Task.async(fn -> parallel_hash(hashfun, a, tasks * 2) end)
      result = parallel_hash(hashfun, b, tasks * 2)
      Task.await(task, :infinity) ++ result
    else
      Enum.map(new_dataset, fn {_key, object} ->
        {hashfun.(object), object}
      end)
    end
  end

  defp write_bloom(estimated_file_entries) do
    bloom_size = estimated_file_entries * 10

    Bloom.create(bloom_size)
    |> async_iterate_consume(fn bloom, entry_hash, _entry, _ ->
      {:cont, Bloom.add(bloom, entry_hash)}
    end)
    |> Bloom.finalize()
  end

  defp write_entries(
         %State{file_entries: old_file_entries, filename: filename},
         new_dataset_length
       ) do
    estimated_entry_count = new_dataset_length + old_file_entries
    entries = EntryWriter.new(filename, estimated_entry_count)

    {entries, _offset} =
      async_iterate_consume({entries, 4}, fn {entries, offset}, entry_hash, entry, _ ->
        size = byte_size(entry)
        table_idx = table_idx(entry_hash)

        entries =
          EntryWriter.insert(
            entries,
            {table_idx, entry_hash, offset + @hash_size}
          )

        offset = offset + @hash_size + @entry_size_size + size
        {:cont, {entries, offset}}
      end)

    entries
  end

  defp write_data(state = %State{fp: fp}) do
    state = %State{state | file_entries: 0, slot_counts: %{}}
    writer = FileWriter.new(fp, 0, module: @wfile)
    writer = FileWriter.write(writer, "DET+")

    {state, writer} =
      async_iterate_consume(
        {state, writer},
        fn {state = %State{file_entries: file_entries, slot_counts: slot_counts}, writer},
           entry_hash,
           entry,
           _ ->
          table_idx = table_idx(entry_hash)
          slot_counts = Map.update(slot_counts, table_idx, 1, fn count -> count + 1 end)
          state = %State{state | file_entries: file_entries + 1, slot_counts: slot_counts}
          size = byte_size(entry)

          writer =
            FileWriter.write(
              writer,
              <<entry_hash::binary-size(@hash_size), size::unsigned-size(@entry_size_size_bits),
                entry::binary()>>
            )

          {:cont, {state, writer}}
        end
      )

    # final zero offset after all data entries
    FileWriter.write(
      writer,
      <<0::unsigned-size(@hash_size_bits), 0::unsigned-size(@entry_size_size_bits)>>
    )
    |> FileWriter.sync()

    state
  end

  _ =
    Enum.reduce(1..56, {[], 2}, fn bits, {code, size} ->
      next =
        defp slot_idx(unquote(size), <<_, slot::unsigned-size(unquote(bits)), _::bitstring()>>) do
          slot
        end

      {[next | code], size * 2}
    end)
    |> elem(0)

  defp write_hashtable(state = %State{slot_counts: slot_counts, fp: fp}, entries) do
    new_slot_counts =
      Enum.map(slot_counts, fn {key, value} ->
        {key, next_power_of_two(trunc(value * 1.3) + 1)}
      end)
      |> Map.new()

    state =
      %State{state | slot_counts: new_slot_counts}
      |> init_table_offsets(@wfile.size(fp))

    writer = FileWriter.new(fp, @wfile.size(fp), module: @wfile)

    Enum.reduce(0..255, writer, fn table_idx, writer ->
      slot_count = Map.get(new_slot_counts, table_idx, 0)
      entries = EntryWriter.lookup(entries, table_idx)
      start_offset = FileWriter.offset(writer)
      {writer, overflow} = reduce_entries(entries, writer, slot_count)

      if overflow == [] do
        writer
      else
        writer = FileWriter.sync(writer)

        reduce_overflow(
          Enum.reverse(overflow),
          FileReader.new(fp, start_offset, module: @wfile),
          fp
        )

        writer
      end
    end)
    |> FileWriter.sync()

    EntryWriter.close(entries)
    state
  end

  defp next_power_of_two(n), do: next_power_of_two(n, 2)
  defp next_power_of_two(n, x) when n < x, do: x
  defp next_power_of_two(n, x), do: next_power_of_two(n, x * 2)

  defp reduce_entries(entries, writer, slot_count),
    do: reduce_entries(entries, writer, slot_count, -1, [])

  defp reduce_entries([], writer, slot_count, last_slot, overflow)
       when last_slot + 1 == slot_count,
       do: {writer, overflow}

  defp reduce_entries([], writer, slot_count, last_slot, overflow) do
    writer = FileWriter.write(writer, @null_binary)
    reduce_entries([], writer, slot_count, last_slot + 1, overflow)
  end

  defp reduce_entries(
         [
           <<entry_hash::binary-size(8), _offset::unsigned-size(@slot_size_bits)>> = entry
           | entries
         ],
         writer,
         slot_count,
         last_slot,
         overflow
       ) do
    slot_idx = slot_idx(slot_count, entry_hash)

    cond do
      last_slot + 1 == slot_count ->
        reduce_entries(entries, writer, slot_count, last_slot, [entry | overflow])

      last_slot + 1 >= slot_idx ->
        writer = FileWriter.write(writer, entry)
        reduce_entries(entries, writer, slot_count, last_slot + 1, overflow)

      true ->
        writer = FileWriter.write(writer, @null_binary)
        reduce_entries([entry | entries], writer, slot_count, last_slot + 1, overflow)
    end
  end

  defp reduce_overflow([], _reader, _fp), do: :ok

  defp reduce_overflow([entry | overflow], reader, fp) do
    curr = FileReader.offset(reader)
    {reader, next} = FileReader.read(reader, @slot_size + @hash_size)

    case next do
      @null_binary ->
        @wfile.pwrite(fp, curr, entry)
        reduce_overflow(overflow, reader, fp)

      _ ->
        reduce_overflow([entry | overflow], reader, fp)
    end
  end

  def async_iterate_produce(
        hash_fun,
        new_dataset,
        file_reader,
        targets
      ) do
    spawn_link(fn ->
      init_acc = {:cont, {[], 0, targets}}

      {:done, {items, _item_count, _targets}} =
        iterate(hash_fun, init_acc, new_dataset, file_reader, &async_iterator/4)

      for pid <- targets do
        send(pid, {:entries, Enum.reverse(items)})
        send(pid, :done)
      end
    end)
  end

  defp async_iterator({items, item_count, targets}, entry_hash, binary_entry, term_entry) do
    items = [{entry_hash, binary_entry, term_entry} | items]
    item_count = item_count + 1

    if item_count > 128 do
      for pid <- targets do
        send(pid, {:entries, Enum.reverse(items)})
      end

      {:cont, {[], 0, targets}}
    else
      {:cont, {items, item_count, targets}}
    end
  end

  def async_iterate_consume(acc, fun) do
    receive do
      {:entries, entries} ->
        Enum.reduce(entries, acc, fn {entry_hash, binary_entry, term_entry}, acc ->
          {:cont, acc} = fun.(acc, entry_hash, binary_entry, term_entry)
          acc
        end)
        |> async_iterate_consume(fun)

      :done ->
        acc
    end
  end

  # this function takes a new_dataset and an old file and merges them, it calls
  # on every entry the callback `fun.(acc, entry_hash, binary_entry, term_entry)` and returns the final acc
  @doc false
  def iterate(_hash_fun, {:halt, acc}, _new_dataset, _file_reader, _fun) do
    {:halted, acc}
  end

  def iterate(
        hash_fun,
        {:cont, acc},
        new_dataset,
        file_reader,
        fun
      ) do
    # Reading a new entry from the top of the dataset or nil
    {new_entry, new_entry_hash} =
      case new_dataset do
        [{new_entry_hash, new_entry} | _rest] ->
          {new_entry, new_entry_hash}

        [] ->
          {nil, nil}
      end

    # Reading a new entry from the file or falling back to else if there is no next
    # entry in the file anymore
    with false <- file_reader == nil,
         {new_file_reader,
          <<entry_hash::binary-size(@hash_size), old_size::unsigned-size(@entry_size_size_bits)>>}
         when old_size > 0 <-
           FileReader.read(file_reader, @hash_size + @entry_size_size) do
      {new_file_reader, old_entry} = FileReader.read(new_file_reader, old_size)

      case new_entry_hash do
        # reached end of new_dataset
        nil ->
          iterate(hash_fun, fun.(acc, entry_hash, old_entry, nil), [], new_file_reader, fun)

        # replacing an old entry with a new entry
        ^entry_hash ->
          # LATER - ensure there is no hash collision
          bin = :erlang.term_to_binary(new_entry)

          iterate(
            hash_fun,
            fun.(acc, new_entry_hash, bin, new_entry),
            tl(new_dataset),
            new_file_reader,
            fun
          )

        # inserting the new entry before the old
        new_entry_hash when new_entry_hash < entry_hash ->
          bin = :erlang.term_to_binary(new_entry)

          iterate(
            hash_fun,
            fun.(acc, new_entry_hash, bin, new_entry),
            tl(new_dataset),
            file_reader,
            fun
          )

        # inserting the old entry before the new
        new_entry_hash when new_entry_hash > entry_hash ->
          iterate(
            hash_fun,
            fun.(acc, entry_hash, old_entry, nil),
            new_dataset,
            new_file_reader,
            fun
          )
      end
    else
      _ ->
        # reached end of both lines
        if new_entry == nil do
          {:done, acc}
        else
          # reached end of file for the old file
          bin = :erlang.term_to_binary(new_entry)
          iterate(hash_fun, fun.(acc, new_entry_hash, bin, new_entry), tl(new_dataset), nil, fun)
        end
    end
  end

  defp table_offset(%State{table_offsets: nil}, _table_idx) do
    nil
  end

  defp table_offset(%State{table_offsets: table_offsets}, table_idx) do
    Map.get(table_offsets, table_idx)
  end

  defp file_lookup(%State{file_entries: 0}, _key, _hash), do: []

  defp file_lookup(state = %State{slot_counts: slot_counts}, key, hash) do
    table_idx = table_idx(hash)
    slot_count = Map.get(slot_counts, table_idx, 0)

    if Bloom.lookup(state, hash) and slot_count > 0 do
      slot = slot_idx(slot_count, hash)

      {ret, _n} =
        file_lookup_slot_loop(state, key, hash, table_offset(state, table_idx), slot, slot_count)

      ret
    else
      []
    end
  end

  defp batch_read(fp, point, count) do
    {:ok, data} = PagedFile.pread(fp, point, (@slot_size + @hash_size) * count)

    for <<hash::binary-size(@hash_size), offset::unsigned-size(@slot_size_bits) <- data>> do
      {hash, offset}
    end ++ [@null_tuple]
  end

  @batch_size 32
  defp file_lookup_slot_loop(
         state = %State{fp: fp, keyfun: keyfun},
         key,
         hash,
         base_offset,
         slot,
         slot_count,
         n \\ 0
       ) do
    slot = rem(slot, slot_count)
    point = base_offset + slot * (@slot_size + @hash_size)
    batch_size = min(@batch_size, slot_count - slot)
    hash_offsets = batch_read(fp, point, batch_size)

    # a zero offset is an indication of the end
    # a hash bigger than the searched hash means
    # we reached the next entry or and overflow entry
    hash_offsets =
      Enum.take_while(hash_offsets, fn {<<khash::binary-size(@hash_size)>>, offset} ->
        offset != 0 and khash <= hash
      end)

    len = length(hash_offsets)

    offsets =
      Enum.filter(hash_offsets, fn {rhash, _offset} ->
        rhash == hash
      end)
      |> Enum.map(fn {_hash, offset} -> offset end)

    {:ok, sizes} =
      PagedFile.pread(fp, Enum.zip(offsets, List.duplicate(@entry_size_size, length(offsets))))

    sizes = Enum.map(sizes, fn <<size::unsigned-size(@entry_size_size_bits)>> -> size end)
    offsets = Enum.map(offsets, fn offset -> offset + @entry_size_size end)
    {:ok, entries} = PagedFile.pread(fp, Enum.zip(offsets, sizes))

    Enum.find_value(entries, fn entry ->
      entry = :erlang.binary_to_term(entry)

      if keyfun.(entry) == key do
        entry
      end
    end)
    |> case do
      nil ->
        if len < batch_size do
          {[], n}
        else
          file_lookup_slot_loop(
            state,
            key,
            hash,
            base_offset,
            slot + batch_size,
            slot_count,
            n + 1
          )
        end

      entry ->
        {[entry], n}
    end
  end

  defp default_hash(key) do
    <<hash::binary-size(@hash_size), _::binary()>> =
      :crypto.hash(:sha256, :erlang.term_to_binary(key))

    hash
  end

  # get a table idx from the hash value
  defp table_idx(<<idx, _::binary()>>) do
    idx
  end

  defp do_string(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp do_string(list) when is_list(list), do: List.to_string(list)
  defp do_string(string) when is_binary(string), do: string

  defp file_open(filename) do
    opts = [page_size: 1_000_000, max_pages: 1000]
    {:ok, fp} = PagedFile.open(filename, opts)
    fp
  end
end

defimpl Enumerable, for: DetsPlus do
  alias DetsPlus.FileReader
  def count(_pid), do: {:error, __MODULE__}
  def member?(_pid, _key), do: {:error, __MODULE__}
  def slice(_pid), do: {:error, __MODULE__}

  def reduce(%DetsPlus{pid: pid, ets: ets}, acc, fun) do
    new_data1 = :ets.tab2list(ets)
    {new_data2, filename, hash_fun} = GenServer.call(pid, :get_reduce_state)

    opts = [page_size: 1_000_000, max_pages: 1000]

    {fp, old_file} =
      with true <- File.exists?(filename),
           {:ok, fp} <- PagedFile.open(filename, opts) do
        old_file = FileReader.new(fp, byte_size("DET+"), module: PagedFile, buffer_size: 512_000)
        {fp, old_file}
      else
        _ -> {nil, nil}
      end

    # Ensuring hash function sort order
    {new_dataset, _} =
      DetsPlus.parallel_hash(hash_fun, new_data1 ++ new_data2)
      |> Enum.sort_by(fn {hash, _tuple} -> hash end, :desc)
      |> Enum.reduce({[], nil}, fn {hash, object}, {ret, prev} ->
        if prev != hash do
          {[{hash, object} | ret], hash}
        else
          {ret, hash}
        end
      end)

    ret =
      DetsPlus.iterate(hash_fun, acc, new_dataset, old_file, fn acc,
                                                                _entry_hash,
                                                                entry_blob,
                                                                entry ->
        fun.(entry || :erlang.binary_to_term(entry_blob), acc)
      end)

    if fp != nil, do: PagedFile.close(fp)
    ret
  end
end
