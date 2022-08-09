defmodule DetsPlus do
  @moduledoc """
  DetsPlus persistent tuple storage.

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

  # We're using sha256 - should not have conflicts ever
  @hash_size 32
  @hash_size_bits @hash_size * 8

  use GenServer
  require Logger

  @enforce_keys [:version]
  defstruct [
    :version,
    :header_offset,
    :bloom,
    :bloom_size,
    :filename,
    :fp,
    :name,
    :ets,
    :mode,
    :auto_save,
    :keypos,
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

  @version 1

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
      if File.exists?(filename) do
        load_state(filename)
        |> init_table_offsets()
      else
        mode = Keyword.get(args, :access, :read_write)
        auto_save = Keyword.get(args, :auto_save, 180_000)
        keypos = Keyword.get(args, :keypos, 1)
        type = Keyword.get(args, :type, :set)

        # unused options from dets:
        # - ram_file
        # - max_no_slots
        # - min_no_slots
        # - repair

        %DetsPlus{
          version: @version,
          bloom: "",
          bloom_size: 0,
          header_offset: 0,
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

    case GenServer.start_link(__MODULE__, state, hibernate_after: 5_000, name: name) do
      {:ok, _pid} -> {:ok, name}
      err -> err
    end
  end

  defp load_state(filename) do
    fp = file_open(filename)

    {:ok, <<header_size::unsigned-size(@entry_size_size_bits)>>} =
      PagedFile.pread(fp, 0, @entry_size_size)

    {:ok, header} = PagedFile.pread(fp, @entry_size_size, header_size)

    %DetsPlus{version: version} = state = :erlang.binary_to_term(header)

    if version > @version do
      raise("incompatible dets+ version #{version}")
    end

    %DetsPlus{
      state
      | fp: fp,
        header_offset: header_size + @entry_size_size,
        file_size: File.stat!(filename).size
    }
  end

  defp store_state(state = %DetsPlus{}, fp) do
    bin =
      :erlang.term_to_binary(
        %DetsPlus{
          state
          | fp: nil,
            sync: nil,
            sync_waiters: [],
            header_offset: 0,
            table_offsets: %{},
            ets: nil
        },
        [:compressed]
      )

    size = byte_size(bin)
    :ok = PagedFile.pwrite(fp, 0, <<size::unsigned-size(@entry_size_size_bits)>>)
    :ok = PagedFile.pwrite(fp, @entry_size_size, bin)
    %DetsPlus{state | header_offset: size + @entry_size_size}
  end

  @impl true
  def init(state = %DetsPlus{auto_save: auto_save}) do
    if is_integer(auto_save) do
      :timer.send_interval(auto_save, :auto_save)
    end

    {:ok, init_ets(state)}
  end

  defp init_ets(state = %DetsPlus{name: name, type: type, keypos: keypos}) do
    %DetsPlus{state | ets: :ets.new(name, [type, {:keypos, keypos}])}
  end

  defp init_table_offsets(
         state = %DetsPlus{header_offset: header_offset, slot_counts: slot_counts}
       ) do
    table_offsets =
      Enum.reduce(1..256, %{0 => header_offset}, fn table_idx, table_offsets ->
        offset =
          Map.get(table_offsets, table_idx - 1) +
            Map.get(slot_counts, table_idx - 1, 0) * @slot_size

        Map.put(table_offsets, table_idx, offset)
      end)

    %DetsPlus{state | table_offsets: table_offsets}
  end

  @doc """
  Syncs pending writes to the persistent file and closes the table.
  """
  @spec close(atom | pid) :: :ok
  def close(pid) do
    call(pid, :sync)
    GenServer.stop(pid)
  end

  @doc """
    Inserts one or more objects into the table. If there already exists an object with a key matching the key of some of the given objects, the old object will be replaced.
  """
  @spec insert(atom | pid, tuple()) :: :ok | {:error, atom()}
  def insert(pid, tuple) do
    call(pid, {:insert, tuple})
  end

  @doc """
    Inserts one or more objects into the table. If there already exists an object with a key matching the key of some of the given objects, the old object will be replaced.
  """
  def insert_new(pid, tuple) do
    case call(pid, {:insert_new, tuple}) do
      :ok -> true
      false -> false
    end
  end

  @doc """
  Returns a list of all objects with key Key stored in the table.

  Example:

  ```
  2> DetsPlus.open_file(:abc)
  {ok,:abc}
  3> DetsPlus.insert(:abc, {1,2,3})
  ok
  4> DetsPlus.insert(:abc, {1,3,4})
  ok
  5> DetsPlus.lookup(:abc, 1).
  [{1,3,4}]
  ```

  If the table type is set, the function returns either the empty list or a list with one object, as there cannot be more than one object with a given key. If the table type is bag or duplicate_bag, the function returns a list of arbitrary length.

  Notice that the order of objects returned is unspecified. In particular, the order in which objects were inserted is not reflected.
  """
  @spec lookup(atom | pid, any) :: [tuple()] | {:error, atom()}
  def lookup(pid, key) do
    call(pid, {:lookup, key})
  end

  @doc """
  Works like `lookup/2`, but does not return the objects. Returns true if one or more table elements has the key `key`, otherwise false.
  """
  @spec member?(atom | pid, any) :: false | true | {:error, atom}
  def member?(pid, key) do
    case lookup(pid, key) do
      [] -> false
      list when is_list(list) -> true
      error -> error
    end
  end

  @doc """
  Same as `member?/2`
  """
  @spec member(atom | pid, any) :: false | true | {:error, atom}
  def member(pid, key) do
    member?(pid, key)
  end

  @doc """
  Ensures that all updates made to table are written to disk. While the sync is running the
  table can still be used for reads and writes, but writes issed after the `sync/1` call
  will not be part of the persistent file. These new changes will only be included in the
  next sync call.
  """
  @spec sync(atom | pid) :: :ok
  def sync(pid) do
    call(pid, :sync, :infinity)
  end

  @doc """
  Returns information about table Name as a list of tuples:

  - `{file_size, integer() >= 0}}` - The file size, in bytes.
  - `{filename, file:name()}` - The name of the file where objects are stored.
  - `{keypos, keypos()}` - The key position.
  - `{size, integer() >= 0}` - The number of objects estimated in the table.
  - `{type, type()}` - The table type.
  """
  @spec info(atom | pid) :: [] | nil
  def info(pid) do
    call(pid, :info)
  end

  @doc """
  Returns the information associated with `item` for the table. The following items are allowed:

  - `{file_size, integer() >= 0}}` - The file size, in bytes.
  - `{filename, file:name()}` - The name of the file where objects are stored.
  - `{keypos, keypos()}` - The key position.
  - `{size, integer() >= 0}` - The number of objects estimated in the table.
  - `{type, type()}` - The table type.
  """
  @spec info(atom | pid, :file_size | :filename | :keypos | :size | :type) :: any()
  def info(pid, item)
      when item == :file_size or item == :filename or item == :keypos or item == :size or
             item == :type or item == :creation_stats do
    case info(pid) do
      nil -> nil
      list -> Keyword.get(list, item)
    end
  end

  defp call(pid, cmd, timeout \\ :infinity) do
    GenServer.call(pid, cmd, timeout)
  end

  @impl true
  def handle_call(
        {:insert, tuple},
        from,
        state = %DetsPlus{
          ets: ets,
          sync: sync,
          sync_fallback: fallback,
          sync_waiters: sync_waiters
        }
      ) do
    if sync == nil do
      :ets.insert(ets, tuple)
      {:reply, :ok, check_auto_save(state)}
    else
      fallback =
        List.wrap(tuple)
        |> Enum.reduce(fallback, fn tuple, fallback ->
          Map.put(fallback, do_key(state, tuple), tuple)
        end)

      if map_size(fallback) > 1_000_000 do
        # this pause exists to protect from out_of_memory situations when the writer can't
        # finish in time
        Logger.warn(
          "DetsPlus flush slower than new inserts - pausing writes until flush is complete"
        )

        {:noreply,
         %DetsPlus{state | sync_fallback: fallback, sync_waiters: [from | sync_waiters]}}
      else
        {:reply, :ok, %DetsPlus{state | sync_fallback: fallback}}
      end
    end
  end

  def handle_call(
        {:insert_new, tuple},
        from,
        state = %DetsPlus{ets: ets, sync_fallback: fallback}
      ) do
    tuples = List.wrap(tuple)

    exists =
      Enum.any?(tuples, fn tuple ->
        key = do_key(state, tuple)

        Map.has_key?(fallback, key) || :ets.lookup(ets, key) != [] ||
          file_lookup(state, key) != []
      end)

    if exists do
      {:reply, false, state}
    else
      handle_call({:insert, tuples}, from, state)
    end
  end

  def handle_call(
        {:lookup, key},
        _from,
        state = %DetsPlus{ets: ets, sync_fallback: fallback}
      ) do
    case Map.get(fallback, key) do
      nil ->
        case :ets.lookup(ets, key) do
          [] -> {:reply, file_lookup(state, key), state}
          other -> {:reply, other, state}
        end

      value ->
        {:reply, [value], state}
    end
  end

  def handle_call(
        :info,
        _from,
        state = %DetsPlus{
          filename: filename,
          keypos: keypos,
          type: type,
          ets: ets,
          file_size: file_size,
          file_entries: file_entries,
          creation_stats: creation_stats
        }
      ) do
    size = :ets.info(ets, :size) + file_entries

    info = [
      file_size: file_size,
      filename: filename,
      keypos: keypos,
      size: size,
      type: type,
      creation_stats: creation_stats
    ]

    {:reply, info, state}
  end

  def handle_call(:sync, from, state = %DetsPlus{sync: nil, ets: ets}) do
    if :ets.info(ets, :size) == 0 do
      {:reply, :ok, state}
    else
      sync = spawn_sync_worker(state)
      {:noreply, %DetsPlus{state | sync: sync, sync_waiters: [from]}}
    end
  end

  def handle_call(:sync, from, state = %DetsPlus{sync: sync, sync_waiters: sync_waiters})
      when is_pid(sync) do
    {:noreply, %DetsPlus{state | sync_waiters: [from | sync_waiters]}}
  end

  @impl true
  def handle_cast(
        {:sync_complete, new_filename, new_state = %DetsPlus{}},
        %DetsPlus{
          fp: fp,
          filename: filename,
          ets: ets,
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
    :ets.insert(ets, Map.values(fallback))

    for w <- waiters do
      :ok = GenServer.reply(w, :ok)
    end

    {:noreply, %DetsPlus{new_state | fp: fp, sync: nil, sync_fallback: %{}, sync_waiters: []}}
  end

  @impl true
  def handle_info(:auto_save, state = %DetsPlus{sync: sync, ets: ets}) do
    sync =
      if sync == nil and :ets.info(ets, :size) > 0 do
        spawn_sync_worker(state)
      else
        sync
      end

    {:noreply, %DetsPlus{state | sync: sync}}
  end

  defp check_auto_save(state) do
    state
  end

  defp add_stats({prev, stats}, label) do
    now = :erlang.timestamp()
    {now, [{label, div(:timer.now_diff(now, prev), 1000)} | stats]}
  end

  defp spawn_sync_worker(
         state = %DetsPlus{
           ets: ets,
           fp: fp,
           filename: filename,
           file_entries: file_entries
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

        stats = add_stats(stats, :ets_flushed)
        send(dets, :continue)

        # Ensuring hash function sort order
        new_dataset =
          parallel_hash(state, new_dataset)
          |> Enum.sort_by(fn {hash, _tuple} -> hash end, :asc)

        stats = add_stats(stats, :ets_sorted)
        new_filename = "#{filename}.buffer"

        # opts =
        #   if String.ends_with?(filename, ".gz") do
        #     [:raw, :read, :read_ahead, :write, :delayed_write, :binary, :compressed]
        #   else
        #     [:raw, :read, :read_ahead, :write, :delayed_write, :binary]
        #   end
        opts = [page_size: 1_000_000, max_pages: 1000]

        # setting the bloom size based of a size estimate
        bloom_size = (file_entries + length(new_dataset)) * 10
        state = bloom_create(state, bloom_size)

        old_file =
          if fp != nil do
            FileReader.new(fp, table_offset(state, 256), module: PagedFile, buffer_size: 512_000)
          else
            nil
          end

        stats = add_stats(stats, :initialize)
        state = pre_scan_file(state, new_dataset, old_file)
        stats = add_stats(stats, :pre_scan)

        PagedFile.delete(new_filename)
        {:ok, new_file} = PagedFile.open(new_filename, opts)
        stats = add_stats(stats, :fopen)

        state =
          %DetsPlus{state | fp: new_file}
          |> bloom_finalize()
          |> store_state(new_file)
          |> init_table_offsets()

        stats = add_stats(stats, :header_store)
        state = write_to_file(state, table_offset(state, 256), new_dataset, old_file)
        stats = add_stats(stats, :write_to_file)

        PagedFile.close(new_file)
        {_, stats} = add_stats(stats, :file_close)

        state = %DetsPlus{state | creation_stats: stats}
        GenServer.cast(dets, {:sync_complete, new_filename, state})
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
  defp parallel_hash(state, new_dataset, tasks \\ 1) do
    len = length(new_dataset)

    if len > @min_chunk_size and tasks < @max_tasks do
      {a, b} = Enum.split(new_dataset, div(len, 2))
      task = Task.async(fn -> parallel_hash(state, a, tasks * 2) end)
      result = parallel_hash(state, b, tasks * 2)
      Task.await(task, :infinity) ++ result
    else
      Enum.map(new_dataset, fn tuple ->
        key = do_key(state, tuple)
        hash = do_hash(state, key)
        {hash, tuple}
      end)
    end
  end

  defp pre_scan_file(state, new_dataset, old_file) do
    state = %DetsPlus{state | file_entries: 0, slot_counts: %{}}

    state =
      %DetsPlus{slot_counts: slot_counts} =
      iterate(
        state,
        state,
        new_dataset,
        old_file,
        fn state = %DetsPlus{file_entries: file_entries, slot_counts: slot_counts},
           entry_hash,
           _ ->
          table_idx = rem(entry_hash, 256)
          slot_counts = Map.update(slot_counts, table_idx, 1, fn count -> count + 1 end)
          state = bloom_add(state, entry_hash)
          %DetsPlus{state | file_entries: file_entries + 1, slot_counts: slot_counts}
        end
      )

    new_slot_counts =
      Enum.map(slot_counts, fn {key, value} -> {key, trunc(value * 1.5) + 1} end)
      |> Map.new()

    %DetsPlus{state | slot_counts: new_slot_counts}
  end

  defp write_to_file(
         state = %DetsPlus{fp: fp, slot_counts: slot_counts},
         new_file_offset,
         new_dataset,
         old_file
       ) do
    writer = FileWriter.new(fp, new_file_offset, module: PagedFile)
    prober = Task.async(fn -> prober(fp) end)

    writer =
      iterate(state, writer, new_dataset, old_file, fn writer, hash, entry ->
        size = byte_size(entry)
        table_idx = rem(hash, 256)

        slot_count = Map.get(slot_counts, table_idx)
        slot = rem(div(hash, 256), slot_count)
        base_offset = table_offset(state, table_idx)
        offset = FileWriter.offset(writer)
        send(prober.pid, {base_offset, slot, slot_count, offset})

        FileWriter.write(writer, <<size::unsigned-size(@entry_size_size_bits), entry::binary()>>)
      end)

    send(prober.pid, :exit)
    Task.await(prober, :infinity)

    FileWriter.close(writer)
    %DetsPlus{state | file_size: FileWriter.offset(writer)}
  end

  defp prober(fp, known \\ %{}, writes \\ [], n \\ 0) do
    receive do
      :exit ->
        :ok = PagedFile.pwrite(fp, writes)
        :ok

      {base_offset, slot0, slot_count, value} ->
        {slot, not_free} = prober_next_free(known, base_offset, slot0)

        # if slot0 != slot, do: IO.inspect({:slot, base_offset, slot0, slot})

        writes = [
          {base_offset + slot * @slot_size, <<value::unsigned-size(@slot_size_bits)>>} | writes
        ]

        next_free = rem(slot + 1, slot_count)

        known =
          Enum.reduce(not_free, known, fn nf, known ->
            Map.put(known, {base_offset, nf}, next_free)
          end)

        n = n + 1

        if n > 32_000 do
          PagedFile.pwrite(fp, writes)
          prober(fp, known)
        else
          prober(fp, known, writes, n)
        end

    end
  end

  defp prober_next_free(known, base_offset, slot) do
    case Map.get(known, {base_offset, slot}) do
      nil ->
        {slot, [slot]}

      other_slot ->
        {free_slot, not_free} = prober_next_free(known, base_offset, other_slot)
        {free_slot, [slot | not_free]}
    end
  end

  # this function takes a new_dataset and an old file and merges them, it calls
  # on every entry the callback `fun.(acc, entry_hash, binary_entry)` and returns the final acc
  defp iterate(
         state = %DetsPlus{},
         acc,
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
         {new_file_reader, <<old_size::unsigned-size(@entry_size_size_bits)>>} <-
           FileReader.read(file_reader, @entry_size_size) do
      {new_file_reader, old_entry} = FileReader.read(new_file_reader, old_size)
      entry = :erlang.binary_to_term(old_entry)
      entry_key = do_key(state, entry)
      entry_hash = do_hash(state, entry_key)
      {entry, old_entry, entry_hash, new_file_reader}

      case new_entry_hash do
        # reached end of new_dataset
        nil ->
          iterate(state, fun.(acc, entry_hash, old_entry), [], new_file_reader, fun)

        # replacing an old entry with a new entry
        ^entry_hash ->
          bin = :erlang.term_to_binary(new_entry)
          iterate(state, fun.(acc, new_entry_hash, bin), tl(new_dataset), new_file_reader, fun)

        # inserting the new entry before the old
        new_entry_hash when new_entry_hash < entry_hash ->
          bin = :erlang.term_to_binary(new_entry)
          iterate(state, fun.(acc, new_entry_hash, bin), tl(new_dataset), file_reader, fun)

        # inserting the old entry before the new
        new_entry_hash when new_entry_hash > entry_hash ->
          iterate(state, fun.(acc, entry_hash, old_entry), new_dataset, new_file_reader, fun)
      end
    else
      atom when atom == true or atom == :eof ->
        # reached end of both lines
        if new_entry == nil do
          acc
        else
          # reached end of file for the old file
          bin = :erlang.term_to_binary(new_entry)
          iterate(state, fun.(acc, new_entry_hash, bin), tl(new_dataset), nil, fun)
        end
    end
  end

  defp bloom_create(state = %DetsPlus{}, bloom_size) do
    {:ok, ram_file} = :file.open(:binary.copy(<<0>>, bloom_size), [:ram, :read, :write, :binary])
    %DetsPlus{state | bloom_size: bloom_size, bloom: {ram_file, [], 0}}
  end

  defp bloom_add(state = %DetsPlus{bloom_size: bloom_size, bloom: {ram_file, keys, n}}, hash) do
    key = rem(hash, bloom_size)
    keys = [key | keys]
    n = n + 1

    if n < 128 do
      %DetsPlus{state | bloom: {ram_file, keys, n}}
    else
      :ok = :file.pwrite(ram_file, Enum.zip(keys, List.duplicate(<<1>>, n)))
      %DetsPlus{state | bloom: {ram_file, [], 0}}
    end
  end

  defp bloom_finalize(state = %DetsPlus{bloom: {ram_file, keys, n}, bloom_size: bloom_size}) do
    :ok = :file.pwrite(ram_file, Enum.zip(keys, List.duplicate(<<1>>, n)))
    {:ok, binary} = :file.pread(ram_file, 0, bloom_size)
    :file.close(ram_file)
    %DetsPlus{state | bloom: binary}
  end

  defp bloom_lookup(%DetsPlus{bloom_size: bloom_size, bloom: bloom}, hash) do
    :binary.at(bloom, rem(hash, bloom_size)) == 1
  end

  defp table_offset(%DetsPlus{table_offsets: nil}, _table_idx) do
    nil
  end

  defp table_offset(%DetsPlus{table_offsets: offsets}, table_idx) do
    Map.get(offsets, table_idx)
  end

  # # recursivley retry next slot if current slot is used already
  # defp file_put_entry_probe(fp, base_offset, slot, slot_count, value) do
  #   slot = rem(slot, slot_count)
  #   # There could be some analytics here for number of probings worst case etc...

  #   # probe batching to improve file io
  #   batch_size = min(32, slot_count - slot)

  #   {slot, probe} =
  #     case PagedFile.pread(fp, base_offset + slot * @slot_size, @slot_size * batch_size) do
  #       # eof means the file has not been extended yet, so safe to write
  #       :eof ->
  #         {slot, true}

  #       {:ok, data} ->
  #         data = data <> :binary.copy(<<0>>, @slot_size * batch_size - byte_size(data))
  #         # all zeros means there is no entry address stored yet
  #         case find_free_slot(data) do
  #           nil -> {slot, false}
  #           idx -> {slot + idx, true}
  #         end
  #     end

  #   if probe do
  #     :ok =
  #       PagedFile.pwrite(
  #         fp,
  #         base_offset + slot * @slot_size,
  #         <<value::unsigned-size(@slot_size_bits)>>
  #       )
  #   else
  #     file_put_entry_probe(fp, base_offset, slot + batch_size, slot_count, value)
  #   end
  # end

  # defp find_free_slot(data, slot \\ 0) do
  #   case data do
  #     <<>> -> nil
  #     <<0::unsigned-size(@slot_size_bits), _::binary>> -> slot
  #     <<_::unsigned-size(@slot_size_bits), rest::binary>> -> find_free_slot(rest, slot + 1)
  #   end
  # end

  defp file_lookup(%DetsPlus{file_entries: 0}, _key), do: []

  defp file_lookup(state = %DetsPlus{slot_counts: slot_counts}, key) do
    hash = do_hash(state, key)
    table_idx = rem(hash, 256)
    slot_count = Map.get(slot_counts, table_idx, 0)

    if bloom_lookup(state, hash) and slot_count > 0 do
      # if slot_count > 0 do
      slot = rem(div(hash, 256), slot_count)

      {ret, _n} =
        file_lookup_slot_loop(state, key, table_offset(state, table_idx), slot, slot_count)

      # if n > 2 do
      #   IO.inspect({ret, n, slot_count})
      # end

      ret
    else
      []
    end
  end

  defp batch_read(fp, point, count) do
    {:ok, data} = PagedFile.pread(fp, point, @slot_size * count)
    data = data <> :binary.copy(<<0>>, @slot_size * count - byte_size(data))
    for <<offset::unsigned-size(@slot_size_bits) <- data>>, do: offset
  end

  @batch_size 32
  defp file_lookup_slot_loop(
         state = %DetsPlus{fp: fp},
         key,
         base_offset,
         slot,
         slot_count,
         n \\ 0
       ) do
    slot = rem(slot, slot_count)
    point = base_offset + slot * @slot_size
    batch_size = min(@batch_size, slot_count - slot)
    offsets = batch_read(fp, point, batch_size)

    if hd(offsets) == 0 do
      {[], n}
    else
      # a zero is an indication of the end
      offsets = Enum.take_while(offsets, fn x -> x != 0 end)
      len = length(offsets)

      {:ok, sizes} = PagedFile.pread(fp, Enum.zip(offsets, List.duplicate(@entry_size_size, len)))
      sizes = Enum.map(sizes, fn <<size::unsigned-size(@entry_size_size_bits)>> -> size end)

      offsets = Enum.map(offsets, fn offset -> offset + @entry_size_size end)
      {:ok, entries} = PagedFile.pread(fp, Enum.zip(offsets, sizes))

      Enum.find_value(entries, fn entry ->
        entry = :erlang.binary_to_term(entry)

        if do_key(state, entry) == key do
          entry
        end
      end)
      |> case do
        nil ->
          if len < batch_size do
            {[], n}
          else
            file_lookup_slot_loop(state, key, base_offset, slot + batch_size, slot_count, n + 1)
          end

        entry ->
          {[entry], n}
      end
    end
  end

  defp do_key(%DetsPlus{keypos: keypos}, tuple) do
    elem(tuple, keypos - 1)
  end

  defp do_hash(%DetsPlus{}, key) do
    <<slot::unsigned-size(@hash_size_bits)>> = :crypto.hash(:sha256, :erlang.term_to_binary(key))

    slot
  end

  defp do_string(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp do_string(list) when is_list(list), do: List.to_string(list)
  defp do_string(string) when is_binary(string), do: string

  defp file_open(filename) do
    # opts =
    #   if String.ends_with?(filename, ".gz") do
    #     [:read, :read_ahead, :binary, :compressed]
    #   else
    #     [:read, :read_ahead, :binary]
    #   end
    opts = [page_size: 1_000_000, max_pages: 1000]
    {:ok, fp} = PagedFile.open(filename, opts)
    fp
  end
end
