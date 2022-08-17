defmodule DetsPlus.EntryWriter do
  @moduledoc false
  alias DetsPlus.{EntryWriter, FileReader}
  defstruct [:map, :fp, :filename]

  defmodule Table do
    @moduledoc false
    defstruct [
      :item_size,
      :items,
      :items_count,
      :items_count_limit,
      :fp,
      :writer_offset,
      :writer_count,
      :writer_count_limit
    ]

    def get(%Table{items: items, writer_count: 0}) do
      Enum.sort(items)
    end

    def get(%Table{
          item_size: item_size,
          items: items,
          fp: fp,
          writer_offset: writer_offset,
          writer_count: writer_count
        }) do
      reader = FileReader.new(fp, writer_offset - writer_count * item_size, module: PagedFile)

      (items ++ collect(writer_count, reader, item_size))
      |> Enum.sort()
    end

    defp collect(0, _reader, _item_size) do
      []
    end

    defp collect(num, reader, item_size) do
      {reader, entry} = FileReader.read(reader, item_size)
      [entry | collect(num - 1, reader, item_size)]
    end

    def info(%Table{items_count: items_count, writer_count: writer_count}) do
      %{items_count: items_count, writer_count: writer_count}
    end

    def insert(
          table = %Table{
            items: items,
            items_count: items_count,
            items_count_limit: items_count_limit,
            fp: fp,
            writer_offset: writer_offset,
            writer_count: writer_count,
            writer_count_limit: writer_count_limit
          },
          item
        ) do
      cond do
        items_count < items_count_limit ->
          %Table{table | items: [item | items], items_count: items_count + 1}

        writer_count + items_count < writer_count_limit ->
          items = [item | items]
          bin = :erlang.iolist_to_binary(items)
          PagedFile.pwrite(fp, writer_offset, bin)

          %Table{
            table
            | writer_offset: writer_offset + byte_size(bin),
              writer_count: writer_count + length(items),
              items: [],
              items_count: 0
          }

        true ->
          # IO.inspect(
          #   {"overflow!",
          #    %{
          #      items_count: items_count,
          #      items_count_limit: items_count_limit,
          #      fp: fp,
          #      writer_offset: writer_offset,
          #      writer_count: writer_count,
          #      writer_count_limit: writer_count_limit
          #    }}
          # )

          %Table{table | items: [item | items], items_count: items_count + 1}
      end
    end
  end

  def new(filename, estimated_size, _opts \\ []) do
    # batch_size = Keyword.get(opts, :batch_size, 1024)

    table_count = 256
    table_size = div(estimated_size, table_count) * 2
    filename = filename <> ".tmp.idx"

    # Using up to 128MB of ram, we keep at least one page cached
    # per table, as each table indexes at a different offset in the file
    opts = [page_size: 512_000, max_pages: table_count]
    {:ok, temp_fp} = PagedFile.open(filename, opts)
    # 64-bit hash + 64 bit offset
    item_size = 8 + 8

    map =
      Enum.reduce(0..(table_count - 1), %{}, fn table_idx, map ->
        Map.put(map, table_idx, %Table{
          item_size: item_size,
          items: [],
          items_count: 0,
          items_count_limit: 1000,
          fp: temp_fp,
          writer_offset: table_idx * table_size * item_size,
          writer_count: 0,
          writer_count_limit: table_size
        })
      end)

    %EntryWriter{map: map, fp: temp_fp, filename: filename}
  end

  def insert(state = %EntryWriter{map: map}, {table_idx, hash, offset}) do
    map =
      Map.update!(map, table_idx, fn table ->
        Table.insert(table, <<hash::binary-size(8), offset::unsigned-size(64)>>)
      end)

    %EntryWriter{state | map: map}
  end

  def lookup(%EntryWriter{map: map}, table_idx) do
    Table.get(Map.get(map, table_idx, []))
  end

  def info(%EntryWriter{map: map}) do
    Enum.map(map, fn {idx, table} -> {idx, Table.info(table)} end)
  end

  def close(state = %EntryWriter{fp: fp, filename: filename}) do
    PagedFile.close(fp)
    PagedFile.delete(filename)
    state
  end
end
