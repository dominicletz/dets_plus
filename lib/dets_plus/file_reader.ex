defmodule DetsPlus.FileReader do
  @moduledoc false
  alias DetsPlus.FileReader
  defstruct [:fp, :module, :buffer_size, :offset, :chunk]

  def new(fp, start_offset \\ 0, opts \\ []) when is_integer(start_offset) do
    module = Keyword.get(opts, :module, :file)
    buffer_size = Keyword.get(opts, :buffer_size, 64_000)

    %FileReader{
      fp: fp,
      module: module,
      buffer_size: buffer_size,
      offset: start_offset,
      chunk: ""
    }
  end

  def offset(%FileReader{offset: offset, chunk: chunk}) do
    offset - byte_size(chunk)
  end

  def read(state = %FileReader{buffer_size: buffer_size, chunk: chunk}, n)
      when is_integer(n) and n < buffer_size do
    if byte_size(chunk) < n do
      case read_chunk(state) do
        :eof -> :eof
        state -> read(state, n)
      end
    else
      ret = binary_part(chunk, 0, n)
      chunk = binary_part(chunk, n, byte_size(chunk) - n)
      {%FileReader{state | chunk: chunk}, ret}
    end
  end

  def read(state = %FileReader{fp: fp, module: module, offset: offset, chunk: chunk}, n)
      when is_integer(n) do
    case module.pread(fp, offset, n - byte_size(chunk)) do
      {:ok, binary} ->
        {%FileReader{state | offset: offset + byte_size(binary), chunk: ""}, chunk <> binary}

      :eof ->
        :eof
    end
  end

  defp read_chunk(
         state = %FileReader{
           fp: fp,
           module: module,
           buffer_size: buffer_size,
           chunk: chunk,
           offset: offset
         }
       ) do
    case module.pread(fp, offset, buffer_size) do
      {:ok, binary} ->
        %FileReader{state | chunk: chunk <> binary, offset: offset + byte_size(binary)}

      :eof ->
        :eof
    end
  end
end
