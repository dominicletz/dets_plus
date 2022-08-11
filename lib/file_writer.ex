defmodule FileWriter do
  @moduledoc """
    Helper module to accelerate linear file reads. Reads next `buffer_size`
    bytes and keeps in state.
  """
  defstruct [:fp, :module, :buffer_size, :offset, :chunk, :chunk_size]

  def new(fp, start_offset \\ 0, opts \\ []) when is_integer(start_offset) do
    module = Keyword.get(opts, :module, :file)
    buffer_size = Keyword.get(opts, :buffer_size, 512_000)

    %FileWriter{
      fp: fp,
      module: module,
      buffer_size: buffer_size,
      offset: start_offset,
      chunk: [],
      chunk_size: 0
    }
  end

  def write(
        state = %FileWriter{buffer_size: buffer_size, chunk: chunk, chunk_size: chunk_size},
        bin
      ) do
    chunk = [bin | chunk]
    chunk_size = chunk_size + byte_size(bin)
    state = %FileWriter{state | chunk: chunk, chunk_size: chunk_size}

    if chunk_size > buffer_size do
      write_chunk(state)
    else
      state
    end
  end

  def offset(%FileWriter{offset: offset, chunk_size: chunk_size}) do
    offset + chunk_size
  end

  def sync(state = %FileWriter{chunk_size: 0}) do
    state
  end

  def sync(state = %FileWriter{}) do
    write_chunk(state)
  end

  defp write_chunk(
         state = %FileWriter{
           fp: fp,
           module: module,
           chunk_size: chunk_size,
           chunk: chunk,
           offset: offset
         }
       ) do
    bin = :erlang.iolist_to_binary(Enum.reverse(chunk))
    ^chunk_size = byte_size(bin)
    :ok = module.pwrite(fp, offset, bin)
    %FileWriter{state | chunk: [], chunk_size: 0, offset: offset + chunk_size}
  end
end
