defmodule DetsPlus.Bloom do
  @moduledoc false
  alias DetsPlus.State
  use Bitwise
  @hash_size_bits 64
  @atomic_bits 64

  def create(state = %State{}, bloom_size) do
    ref = :atomics.new(ceil(bloom_size / @atomic_bits), signed: false)
    %State{state | bloom_size: bloom_size, bloom: ref}
  end

  def add(
        state = %State{bloom_size: bloom_size, bloom: ref},
        <<hash::unsigned-size(@hash_size_bits)>>
      ) do
    key = rem(hash, bloom_size)
    dword = div(key, @atomic_bits) + 1
    bit = rem(key, @atomic_bits)
    old = :atomics.get(ref, dword)
    new = old ||| 1 <<< bit
    :atomics.put(ref, dword, new)
    state
  end

  def finalize(state = %State{bloom: ref, bloom_size: bloom_size}) do
    e = ceil(bloom_size / @atomic_bits)

    binary =
      for i <- 1..e do
        <<:atomics.get(ref, i)::unsigned-little-size(@atomic_bits)>>
      end
      |> :erlang.iolist_to_binary()

    %State{state | bloom: binary}
  end

  def lookup(
        %State{bloom_size: bloom_size, bloom: bloom},
        <<hash::unsigned-size(@hash_size_bits)>>
      ) do
    key = rem(hash, bloom_size)
    byte = div(key, 8)
    bit = rem(key, 8)
    (:binary.at(bloom, byte) &&& 1 <<< bit) > 0
  end
end
