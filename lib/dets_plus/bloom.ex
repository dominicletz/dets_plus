defmodule DetsPlus.Bloom do
  @moduledoc false
  alias DetsPlus.{Bloom, State}
  import Bitwise
  @hash_size_bits 64
  @atomic_bits 64

  defstruct [:bloom, :bloom_size]

  def create(bloom_size) do
    bloom = :atomics.new(ceil(bloom_size / @atomic_bits), signed: false)
    %Bloom{bloom: bloom, bloom_size: bloom_size}
  end

  def add(
        state = %Bloom{bloom: ref, bloom_size: bloom_size},
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

  def finalize(%Bloom{bloom: ref, bloom_size: bloom_size}) do
    e = ceil(bloom_size / @atomic_bits)

    binary =
      for i <- 1..e do
        <<:atomics.get(ref, i)::unsigned-little-size(@atomic_bits)>>
      end
      |> :erlang.iolist_to_binary()

    {binary, bloom_size}
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
