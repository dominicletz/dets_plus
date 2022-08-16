defmodule DetsPlus.Bloom do
  @moduledoc false
  alias DetsPlus.State
  use Bitwise
  @hash_size_bits 64
  @atomic_bits 64
  use GenServer

  def create(bloom_size) do
    atomics = :atomics.new(ceil(bloom_size / @atomic_bits), signed: false)
    {:ok, pid} = GenServer.start_link(__MODULE__, %{bloom: atomics, bloom_size: bloom_size})
    pid
  end

  def add(ref, entry) do
    GenServer.cast(ref, {:add, entry})
    ref
  end

  def finalize(state = %State{}, ref) do
    {binary, bloom_size} = GenServer.call(ref, :finalize, :infinity)
    GenServer.stop(ref)
    %State{state | bloom: binary, bloom_size: bloom_size}
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

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_cast(
        {:add, <<hash::unsigned-size(@hash_size_bits)>>},
        state = %{bloom: ref, bloom_size: bloom_size}
      ) do
    key = rem(hash, bloom_size)
    dword = div(key, @atomic_bits) + 1
    bit = rem(key, @atomic_bits)
    old = :atomics.get(ref, dword)
    new = old ||| 1 <<< bit
    :atomics.put(ref, dword, new)
    {:noreply, state}
  end

  @impl true
  def handle_call(:finalize, _from, state = %{bloom: ref, bloom_size: bloom_size}) do
    e = ceil(bloom_size / @atomic_bits)

    binary =
      for i <- 1..e do
        <<:atomics.get(ref, i)::unsigned-little-size(@atomic_bits)>>
      end
      |> :erlang.iolist_to_binary()

    {:reply, {binary, bloom_size}, state}
  end
end
