defmodule DetsPlus.Bench do
  alias DetsPlus.Bench.CubDBWrap
  @moduledoc false

  @to_be_hashed :rand.bytes(256)
  def hash_test(algo, test_size) do
    _ =
      Enum.reduce(0..test_size, @to_be_hashed, fn _x, bin ->
        :crypto.hash(algo, bin)
      end)

    :ok
  end

  def bloom_test(module, test_size) do
    bloom = module.create(test_size * 10)

    _ =
      Enum.reduce(0..test_size, bloom, fn x, bloom ->
        module.add(bloom, <<x::unsigned-size(64)>>)
      end)

    :ok
  end

  def test(module, test_size) do
    file = filename()
    rm(file)
    {:ok, dets} = open_file(module, file)

    for x <- 0..test_size do
      y =
        if x == 0 do
          1
        else
          [{_x, y}] = module.lookup(dets, x - 1)
          y
        end

      :ok = module.insert(dets, {x, y + x})
    end

    :ok = module.sync(dets)

    for x <- 0..test_size do
      [{^x, _y}] = module.lookup(dets, x)
    end

    :ok = module.close(dets)
  end

  def prepare_read_test(module, test_size) do
    filename = filename([test_size, module])

    if File.exists?(filename) do
      :ok
    else
      {:ok, dets} = open_file(module, filename)

      sync_size = 10_000_000
      step_size = 100

      for x <- 0..div(test_size, step_size) do
        i = x * step_size

        objects =
          for y <- 1..step_size do
            {i + y, 2 * (i + y)}
          end

        if x > 0 and rem(x * step_size, sync_size) == 0 do
          module.start_sync(dets)
        end

        :ok = module.insert(dets, objects)
      end

      :ok = module.close(dets)
      filename
    end
  end

  def iterate_test(_, module, test_size) do
    filename = filename([test_size, module])
    {:ok, dets} = open_file(module, filename)
    ret = module.reduce(dets, 0, fn _, x -> x + 1 end)
    IO.puts("iterated: #{ret} items")
    :ok = module.close(dets)
  end

  def read_test(filename, module, test_size) do
    {:ok, dets} = open_file(module, filename)

    for x <- 0..test_size do
      module.lookup(dets, test_size + x)
      module.lookup(dets, x)
    end

    :ok = module.close(dets)
  end

  def write_test(module, test_size) do
    filename = filename(["write", module])
    rm(filename)
    {:ok, dets} = open_file(module, filename)

    for x <- 0..test_size do
      module.insert(dets, {x, x * x})
    end

    :ok = module.sync(dets)

    for x <- 0..test_size do
      module.insert(dets, {x + test_size, x * (x + test_size)})
    end

    :ok = module.close(dets)
  end

  def large_write_test(module, test_size) do
    filename = filename(["large_write", module])
    rm(filename)
    test_data = :rand.bytes(1024 * 1024 * 10)
    {:ok, dets} = open_file(module, filename)

    for x <- 0..test_size do
      module.insert(dets, {x, test_data})
    end

    :ok = module.sync(dets)

    for x <- 0..test_size do
      module.insert(dets, {x + test_size, x * (x + test_size)})
    end

    :ok = module.close(dets)
    rm(filename)
    :ok
  end

  def prepare_sync_test(module, test_size) do
    filename = filename([module])
    rm(filename)
    {:ok, dets} = open_file(module, filename)

    for x <- 0..test_size do
      module.insert(dets, {x, x * x})
    end

    dets
  end

  def sync_test(dets, module, _test_size) do
    :ok = module.sync(dets)
    # if module != :dets, do: IO.inspect(module.info(dets, :creation_stats))
    module.close(dets)
  end

  def sync_test2(module, _test_size) do
    filename = filename([module])
    {:ok, dets} = open_file(module, filename)
    module.insert(dets, {1, 1})
    :ok = module.sync(dets)
    # if module != :dets, do: IO.inspect(module.info(dets, :creation_stats))
    module.close(dets)
  end

  def run(context = %{test_size: test_size, modules: modules, rounds: rounds}, label, fun) do
    for module <- modules do
      IO.puts("running #{label} test: #{inspect(module)}")

      for _ <- 1..rounds do
        time =
          if context.prepare do
            fp = context.prepare.(module, test_size)
            tc(fun, [fp, module, test_size])
          else
            tc(fun, [module, test_size])
          end

        IO.puts("#{div(time, 1000) / 1000}s")
      end
    end

    IO.puts("")
  end

  def insert_flush(filename) do
    {:ok, dets} = DetsPlus.open_file(String.to_atom(filename))
    DetsPlus.insert(dets, {1, 1})
    {time, _} = :timer.tc(fn -> DetsPlus.sync(dets) end)
    IO.puts("#{div(time, 1000)}ms")
    IO.puts("#{inspect(DetsPlus.info(dets, :creation_stats))}")
    DetsPlus.close(dets)
  end

  defp tc(fun, args) do
    {time, :ok} = :timer.tc(fun, args)
    time
  end

  def list() do
    all_tests()
    |> Enum.map(fn {_context, label, _fun} -> label end)
  end

  def runa(name \\ :all) do
    spawn(__MODULE__, :run, [name])
  end

  def run(name \\ :all) do
    tests =
      all_tests()
      |> Enum.filter(fn {_context, label, _fun} ->
        if name == :all do
          not String.starts_with?(label, ".")
        else
          label == name
        end
      end)

    if tests == [] do
      IO.puts("No test selected. Valid tests are #{inspect(list())}")
    else
      Enum.each(tests, fn {context, label, fun} -> run(context, label, fun) end)
    end
  end

  def all_tests() do
    # Normal read/write
    context = %{rounds: 3, modules: [:dets, DetsPlus, CubDBWrap], prepare: nil, test_size: 50_000}

    all = [
      {context, "write", &write_test/2},
      {context, "rw", &test/2},
      {%{context | prepare: &prepare_read_test/2}, "read", &read_test/3}
    ]

    # Large read/write (:dets can't to because of it's 2GB limit)
    all =
      all ++
        [
          {%{context | rounds: 1, test_size: 4_00, modules: [DetsPlus, CubDBWrap]}, "large_write",
           &large_write_test/2}
        ]

    context = %{rounds: 3, modules: [DetsPlus], prepare: &prepare_sync_test/2, test_size: nil}

    all =
      all ++
        [
          {%{context | test_size: 150_000}, "sync_test: 0 + 150_000 new inserts", &sync_test/3},
          {%{context | test_size: 1_500_000}, "sync_test: 0 + 1_500_000 new inserts",
           &sync_test/3},
          {%{context | prepare: nil}, "sync_test 1_500_000 + 1 new inserts", &sync_test2/2}
        ]

    context = %{rounds: 3, modules: [DetsPlus.Bloom], prepare: nil, test_size: 1_500_000}
    all = all ++ [{context, "bloom", &bloom_test/2}]

    # The impact of a faster hash doesn't seem relevant in testing, so we stick with sha256
    hashes = [
      :ripemd160,
      :sha,
      :sha224,
      :sha256,
      :sha384,
      :sha512,
      :sha3_224,
      :sha3_256,
      :sha3_384,
      :sha3_512,
      :blake2b,
      :blake2s,
      :md5,
      :md4
    ]

    context = %{rounds: 3, modules: hashes, prepare: nil, test_size: 1_500_000}
    all = all ++ [{context, ".hash", &hash_test/2}]

    all
  end

  defp filename(args \\ nil) do
    case args do
      nil ->
        num = Process.get(:filename_counter, 0)
        Process.put(:filename_counter, num + 1)
        'tmp/test_file_#{num}_detsplus_bench'

      bin when is_binary(bin) ->
        '#{filename()}_#{args}'

      args when is_list(args) ->
        filename(Enum.join(args, "_"))
    end
  end

  defp rm(filename) do
    File.rm_rf(filename)
  end

  defp open_file(module, filename) do
    module.open_file(List.to_atom(filename), file: filename)
  end
end
