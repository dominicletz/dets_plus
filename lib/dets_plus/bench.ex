defmodule DetsPlus.Bench do
  @moduledoc false

  def bloom_test(module, test_size) do
    bloom = module.create(test_size * 10)

    _ =
      Enum.reduce(0..test_size, bloom, fn x, bloom ->
        module.add(bloom, <<x::unsigned-size(64)>>)
      end)

    :ok
  end

  def test(module, test_size) do
    File.rm("test_file_dets_bench")
    {:ok, dets} = module.open_file(:test_file_dets_bench, [])

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
    filename = 'test_file_dets_read_bench.#{module}'

    if File.exists?(filename) do
      :ok
    else
      {:ok, dets} = module.open_file(:test_file_dets_read_bench, file: filename)

      for x <- 0..test_size do
        :ok = module.insert(dets, {x, x * x})
      end

      :ok = module.close(dets)
    end
  end

  def read_test(_, module, test_size) do
    filename = 'test_file_dets_read_bench.#{module}'
    {:ok, dets} = module.open_file(:test_file_dets_bench, file: filename)

    for x <- 0..test_size do
      module.lookup(dets, test_size + x)
      module.lookup(dets, x)
    end

    :ok = module.close(dets)
  end

  def write_test(module, test_size) do
    filename = 'test_file_dets_write_bench.#{module}'
    File.rm(filename)
    {:ok, dets} = module.open_file(:test_file_dets_bench, file: filename)

    for x <- 0..test_size do
      module.insert(dets, {x, x * x})
    end

    :ok = module.sync(dets)

    for x <- 0..test_size do
      module.insert(dets, {x + test_size, x * (x + test_size)})
    end

    :ok = module.close(dets)
  end

  def prepare_sync_test(module, test_size) do
    filename = 'test_file_dets_sync_bench.#{module}'
    File.rm(filename)
    {:ok, dets} = module.open_file(:test_file_dets_bench, file: filename, auto_save: :never)

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
    filename = 'test_file_dets_sync_bench.#{module}'
    {:ok, dets} = module.open_file(:test_file_dets_bench, file: filename, auto_save: :never)
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

  def run() do
    # :observer.start()

    context = %{rounds: 3, modules: [:dets, DetsPlus], prepare: nil, test_size: 50_000}
    run(context, "write", &write_test/2)
    run(context, "rw", &test/2)
    run(%{context | prepare: &prepare_read_test/2}, "read", &read_test/3)

    context = %{rounds: 3, modules: [DetsPlus], prepare: &prepare_sync_test/2, test_size: nil}
    run(%{context | test_size: 150_000}, "sync_test: 0 + 150_000 new inserts", &sync_test/3)
    run(%{context | test_size: 1_500_000}, "sync_test: 0 + 1_500_000 new inserts", &sync_test/3)
    run(%{context | prepare: nil}, "sync_test 1_500_000 + 1 new inserts", &sync_test2/2)

    context = %{rounds: 3, modules: [DetsPlus.Bloom], prepare: nil, test_size: 1_500_000}
    run(context, "bloom", &bloom_test/2)
  end
end
