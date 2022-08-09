defmodule DetsPlus.Bench do
  @moduledoc false
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

  def read_test(module, test_size) do
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
    filename = 'test_file_dets_write_bench.#{module}'
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

  def run(context = %{test_size: test_size, modules: modules, rounds: rounds}, label, fun) do
    for module <- modules do
      IO.puts("running #{label} test: #{module}")

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

  defp tc(fun, args) do
    {time, :ok} = :timer.tc(fun, args)
    time
  end

  def run() do
    # :observer.start()
    context = %{rounds: 3, modules: [DetsPlus], prepare: &prepare_sync_test/2, test_size: 50_000}
    run(%{context | test_size: 150_000}, "sync_test 150_000", &sync_test/3)
    run(%{context | test_size: 1_500_000}, "sync_test 1_500_000", &sync_test/3)

    context = %{context | modules: [:dets, DetsPlus], prepare: nil}
    run(context, "write", &write_test/2)
    run(context, "rw", &test/2)
    run(context, "read", &read_test/2)
  end
end
