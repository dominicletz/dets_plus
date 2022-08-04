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

  def run() do
    # :observer.start()
    rounds = 3
    test_size = 100_000
    modules = [:dets, DetsPlus]

    for module <- modules do
      # for module <- [DetsPlus] do
      IO.puts("running rw test: #{module}")

      for _ <- 1..rounds do
        {time, :ok} = :timer.tc(&test/2, [module, test_size])
        IO.puts("#{time}μs")
      end
    end

    for module <- modules do
      IO.puts("running read test: #{module}")
      prepare_read_test(module, test_size)

      for _ <- 1..rounds do
        {time, :ok} = :timer.tc(&read_test/2, [module, test_size])
        IO.puts("#{time}μs")
      end
    end
  end
end
