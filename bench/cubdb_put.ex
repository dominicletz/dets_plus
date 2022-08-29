defmodule DetsPlus.Bench.CubDBPut do
  alias DetsPlus.Bench.{DetsWrap, DetsPlusWrap}

  def run() do
    data_dir = "tmp/bm_put"

    cleanup = fn ->
      File.rm_rf(data_dir)
    end

    small = "small value"
    :rand.seed(:exsss, {1, 1, 1})
    one_kb = :rand.bytes(1024)
    one_mb = :rand.bytes(1024 * 1024)
    ten_mb = :rand.bytes(1024 * 1024 * 10)

    scenarios =
      for {label, module} <- [{"CubDB", CubDB}, {":dets", DetsWrap}, {"DetsPlus", DetsPlusWrap}] do
        label = "#{label}.put/3"
        fun = fn {key, value, db} -> module.put(db, key, value) end

        hooks = [
          before_scenario: fn {value, options} ->
            cleanup.()
            {:ok, db} = module.start_link(data_dir, options)
            {value, db}
          end,
          after_scenario: fn {_value, db} ->
            IO.puts("#{module.size(db)} entries written to database.")
            module.stop(db)
            cleanup.()
          end
        ]

        {label, {fun, hooks}}
      end
      |> Map.new()

    Benchee.run(
      scenarios,
      inputs: %{
        "small value, auto sync" => {small, [auto_compact: false, auto_file_sync: true]},
        "small value" => {small, [auto_compact: false, auto_file_sync: false]},
        "1KB value" => {one_kb, [auto_compact: false, auto_file_sync: false]},
        "1MB value" => {one_mb, [auto_compact: false, auto_file_sync: false]},
        "10MB value" => {ten_mb, [auto_compact: false, auto_file_sync: false]}
      },
      before_each: fn {value, db} ->
        key = :rand.uniform(10_000)
        {key, value, db}
      end
    )
  end
end
