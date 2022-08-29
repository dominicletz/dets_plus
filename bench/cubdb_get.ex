defmodule DetsPlus.Bench.CubDBGet do
  alias DetsPlus.Bench.{DetsWrap, DetsPlusWrap}

  def run() do
    data_dir = "tmp/bm_get"

    cleanup = fn ->
      File.rm_rf(data_dir)
    end

    small = "small value"
    :rand.seed(:exsss, {1, 1, 1})
    one_kb = :rand.bytes(1024)
    one_mb = :rand.bytes(1024 * 1024)
    ten_mb = :rand.bytes(1024 * 1024 * 10)
    n = 100

    scenarios =
      for {label, module} <- [{"CubDB", CubDB}, {":dets", DetsWrap}, {"DetsPlus", DetsPlusWrap}] do
        label = "#{label}.get/2"
        fun = fn {key, db} -> module.get(db, key) end

        hooks = [
          before_scenario: fn input ->
            cleanup.()
            {:ok, db} = module.start_link(data_dir, auto_file_sync: false, auto_compact: false)
            for key <- 0..n, do: module.put(db, key, input)
            db
          end,
          after_scenario: fn db ->
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
        "small value" => small,
        "1KB value" => one_kb,
        "1MB value" => one_mb,
        "10MB value" => ten_mb
      },
      before_each: fn db ->
        key = :rand.uniform(n)
        {key, db}
      end
    )
  end
end
