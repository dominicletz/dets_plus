case System.argv() do
  [] -> DetsPlus.Bench.run()
  [name] -> DetsPlus.Bench.run(name)
end
