defmodule DetsPlus.HashLRU.Test do
  use ExUnit.Case
  doctest DetsPlus.HashLRU

  test "base" do
    lru = DetsPlus.HashLRU.new(open_dets("base"), 10)
    assert DetsPlus.HashLRU.size(lru) == 0

    DetsPlus.HashLRU.put(lru, "key", "value")
    assert DetsPlus.HashLRU.size(lru) == 1

    assert DetsPlus.HashLRU.get(lru, "key") == "value"

    # DetsPlus.HashLRU should not cache nil return values
    assert DetsPlus.HashLRU.fetch(lru, "nothing", fn -> nil end) == nil
    assert DetsPlus.HashLRU.fetch(lru, "nothing", fn -> "yay" end) == "yay"
    assert DetsPlus.HashLRU.get(lru, "nothing") == "yay"
  end

  test "limit" do
    lru = DetsPlus.HashLRU.new(open_dets("limit"), 3)
    assert DetsPlus.HashLRU.size(lru) == 0

    DetsPlus.HashLRU.put(lru, "a", "avalue")
    DetsPlus.HashLRU.put(lru, "b", "bvalue")
    DetsPlus.HashLRU.put(lru, "c", "cvalue")

    assert DetsPlus.HashLRU.size(lru) == 3
    # we assert 2 of three should be present:
    count =
      [
        DetsPlus.HashLRU.get(lru, "a") == "avalue",
        DetsPlus.HashLRU.get(lru, "b") == "bvalue",
        DetsPlus.HashLRU.get(lru, "c") == "cvalue"
      ]
      |> Enum.count(fn z -> z == true end)

    assert count == 2

    DetsPlus.HashLRU.put(lru, "d", "dvalue")

    assert DetsPlus.HashLRU.size(lru) == 3
    assert DetsPlus.HashLRU.get(lru, "a") == nil
    assert DetsPlus.HashLRU.get(lru, "b") == "bvalue"
    assert DetsPlus.HashLRU.get(lru, "c") == "cvalue"
    assert DetsPlus.HashLRU.get(lru, "d") == "dvalue"
  end

  test "repeat" do
    lru = DetsPlus.HashLRU.new(open_dets("repeat"), 300)
    assert DetsPlus.HashLRU.size(lru) == 0

    DetsPlus.HashLRU.put(lru, "a", "avalue")
    DetsPlus.HashLRU.put(lru, "b", "bvalue")
    DetsPlus.HashLRU.put(lru, "c", "cvalue")

    assert DetsPlus.HashLRU.size(lru) == 3
    assert DetsPlus.HashLRU.get(lru, "a") == "avalue"
    assert DetsPlus.HashLRU.get(lru, "b") == "bvalue"
    assert DetsPlus.HashLRU.get(lru, "c") == "cvalue"

    DetsPlus.HashLRU.put(lru, "a", "avalue2")

    assert DetsPlus.HashLRU.size(lru) == 4
    assert DetsPlus.HashLRU.get(lru, "a") == "avalue2"
    assert DetsPlus.HashLRU.get(lru, "b") == "bvalue"
    assert DetsPlus.HashLRU.get(lru, "c") == "cvalue"
  end

  test "misc" do
    lru = DetsPlus.HashLRU.new(open_dets("misc"), 3)

    DetsPlus.HashLRU.put(lru, "a", "avalue")
    DetsPlus.HashLRU.put(lru, "b", "bvalue")
    DetsPlus.HashLRU.put(lru, "c", "cvalue")

    list = DetsPlus.HashLRU.to_list(lru)
    assert Enum.count(list) >= 2
    assert list -- [{"a", "avalue"}, {"b", "bvalue"}, {"c", "cvalue"}] == []

    DetsPlus.HashLRU.flush(lru)
    assert DetsPlus.HashLRU.size(lru) == 0
    assert DetsPlus.HashLRU.get(lru, "a") == nil
    assert DetsPlus.HashLRU.get(lru, "b") == nil
    assert DetsPlus.HashLRU.get(lru, "c") == nil
  end

  test "file_size" do
    size = 3

    for i <- 1..5 do
      dets = open_dets("test_file_size")
      lru = DetsPlus.HashLRU.new(dets, size)

      for x <- 1..50 do
        for s <- 1..size do
          DetsPlus.HashLRU.put(lru, "#{x}-#{s}-#{i}", "#{x}-#{s}")
        end

        DetsPlus.sync(dets)
        assert 1 < Enum.count(dets) and Enum.count(dets) <= size + 1
      end

      DetsPlus.close(dets)
    end
  end

  defp open_dets(name, args \\ []) do
    File.rm(name)
    {:ok, dets} = DetsPlus.open_file(String.to_atom(name), args)
    dets
  end
end
