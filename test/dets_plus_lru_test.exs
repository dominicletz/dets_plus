defmodule DetsPlus.LRU.Test do
  use ExUnit.Case
  doctest DetsPlus.LRU

  test "base" do
    lru = DetsPlus.LRU.new(open_dets("base"), 10)
    assert DetsPlus.LRU.size(lru) == 0

    DetsPlus.LRU.put(lru, "key", "value")
    assert DetsPlus.LRU.size(lru) == 1

    assert DetsPlus.LRU.get(lru, "key") == "value"

    # DetsPlus.LRU should not cache nil return values
    assert DetsPlus.LRU.fetch(lru, "nothing", fn -> nil end) == nil
    assert DetsPlus.LRU.fetch(lru, "nothing", fn -> "yay" end) == "yay"
    assert DetsPlus.LRU.get(lru, "nothing") == "yay"
  end

  test "limit" do
    lru = DetsPlus.LRU.new(open_dets("limit"), 3)
    assert DetsPlus.LRU.size(lru) == 0

    DetsPlus.LRU.put(lru, "a", "avalue")
    DetsPlus.LRU.put(lru, "b", "bvalue")
    DetsPlus.LRU.put(lru, "c", "cvalue")

    assert DetsPlus.LRU.size(lru) == 3
    assert DetsPlus.LRU.get(lru, "a") == "avalue"
    assert DetsPlus.LRU.get(lru, "b") == "bvalue"
    assert DetsPlus.LRU.get(lru, "c") == "cvalue"

    DetsPlus.LRU.put(lru, "d", "dvalue")

    assert DetsPlus.LRU.size(lru) == 3
    assert DetsPlus.LRU.get(lru, "a") == nil
    assert DetsPlus.LRU.get(lru, "b") == "bvalue"
    assert DetsPlus.LRU.get(lru, "c") == "cvalue"
    assert DetsPlus.LRU.get(lru, "d") == "dvalue"
  end

  test "repeat" do
    lru = DetsPlus.LRU.new(open_dets("repeat"), 3)
    assert DetsPlus.LRU.size(lru) == 0

    DetsPlus.LRU.put(lru, "a", "avalue")
    DetsPlus.LRU.put(lru, "b", "bvalue")
    DetsPlus.LRU.put(lru, "c", "cvalue")

    assert DetsPlus.LRU.size(lru) == 3
    assert DetsPlus.LRU.get(lru, "a") == "avalue"
    assert DetsPlus.LRU.get(lru, "b") == "bvalue"
    assert DetsPlus.LRU.get(lru, "c") == "cvalue"

    DetsPlus.LRU.put(lru, "a", "avalue2")

    assert DetsPlus.LRU.size(lru) == 3
    assert DetsPlus.LRU.get(lru, "a") == "avalue2"
    assert DetsPlus.LRU.get(lru, "b") == "bvalue"
    assert DetsPlus.LRU.get(lru, "c") == "cvalue"
  end

  test "misc" do
    lru = DetsPlus.LRU.new(open_dets("misc"), 3)

    DetsPlus.LRU.put(lru, "a", "avalue")
    DetsPlus.LRU.put(lru, "b", "bvalue")
    DetsPlus.LRU.put(lru, "c", "cvalue")

    assert Enum.sort(DetsPlus.LRU.to_list(lru)) ==
             [{"a", "avalue"}, {"b", "bvalue"}, {"c", "cvalue"}]

    DetsPlus.LRU.flush(lru)
    assert DetsPlus.LRU.size(lru) == 0
    assert DetsPlus.LRU.get(lru, "a") == nil
    assert DetsPlus.LRU.get(lru, "b") == nil
    assert DetsPlus.LRU.get(lru, "c") == nil
  end

  test "file_size" do
    size = 3

    for i <- 1..5 do
      dets = open_dets("test_file_size")
      lru = DetsPlus.LRU.new(dets, size)

      for x <- 1..50 do
        for s <- 1..size do
          DetsPlus.LRU.put(lru, "#{x}-#{s}-#{i}", "#{x}-#{s}")
        end

        DetsPlus.sync(dets)
        assert Enum.count(dets) == size * 2 + 1
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
