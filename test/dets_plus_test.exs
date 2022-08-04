defmodule DetsPlus.Test do
  use ExUnit.Case, async: true

  describe "open_file" do
    test "check for overlapping buckets" do
      File.rm("test_file")
      {:ok, dets} = DetsPlus.open_file(:test_file)

      range = 1..5000

      for x <- range do
        assert DetsPlus.insert(dets, {x}) == :ok
      end

      for x <- range do
        assert DetsPlus.lookup(dets, x) == [{x}]
      end

      DetsPlus.sync(dets)

      for x <- range do
        assert DetsPlus.lookup(dets, x) == [{x}]
      end

      range = 5000..10_000

      for x <- range do
        assert DetsPlus.insert(dets, {x}) == :ok
      end

      DetsPlus.sync(dets)

      for x <- 1..10_000 do
        assert DetsPlus.lookup(dets, x) == [{x}]
      end
    end

    # test "check compression" do
    #   File.rm("test_file.gz")
    #   {:ok, dets} = DetsPlus.open_file(:test_file, file: "test_file.gz")

    #   range = 1..16

    #   for x <- range do
    #     assert DetsPlus.insert(dets, {x}) == true
    #   end

    #   for x <- range do
    #     assert DetsPlus.lookup(dets, x) == [{x}]
    #   end

    #   DetsPlus.sync(dets)

    #   for x <- range do
    #     assert DetsPlus.lookup(dets, x) == [{x}]
    #   end
    # end

    test "creates new database and writes values" do
      File.rm("test_file2")
      {:ok, dets} = DetsPlus.open_file(:test_file2)

      assert DetsPlus.info(dets, :size) == 0
      assert DetsPlus.lookup(dets, 1) == []
      assert DetsPlus.insert(dets, {1, 1, 1}) == :ok
      assert DetsPlus.info(dets, :size) == 1
      assert DetsPlus.lookup(dets, 1) == [{1, 1, 1}]
      assert DetsPlus.insert_new(dets, {1, 1, 1}) == false

      # Save and reopen to test persistency
      assert DetsPlus.close(dets) == :ok
      {:ok, dets} = DetsPlus.open_file(:test_file2)
      assert DetsPlus.info(dets, :size) == 1
      assert DetsPlus.lookup(dets, 1) == [{1, 1, 1}]
      assert DetsPlus.insert_new(dets, {1, 1, 1}) == false

      # Overwrite a value
      assert DetsPlus.insert(dets, {1, 2, 2}) == :ok
      assert DetsPlus.lookup(dets, 1) == [{1, 2, 2}]
      assert DetsPlus.sync(dets) == :ok
      assert DetsPlus.lookup(dets, 1) == [{1, 2, 2}]

      # Overwrite again
      assert DetsPlus.insert(dets, {1, 2, 3}) == :ok
      assert DetsPlus.lookup(dets, 1) == [{1, 2, 3}]
      assert DetsPlus.sync(dets) == :ok
      assert DetsPlus.lookup(dets, 1) == [{1, 2, 3}]
    end
  end
end
