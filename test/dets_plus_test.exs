defmodule DetsPlus.Test do
  use ExUnit.Case, async: true

  describe "open_file" do
    test "check for overlapping buckets" do
      dets = open_dets("test_file")

      range = 1..5000

      for x <- range do
        assert DetsPlus.insert(dets, {x}) == :ok
      end

      for x <- range do
        assert DetsPlus.lookup(dets, x) == [{x}]
      end

      DetsPlus.sync(dets)

      # IO.inspect({:dets, Enum.to_list(dets) |> Enum.sort()})

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

    test "creates new database and writes values" do
      dets = open_dets("test_file2")

      assert DetsPlus.info(dets, :size) == 0
      assert DetsPlus.lookup(dets, 1) == []
      assert DetsPlus.insert(dets, {1, 1, 1}) == :ok
      assert DetsPlus.info(dets, :size) == 1
      assert DetsPlus.lookup(dets, 1) == [{1, 1, 1}]
      assert DetsPlus.insert_new(dets, {1, 1, 1}) == false
      assert DetsPlus.delete_object(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == []
      assert DetsPlus.lookup(dets, 1) == []
      assert DetsPlus.insert_new(dets, {1, 1, 1}) == true

      # Save and reopen to test persistency
      assert DetsPlus.close(dets) == :ok
      {:ok, dets} = DetsPlus.open_file(:test_file2)
      assert DetsPlus.info(dets, :size) == 1
      assert DetsPlus.lookup(dets, 1) == [{1, 1, 1}]
      assert DetsPlus.insert_new(dets, {1, 1, 1}) == false
      assert DetsPlus.delete_object(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == []
      assert DetsPlus.lookup(dets, 1) == []
      assert DetsPlus.insert_new(dets, {1, 1, 1}) == true

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

    test "member" do
      dets = open_dets("test_file3")

      assert DetsPlus.member?(dets, 1) == false
      assert DetsPlus.insert(dets, {1, 1, 1}) == :ok
      assert DetsPlus.member?(dets, 1) == true
    end

    test "keypos != 1" do
      dets = open_dets("test_file4", keypos: 2)

      assert DetsPlus.info(dets, :size) == 0
      assert DetsPlus.lookup(dets, 1) == []
      assert DetsPlus.insert(dets, {1, 1, 1}) == :ok
      assert DetsPlus.info(dets, :size) == 1
      assert DetsPlus.lookup(dets, 1) == [{1, 1, 1}]
      assert DetsPlus.insert_new(dets, {1, 1, 1}) == false

      # Save and reopen to test persistency
      assert DetsPlus.close(dets) == :ok
      {:ok, dets} = DetsPlus.open_file(:test_file4)
      assert DetsPlus.info(dets, :size) == 1
      assert DetsPlus.lookup(dets, 1) == [{1, 1, 1}]
      assert DetsPlus.insert_new(dets, {1, 1, 1}) == false

      # Overwrite a value
      assert DetsPlus.insert(dets, {2, 1, 2}) == :ok
      assert DetsPlus.lookup(dets, 1) == [{2, 1, 2}]
      assert DetsPlus.sync(dets) == :ok
      assert DetsPlus.lookup(dets, 1) == [{2, 1, 2}]

      # Overwrite again
      assert DetsPlus.insert(dets, {3, 1, 3}) == :ok
      assert DetsPlus.lookup(dets, 1) == [{3, 1, 3}]
      assert DetsPlus.sync(dets) == :ok
      assert DetsPlus.lookup(dets, 1) == [{3, 1, 3}]
    end

    test "delete_all_objects" do
      File.rm("test_file5")
      {:ok, dets} = DetsPlus.open_file(:test_file5)

      for x <- 1..1000 do
        DetsPlus.insert(dets, {x})
      end

      assert DetsPlus.count(dets) == 1000
      assert DetsPlus.start_sync(dets) == :ok
      assert DetsPlus.delete_all_objects(dets) == :ok
      assert DetsPlus.count(dets) == 0
      assert DetsPlus.lookup(dets, 1) == []
      assert Enum.to_list(dets) == []

      # Test run 2 with a  pre-existing file
      DetsPlus.insert(dets, {1})
      assert Enum.to_list(dets) == [{1}]
      DetsPlus.close(dets)

      {:ok, dets} = DetsPlus.open_file(:test_file5)
      assert Enum.to_list(dets) == [{1}]

      for x <- 1..1000 do
        DetsPlus.insert(dets, {x})
      end

      assert DetsPlus.count(dets) == 1001
      assert DetsPlus.start_sync(dets) == :ok
      assert DetsPlus.delete_all_objects(dets) == :ok
      assert DetsPlus.count(dets) == 0
      assert DetsPlus.lookup(dets, 1) == []
      assert Enum.to_list(dets) == []
    end

    test "map storage" do
      dets = open_dets("test_file6", keypos: :id)
      DetsPlus.insert(dets, %{id: 1, value: 42})
      [%{id: 1, value: 42}] = DetsPlus.lookup(dets, 1)
      :ok = DetsPlus.close(dets)
    end

    alias DetsPlus.FileWriter

    test "file writer limit" do
      PagedFile.delete("test_file7")
      {:ok, fp} = PagedFile.open("test_file7")
      PagedFile.pwrite(fp, 0, "1234567890")
      writer = FileWriter.new(fp, 0, limit: 5, module: PagedFile)
      writer = FileWriter.write(writer, "abcdefghijklmnopqrstuvw")
      FileWriter.sync(writer)
      assert PagedFile.pread(fp, 0, 10) == {:ok, "abcde67890"}
    end

    test "delete" do
      dets = open_dets("test_file8")

      assert Enum.to_list(dets) == []
      assert DetsPlus.delete(dets, 1) == :ok
      assert DetsPlus.insert(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == [{1, 1, 1}]
      assert DetsPlus.delete(dets, 1) == :ok
      assert Enum.to_list(dets) == []

      assert DetsPlus.insert(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == [{1, 1, 1}]
      assert DetsPlus.delete_object(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == []
    end

    test "leave no zombies" do
      assert count_processes_by_module(PagedFile) == 0
      dets1 = open_dets("test_file9")
      DetsPlus.insert(dets1, {:a, :b})
      DetsPlus.sync(dets1)
      assert count_processes_by_module(PagedFile) == 1
      DetsPlus.close(dets1)
      assert count_processes_by_module(PagedFile) == 0
    end

    def count_processes_by_module(module) do
      Process.list()
      |> Enum.map(fn p -> Process.info(p)[:dictionary][:"$initial_call"] end)
      |> Enum.count(fn tuple -> is_tuple(tuple) and elem(tuple, 0) == module end)
    end

    defp open_dets(name, args \\ []) do
      File.rm(name)
      {:ok, dets} = DetsPlus.open_file(String.to_atom(name), args)
      dets
    end
  end
end
