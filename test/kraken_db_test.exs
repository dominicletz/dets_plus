defmodule KrakenDB.Test do
  use ExUnit.Case, async: false

  describe "open_file" do
    test "check for overlapping buckets" do
      dets = open_kraken("test_dir")

      range = 1..5000

      for x <- range do
        assert KrakenDB.insert(dets, {x}) == :ok
      end

      for x <- range do
        assert KrakenDB.lookup(dets, x) == [{x}]
      end

      KrakenDB.sync(dets)

      # IO.inspect({:dets, Enum.to_list(dets) |> Enum.sort()})

      for x <- range do
        assert KrakenDB.lookup(dets, x) == [{x}]
      end

      range = 5000..10_000

      for x <- range do
        assert KrakenDB.insert(dets, {x}) == :ok
      end

      KrakenDB.sync(dets)

      for x <- 1..10_000 do
        assert KrakenDB.lookup(dets, x) == [{x}]
      end
    end

    test "creates new database and writes values" do
      dets = open_kraken("test_dir2")

      assert KrakenDB.info(dets, :size) == 0
      assert KrakenDB.lookup(dets, 1) == []
      assert KrakenDB.insert(dets, {1, 1, 1}) == :ok
      assert KrakenDB.info(dets, :size) == 1
      assert KrakenDB.lookup(dets, 1) == [{1, 1, 1}]
      assert KrakenDB.insert_new(dets, {1, 1, 1}) == false
      assert KrakenDB.delete_object(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == []
      assert KrakenDB.lookup(dets, 1) == []
      assert KrakenDB.insert_new(dets, {1, 1, 1}) == true

      # Save and reopen to test persistency
      assert KrakenDB.close(dets) == :ok
      dets = reopen_kraken("test_dir2")
      assert KrakenDB.info(dets, :size) == 1
      assert KrakenDB.lookup(dets, 1) == [{1, 1, 1}]
      assert KrakenDB.insert_new(dets, {1, 1, 1}) == false
      assert KrakenDB.delete_object(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == []
      assert KrakenDB.lookup(dets, 1) == []
      assert KrakenDB.insert_new(dets, {1, 1, 1}) == true

      # Overwrite a value
      assert KrakenDB.insert(dets, {1, 2, 2}) == :ok
      assert KrakenDB.lookup(dets, 1) == [{1, 2, 2}]
      assert KrakenDB.sync(dets) == :ok
      assert KrakenDB.lookup(dets, 1) == [{1, 2, 2}]

      # Overwrite again
      assert KrakenDB.insert(dets, {1, 2, 3}) == :ok
      assert KrakenDB.lookup(dets, 1) == [{1, 2, 3}]
      assert KrakenDB.sync(dets) == :ok
      assert KrakenDB.lookup(dets, 1) == [{1, 2, 3}]
    end

    test "member" do
      dets = open_kraken("test_dir3")

      assert KrakenDB.member?(dets, 1) == false
      assert KrakenDB.insert(dets, {1, 1, 1}) == :ok
      assert KrakenDB.member?(dets, 1) == true
    end

    test "keypos != 1" do
      dets = open_kraken("test_dir4", keypos: 2)
      assert KrakenDB.info(dets, :keypos) == 2

      assert KrakenDB.info(dets, :size) == 0
      assert KrakenDB.lookup(dets, 1) == []
      assert KrakenDB.insert(dets, {1, 1, 1}) == :ok
      assert KrakenDB.info(dets, :size) == 1
      assert KrakenDB.lookup(dets, 1) == [{1, 1, 1}]
      assert KrakenDB.insert_new(dets, {1, 1, 1}) == false

      # Save and reopen to test persistency
      assert KrakenDB.close(dets) == :ok
      dets = reopen_kraken("test_dir4")
      assert KrakenDB.info(dets, :keypos) == 2
      assert KrakenDB.info(dets, :size) == 1
      assert KrakenDB.lookup(dets, 1) == [{1, 1, 1}]
      assert KrakenDB.insert_new(dets, {1, 1, 1}) == false

      # Overwrite a value
      assert KrakenDB.insert(dets, {2, 1, 2}) == :ok
      assert KrakenDB.lookup(dets, 1) == [{2, 1, 2}]
      assert KrakenDB.sync(dets) == :ok
      assert KrakenDB.lookup(dets, 1) == [{2, 1, 2}]

      # Overwrite again
      assert KrakenDB.insert(dets, {3, 1, 3}) == :ok
      assert KrakenDB.lookup(dets, 1) == [{3, 1, 3}]
      assert KrakenDB.sync(dets) == :ok
      assert KrakenDB.lookup(dets, 1) == [{3, 1, 3}]
    end

    test "delete_all_objects" do
      dets = open_kraken("test_dir5")

      for x <- 1..1000 do
        KrakenDB.insert(dets, {x})
      end

      assert KrakenDB.count(dets) == 1000
      assert KrakenDB.start_sync(dets) == :ok
      assert KrakenDB.delete_all_objects(dets) == :ok
      assert KrakenDB.count(dets) == 0
      assert KrakenDB.lookup(dets, 1) == []
      assert Enum.to_list(dets) == []

      # Test run 2 with a  pre-existing file
      KrakenDB.insert(dets, {1})
      assert Enum.to_list(dets) == [{1}]
      KrakenDB.close(dets)

      dets = reopen_kraken("test_dir5")
      assert Enum.to_list(dets) == [{1}]

      for x <- 1..1000 do
        KrakenDB.insert(dets, {x})
      end

      assert KrakenDB.count(dets) == 1001
      assert KrakenDB.start_sync(dets) == :ok
      assert KrakenDB.delete_all_objects(dets) == :ok
      assert KrakenDB.count(dets) == 0
      assert KrakenDB.lookup(dets, 1) == []
      assert Enum.to_list(dets) == []
    end

    test "map storage" do
      dets = open_kraken("test_dir6", keypos: :id)
      KrakenDB.insert(dets, %{id: 1, value: 42})
      [%{id: 1, value: 42}] = KrakenDB.lookup(dets, 1)
      :ok = KrakenDB.close(dets)
    end

    test "delete" do
      dets = open_kraken("test_dir8")

      assert Enum.to_list(dets) == []
      assert KrakenDB.delete(dets, 1) == :ok
      assert KrakenDB.insert(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == [{1, 1, 1}]
      assert KrakenDB.delete(dets, 1) == :ok
      assert Enum.to_list(dets) == []

      assert KrakenDB.insert(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == [{1, 1, 1}]
      assert KrakenDB.delete_object(dets, {1, 1, 1}) == :ok
      assert Enum.to_list(dets) == []
    end

    defp open_kraken(name, args \\ []) do
      name = "tmp/#{name}"
      File.rm_rf(name)
      {:ok, kdb} = KrakenDB.open_directory(name, args)
      kdb
    end

    defp reopen_kraken(name, args \\ []) do
      name = "tmp/#{name}"
      {:ok, kdb} = KrakenDB.open_directory(name, args)
      kdb
    end
  end
end
