# DetsPlus

DetsPlus persistent tuple storage.

DetsPlus has a similiar API as `dets` but without
the 2GB file storage limit. Writes are buffered in an
internal ETS table and synced every `auto_save` period
to the persistent storage.

While `sync()` or `auto_save` is in progress the database
can still be read from and written to.

`DetsPlus` supports the `Enumerable` protocol, so you can most of the `Enum.*` functions on `DetsPlus`

There is no commitlog - not synced writes are lost.
Lookups are possible by key and non-matches are accelerated
using a bloom filter. The persistent file concept follows
DJ Bernsteins CDB database format, but uses an Elixir
encoded header https://cr.yp.to/cdb.html

Limits are:

- Total file size: 18.446 Petabyte
- Maximum per entry size: 4.2 Gigabyte
- Maximum entry count: `:infinity`

## Notes

- `:dets` is limited to 2gb of data `DetsPlus` has no such limit.
- `DetsPlus` is SLOWER in reading than `:dets` because it goes to disk for that. 
- Only type = `:set` is supported

The `:dets` limitation of 2gb caused me to create this library. I needed to store and lookup key-value pairs from sets larger than what fits into memory. Thus the current implementation did not try to be equivalent to `:dets` nor to be complete. Instead it's focused on storing large amounts of values and have fast lookups. PRs to make it more complete and use it for other things are welcome. 

## Basic usage

```elixir
{:ok, dets} = DetsPlus.open_file(:example)
DetsPlus.insert(dets, {1, 1, 1})
[{1, 1, 1}] = DetsPlus.lookup(dets, 1)
:ok =  DetsPlus.close(dets)
```

## Ideas for PRs and future improvements

- Add the `delete_object/2` function
- Add `update_counter/3`
- Add support for `bag` and `sorted_set`/`ordered_set`
- ~~Add `traverse/2`, `foldr/3`, `first/1` ,`next/1`~~ `Enumerable` protocol is supported now. Use the `Enum.*` functions
- Further reduce memory usage while syncing the hash tables to disk

- Maybe Add `match()/select()` - no idea how to do that efficiently though?
- Maybe allow customizing the hash function?
- Maybe allow customizing bloom filter size / usage?
- Maybe use bits for smaller bloom filter?
- Maybe adaptively choose @slot_size and @entry_size based on biggest entry and total number of entries for smaller file 

## Installation

The package can be installed by adding `dets_plus` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:dets_plus, "~> 2.0"}
  ]
end
```

The docs can be found at [https://hexdocs.pm/dets_plus](https://hexdocs.pm/dets_plus).

## Performance

As mentioned above reading `DetsPlus` is slower than `:dets` because `DetsPlus` is reading from disk primarily. Here 
some measurements:

```bash
$ mix run scripts/bench.exs 
running write test: :dets
4.357s
4.358s
4.261s
running write test: DetsPlus
1.403s
1.328s
1.358s

running rw test: :dets
2.896s
2.873s
2.958s
running rw test: DetsPlus
2.831s
2.753s
2.8s

running read test: :dets
0.946s
1.043s
0.941s
running read test: DetsPlus
2.352s
2.349s
2.283s

running sync_test: 0 + 150_000 new inserts test: DetsPlus
0.939s
0.927s
0.926s

running sync_test: 0 + 1_500_000 new inserts test: DetsPlus
11.531s
11.776s
11.288s

running sync_test 1_500_000 + 1 new inserts test: DetsPlus
9.589s
10.039s
9.882s
```
