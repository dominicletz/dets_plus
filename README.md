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
4.705s
4.335s
4.29s
running write test: DetsPlus
1.46s
1.385s
1.355s

running rw test: :dets
2.83s
2.862s
2.862s
running rw test: DetsPlus
2.606s
2.534s
2.564s

running read test: :dets
1.043s
0.935s
0.887s
running read test: DetsPlus
2.104s
2.091s
2.07s

running sync_test: 0 + 150_000 new inserts test: DetsPlus
1.035s
1.056s
1.033s

running sync_test: 0 + 1_500_000 new inserts test: DetsPlus
13.195s
13.062s
13.217s

running sync_test 1_500_000 + 1 new inserts test: DetsPlus
8.917s
8.816s
8.855s
```
