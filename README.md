# DetsPlus

DetsPlus persistent tuple storage.

DetsPlus has a similiar API as `dets` but without
the 2GB file storage limit. Writes are buffered in an
internal ETS table and synced every `auto_save` period
to the persistent storage.

While `sync()` or `auto_save` is in progress the database
can still read and written.

There is no commitlog so not synced writes are lost.
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

- Add `delete*` functions and add tombstone markers to the ets table
- Add `update_counter/3`
- Add `traverse/2`, `foldr/3`, `first/1` ,`next/1` based on the `iterate()` function
- Add `match()/select()` - no idea how to do that efficiently though?
- Add support for `bag` and `sorted_set` 

- Maybe allow customizing the hash function?
- Maybe allow customizing bloom filter size / usage?
- Maybe use bits for smaller bloom filter?
- Maybe adaptively choose @slot_size and @entry_size based on biggest entry and total number of entries for smaller file 

## Installation

The package can be installed by adding `dets_plus` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:dets_plus, "~> 1.0"}
  ]
end
```

The docs can be found at [https://hexdocs.pm/dets_plus](https://hexdocs.pm/dets_plus).

## Performance

As mentioned above reading `DetsPlus` is slower than `:dets` because `DetsPlus` is reading from disk primarily. Here 
some measurements:

```bash
$ mix run bench/dets_plus.exs 
running sync_test 150_000 test: Elixir.DetsPlus
1.426s
1.406s
1.363s
running sync_test 1_500_000 test: Elixir.DetsPlus
18.489s
18.567s
18.031s

running write test: dets
4.491s
4.502s
4.658s
running write test: Elixir.DetsPlus
1.885s
1.853s
1.842s

running rw test: dets
3.026s
3.023s
2.981s
running rw test: Elixir.DetsPlus
2.986s
2.996s
2.946s

running read test: dets
1.079s
0.992s
1.017s
running read test: Elixir.DetsPlus
4.251s
4.229s
4.303s
```
