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

Total file size: 18.446 Petabyte
Maximum entry size: 4.2 Gigabyte
Maximum entry count: :infinity

## Notes

- `:dets` is limited to 2gb of data `DetsPlus` has no such limit.
- `DetsPlus` is SLOWER in reading than `:dets` because it goes to disk for that. 

The `:dets` limitation of 2gb caused me to create this library. I needed to store and lookup key-value pairs from sets larger than what fits into memory. Thus the current implementation did not try to be equivalent to `:dets` nor to be complete. Instead it's focused on storing large amounts of values and have fast lookups. PRs to make it more complete and use it for other things are welcome. 

## Ideas for PRs and future improvements

- Add `dets:member/2`
- Add `delete*` functions and add tombstone markers to the ets table
- Add `update_counter/3`
- Add `traverse/2`, `foldr/3`, `first/1` ,`next/1` based on the `iterate()` function
- Add `match()/select()` - no idea how to do that efficiently though?

- Maybe allow customizing the hash function?
- Maybe allow customizing bloom filter size / usage?
- Maybe use bits for smaller bloom filter?
- Maybe compress header for smaller file
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

