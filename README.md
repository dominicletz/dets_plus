# DetsPlus

DetsPlus persistent tuple/struct/map storage.

DetsPlus has a similiar API as `dets` but without
the 2GB file storage limit. Writes are buffered in an
internal ETS table and synced every `auto_save` period
to the persistent storage.

While `sync()` or `auto_save` is in progress the database
can still be read from and written to.

`DetsPlus` supports the `Enumerable` protocol, so you can use most of the `Enum.*` functions on `DetsPlus`

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
- `DetsPlus` supports the `Enumerable` protocol. So you can call `Enum.reduce()` on it.
- `DetsPlus` supports Maps and Structs in addition to tuples when `keypos` is an atom (name of the key field).
- `DetsPlus` is SLOWER in reading than `:dets` because it goes to disk for that. 
- Only type = `:set` is supported

The `:dets` limitation of 2gb caused me to create this library. I needed to store and lookup key-value pairs from sets larger than what fits into memory. Thus the current implementation did not try to be equivalent to `:dets` nor to be complete. Instead it's focused on storing large amounts of values and have fast lookups. PRs to make it more complete and use it for other things are welcome. 

## Basic usage

With tuples:

```elixir
{:ok, dets} = DetsPlus.open_file(:example)
DetsPlus.insert(dets, {1, 1, 1})
[{1, 1, 1}] = DetsPlus.lookup(dets, 1)
:ok =  DetsPlus.close(dets)
```

With maps/structs:

```elixir
{:ok, dets} = DetsPlus.open_file(:example, keypos: :id)
DetsPlus.insert(dets, %{id: 1, value: 42})
[{%id: 1, value: 42}] = DetsPlus.lookup(dets, 1)
:ok =  DetsPlus.close(dets)
```

## Ideas for PRs and future improvements

- Add `update_counter/3`
- Add support for `bag` and `sorted_set`/`ordered_set`
- Maybe Add `match()/select()` - no idea how to do that efficiently though?

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

```
$ mix run scripts/bench.exs
running write test: :dets
4.792s
4.869s
4.861s
running write test: DetsPlus
1.223s
1.098s
1.001s

running rw test: :dets
4.2s
3.118s
3.485s
running rw test: DetsPlus
2.057s
2.019s
2.03s

running read test: :dets
0.929s
0.905s
0.905s
running read test: DetsPlus
1.617s
1.657s
1.645s

running sync_test: 0 + 150_000 new inserts test: DetsPlus
0.577s
0.599s
0.609s

running sync_test: 0 + 1_500_000 new inserts test: DetsPlus
6.999s
6.689s
6.526s

running sync_test 1_500_000 + 1 new inserts test: DetsPlus
2.915s
2.818s
2.812s

running bloom test: DetsPlus.Bloom
0.277s
0.254s
0.276s
```

## File Structure
 
The data format has been inspired by DJ Bernsteins CDB database format, but with changes to allow
relatively low memory usage when creating the file in a single pass. 

1) The header has been moved to the end of the file, to allow for a header who's size is not known before writing the data entries. The header is an encoded & compressed Erlang term - for fast retrieval in Elixir.

2) There is an additional storage overhead (compared to CDB) for storing the 64bit (8 bytes) hashes of all entries  twice in the file. This accelerates lookups and database updates but costs storage. 
For 1_000_000 entries this means an additional storage of 16MB.

3) The header includes bloom filter with the size of `(10/8)*entry_count` bytes. Again for 1_000_000 entries this means an additional storage and memory overhead (compared to CDB) of 1.25MB

4) Hash tables are sized in powers of two, and their sizes are stored in the compressed header (not before each table as in CDB). This means an additional storage overhead for empty hash table slots (compared to CDB) but faster read times. Also it allows to pick hash table slots in a way so they are in the same sort order the base hash itself. 

File Layout Overview:
```
<4 byte file id "DET+"> 
<all entries>
<256 hash tables>
<bloom filter>
<compressed header>
```

Detailed Layout:
```
<4 byte file id "DET+">

for x <- all_entries
  <8 byte entry_hash> <4 byte entry_size> <entry_blob (from term_to_binary())>
<16 bytes of zeros> 

for table <- 0..255
  <8 byte entry_hash> <8 entry_offset>

<binary bloom filter (size in header)>

<%DetsPlus.State{} from term_to_binary()>
<8 offset to %DetsPlus.State{}.
```

Because the header is at the very end of the file opening a file starts by reading the header offset from the last 8 bytes of the file. Then the header contains all additional required metadata and offsets to read entries and hash tables.  