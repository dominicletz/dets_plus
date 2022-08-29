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
- `DetsPlus` is SLOWER in reading than `:dets` because `DetsPlus` reads from disk. 
- `DetsPlus` updates it's CDB database atomically making it more resistant to power failures such as in Nerves.
- Only the type = `:set` is supported at the moment

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
    {:dets_plus, "~> 2.1"}
  ]
end
```

The docs can be found at [https://hexdocs.pm/dets_plus](https://hexdocs.pm/dets_plus).

## Comparison to CubDB

When implementing DetsPlus I wasn't aware of CubDB which is another pure Elixir implementation of a Key-Value-Store. CubDB is more mature and has more features -- so go with that one if you don't want to worry about stuff. BUT DetsPlus is based on DJBs hash table format and super fast, so if you're happy to ride the bleeding edge go for it

Technical differences:
- CubDB is using btrees / DetsPlus is using a hash table - reads and writes are faster
- CubDB is ordered and allows range queries based on the order / DetsPlus not
- CubDB has atomic transactions / DetsPlus not
- CubDB is using an auto-append file format and has fine grained commit control / DetsPlus not, it has no incremental append and has to replace the whole file on sync.

Performance differences
- DetsPlus is significantly faster for reads and writes of most data because of it's hash table structure
- For huge entries (>1mb per entry) CubDB is faster in writing because of it's incremental file structure

## Performance Measurements

The initial write/read-write/read tests are all executed on 50,000 small elements. Lower numbers are better:

```
$ mix run scripts/bench.exs
running write test: :dets
4.915s
4.754s
4.767s
running write test: DetsPlus
1.092s
1.078s
1.161s
running write test: DetsPlus.Bench.CubDBWrap
32.054s
30.963s
31.279s

running rw test: :dets
3.375s
3.306s
3.357s
running rw test: DetsPlus
2.196s
2.259s
2.257s
running rw test: DetsPlus.Bench.CubDBWrap
22.771s
20.924s
20.854s

running read test: :dets
0.998s
0.966s
0.966s
running read test: DetsPlus
1.699s
1.675s
1.675s
running read test: DetsPlus.Bench.CubDBWrap
6.815s
7.053s
7.024s

running large_write test: DetsPlus
40.331s
40.882s
47.976s
running large_write test: DetsPlus.Bench.CubDBWrap
17.838s
16.143s
16.133s

running sync_test: 0 + 150_000 new inserts test: DetsPlus
0.672s
0.632s
0.683s

running sync_test: 0 + 1_500_000 new inserts test: DetsPlus
6.677s
6.648s
6.272s

running sync_test 1_500_000 + 1 new inserts test: DetsPlus
3.0s
2.617s
2.845s
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