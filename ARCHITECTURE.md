# Architecture

EngineeredWood is a .NET 10 library for reading and writing Apache Parquet files as Apache Arrow `RecordBatch` objects. It is designed around cloud storage access patterns — batched range reads, concurrent column chunk I/O, and pooled buffer management — rather than the traditional single-cursor `Stream` abstraction.

This document describes the implementation. For usage and build instructions, see `README.md`.

## Project layout

```
EngineeredWood/                 Main library (~63 source files)
  IO/                           Random-access and sequential I/O abstractions
    Azure/                      Azure Blob Storage backends
    Local/                      Local file backends
  Parquet/                      Format implementation
    Thrift/                     Thrift Compact Protocol codec
    Metadata/                   Footer metadata model and (de)serialization
    Schema/                     Schema tree with definition/repetition levels
    Data/                       Column encoding/decoding, Arrow conversion
    Compression/                Codec dispatch (read and write)
EngineeredWood.Tests/           xUnit tests
EngineeredWood.Benchmarks/      BenchmarkDotNet suites
EngineeredWood.Compatibility/   92-file cross-tool validation suite
parquet-testing/                Git submodule with 100+ sample files
```

## Conventions

- **Async model.** `ValueTask<T>` everywhere, `.ConfigureAwait(false)` on every await.
- **Buffer ownership.** I/O methods return `IMemoryOwner<byte>`; the caller disposes.
- **Ref structs.** Hot-path decoders (`ThriftCompactReader`, `RleBitPackedDecoder`, `DeltaBinaryPackedDecoder`) are `ref struct` over `ReadOnlySpan<byte>` — zero allocation, but they cannot be captured in lambdas or used across awaits.
- **Nullable enabled, implicit usings enabled.**
- **`checked()` casts** for `long → int` conversions; `ObjectDisposedException.ThrowIf()` for disposed guards.

---

## Layer 1 — I/O (`EngineeredWood.IO`)

The I/O layer replaces `Stream` with two interfaces designed for Parquet's access patterns.

### Reading: `IRandomAccessFile`

```
IRandomAccessFile
├── GetLengthAsync()
├── ReadAsync(FileRange)
└── ReadRangesAsync(IReadOnlyList<FileRange>)
```

No shared position cursor, so concurrent reads of disjoint ranges are safe. Implementations:

| Type | Notes |
|---|---|
| `LocalRandomAccessFile` | `RandomAccess` API; sequential sync reads (OS page cache efficient) |
| `AzureBlobRandomAccessFile` | HTTP range requests; `SemaphoreSlim` throttle (default 16); delegates multi-range reads to `CoalescingFileReader` |
| `CoalescingFileReader` | Decorator that merges nearby ranges (gap ≤ 64 KB, merged ≤ 16 MB) to reduce round trips, then slices results back to original order |

### Writing: `ISequentialFile`

Append-only: `WriteAsync(ReadOnlyMemory<byte>)` + `FlushAsync()` + `Position`. Implementations for local files (`LocalSequentialFile`) and Azure (`AzureBlobSequentialFile`).

### Buffer management

`BufferAllocator` is an abstract factory for `IMemoryOwner<byte>`. The concrete `PooledBufferAllocator` wraps `ArrayPool<byte>.Shared` and slices rented arrays to exact size, reducing GC pressure.

---

## Layer 2 — Thrift (`EngineeredWood.Parquet.Thrift`)

Parquet's footer and page headers are encoded in the Thrift Compact Protocol. EngineeredWood implements its own codec rather than depending on an external Thrift library.

### `ThriftCompactReader`

A `ref struct` over `ReadOnlySpan<byte>`. Methods: `ReadVarint()` (ULEB128), `ReadZigZagInt32/Int64()`, `ReadFieldHeader()` (delta-encoded field IDs), `ReadBinary()`, `ReadListHeader()`, `Skip()`. An inline stack (8 slots) tracks field ID context for nested structs. Boolean values are encoded in the type nibble of the field header (Compact Protocol optimization).

### `ThriftCompactWriter`

Mirror encoder with a growable `byte[]` buffer. Reusable via `Reset()`. Supports the same field-ID delta encoding and boolean-in-nibble compaction.

---

## Layer 3 — Metadata (`EngineeredWood.Parquet.Metadata`)

### Model

Immutable record types mirroring the Parquet Thrift spec:

- `FileMetaData` — version, schema elements, num_rows, row groups, key-value metadata, created_by
- `RowGroup` — list of `ColumnChunk`, total byte size, num_rows
- `ColumnChunk` → `ColumnMetaData` — physical type, encodings, codec, offsets, statistics
- `SchemaElement` — one node of the flat pre-order schema
- `LogicalType` — sealed record hierarchy (`StringType`, `DecimalType(Scale, Precision)`, `TimestampType(IsAdjustedToUtc, TimeUnit)`, etc.)

### `MetadataDecoder` / `MetadataEncoder`

Static methods that convert between the model and Thrift-encoded bytes. Parameterless `LogicalType` variants are cached as singletons.

---

## Layer 4 — Schema (`EngineeredWood.Parquet.Schema`)

`SchemaDescriptor` converts the flat `SchemaElement` list from the footer into a tree of `SchemaNode` objects, then collects leaf columns into `ColumnDescriptor` objects with computed definition and repetition levels.

- **Definition level** increments for each `OPTIONAL` ancestor.
- **Repetition level** increments for each `REPEATED` ancestor.
- `ColumnDescriptor` carries the full path, physical type, type length, max def/rep levels, and a pointer back to the `SchemaNode`.

---

## Layer 5 — Data: Reading (`EngineeredWood.Parquet.Data`)

### Read pipeline overview

```
ParquetFileReader.ReadRowGroupAsync()
  ├─ PrepareRowGroupAsync()          Gather column ranges + metadata
  ├─ ReadRangesAsync()               Parallel I/O (coalescing applied)
  ├─ Parallel.For → ColumnChunkReader.ReadColumn()   per column
  │    ├─ PageHeaderDecoder          Thrift → PageHeader (V1/V2/Dict)
  │    ├─ Decompressor               Codec dispatch
  │    ├─ LevelDecoder               Def/rep levels (V1: 4-byte prefix; V2: raw)
  │    ├─ Value decoder              PLAIN / RLE_DICTIONARY / DELTA_* / BYTE_STREAM_SPLIT
  │    └─ ArrowArrayBuilder          Build typed Arrow array, insert nulls from def levels
  └─ AssembleRecordBatch()
       └─ NestedAssembler            (if nested columns) flat leaves → Struct/List/Map arrays
```

### Encodings (read)

| Encoding | Decoder | Notes |
|---|---|---|
| PLAIN | `PlainDecoder` | Boolean bit-packed (LSB), fixed-width via `MemoryMarshal.Cast`, BYTE_ARRAY with 4-byte length prefix |
| PLAIN_DICTIONARY / RLE_DICTIONARY | `DictionaryDecoder` + `RleBitPackedDecoder` | Dictionary stored as typed arrays; indices RLE/bit-packed |
| RLE | `RleBitPackedDecoder` | For def/rep levels; LSB bit-packing, 8-value groups |
| BIT_PACKED (deprecated) | `RleBitPackedDecoder` | MSB-first packing, no length prefix |
| DELTA_BINARY_PACKED | `DeltaBinaryPackedDecoder` | 128-value blocks, 4 miniblocks; 8-byte aligned fast path for bit widths ≤ 56 |
| DELTA_LENGTH_BYTE_ARRAY | `DeltaLengthByteArrayDecoder` | Delta-encoded lengths + raw data |
| DELTA_BYTE_ARRAY | `DeltaByteArrayDecoder` | Delta-encoded prefix lengths + suffix lengths + suffix data |
| BYTE_STREAM_SPLIT | `ByteStreamSplitDecoder` | AVX2 SIMD with scalar fallback; interleaved byte streams for float/double |

### Arrow array construction

`ArrowArrayBuilder` dispatches by Arrow type. For nullable flat columns, it scatters non-null values right-to-left in-place to open gaps for nulls, avoiding a temporary buffer. Validity bitmaps are built from definition levels with SIMD (`Vector256`/`Vector128`) when available.

`ColumnBuildState` manages pooled and native buffers during page accumulation, supporting three BYTE_ARRAY output modes: default (32-bit offsets), view type (16-byte inline entries), and large offsets (64-bit).

### Nested types

`NestedAssembler` reconstructs Arrow `StructArray`, `ListArray`, and `MapArray` from flat leaf columns plus their def/rep level arrays.

- **Struct:** Child arrays grouped; validity bitmap derived from def levels at the struct's depth.
- **List/Map:** `BuildOffsetsAndBitmap()` uses rep/def level thresholds to determine list boundaries and null/empty markers. Phantom entries (null or empty list markers in the level arrays) are filtered before recursing into inner element assembly.
- **Deeply nested** (list-of-list, map-of-map, etc.): Recursive assembly with phantom filtering at each nesting depth.

### `ParquetFileReader` concurrency strategies

| Method | I/O | Decode |
|---|---|---|
| `ReadRowGroupAsync` (default) | Parallel batch via `ReadRangesAsync` | `Parallel.For` |
| `ReadRowGroupIncrementalAsync` | Sequential per column | Sequential |
| `ReadRowGroupIncrementalParallelAsync` | Parallel with bounded concurrency | `Parallel.For` |
| `ReadAllAsync` | Streams row groups as `IAsyncEnumerable<RecordBatch>` | Per-group strategy above |

### Read options

`ParquetReadOptions` controls Arrow type mapping for BYTE_ARRAY columns:
- `Default` — `StringType` / `BinaryType` (32-bit offsets)
- `ViewType` — `StringViewType` / `BinaryViewType` (inline ≤ 12 bytes)
- `LargeOffsets` — 64-bit offsets (removes 2 GB column limit)

---

## Layer 5 — Data: Writing (`EngineeredWood.Parquet.Data`)

### Write pipeline overview

```
ParquetFileWriter.WriteRowGroupAsync(RecordBatch)
  ├─ Auto-split if batch > RowGroupMaxRows
  ├─ Schema inference from first batch (ArrowToSchemaConverter)
  ├─ Parallel.For → ColumnChunkWriter.WriteColumn()   per leaf column
  │    ├─ NestedLevelWriter.Decompose()     (if nested) Arrow → flat leaves + def/rep levels
  │    ├─ DictionaryEncoder.TryEncode()     Analyze cardinality; open-addressing hash for ByteArray
  │    ├─ Dictionary path:
  │    │    ├─ PlainEncoder (dictionary page)
  │    │    └─ RleBitPackedEncoder (index pages)
  │    ├─ Non-dictionary path:
  │    │    └─ Type-aware V2 encoder or PLAIN V1
  │    ├─ Compressor                        Codec dispatch
  │    └─ StatisticsCollector               Min/max/null_count (O(unique) when dict available)
  ├─ Sequential column writes to ISequentialFile
  └─ CloseAsync() → MetadataEncoder → footer + footer length + trailing PAR1
```

### Encoding strategy

`EncodingStrategyResolver` selects the encoding per column based on physical type and page version:

| Physical type | V2 encoding | V1 encoding |
|---|---|---|
| Boolean | RLE (1-bit) | PLAIN |
| Int32, Int64 | DELTA_BINARY_PACKED | PLAIN |
| Float, Double | BYTE_STREAM_SPLIT | PLAIN |
| ByteArray | DELTA_LENGTH_BYTE_ARRAY or DELTA_BYTE_ARRAY | PLAIN |
| FixedLenByteArray | DELTA_BYTE_ARRAY | PLAIN |

Dictionary encoding is attempted first (unless disabled or Boolean). Cardinality threshold: 20% of non-null values. The `DictionaryEncoder` uses an open-addressing hash table with FNV-1a hashing for ByteArray/FLBA to avoid GC pressure from collision chains.

### Page format

- **V2 (default):** Def/rep levels are RLE-encoded and stored uncompressed; only the values section is compressed. This yields better compressibility and is required by some tools for nested columns.
- **V1 (legacy):** Levels + values are concatenated with 4-byte length prefixes, then the entire body is compressed together.

### Statistics

`StatisticsCollector` computes `min`, `max`, and `null_count` per column chunk. When dictionary encoding is used, statistics are computed from the dictionary entries alone (O(unique) instead of O(total)). Binary min/max are truncated to 64 bytes to prevent metadata bloat. NaN and negative-zero are handled correctly for floats.

### Write options

`ParquetWriteOptions` controls:
- `Compression` — default Snappy; per-column overrides via `ColumnCodecs`
- `DataPageVersion` — V1 or V2 (default V2)
- `DataPageSize` — target uncompressed page size (default 1 MB)
- `DictionaryPageSizeLimit` — abandon dictionary if it exceeds this (default 1 MB)
- `DictionaryEnabled` — analyze-before-write dictionary strategy (default true)
- `RowGroupMaxRows` / `RowGroupMaxBytes` — auto-splitting thresholds
- `ByteArrayEncoding` — DELTA_LENGTH_BYTE_ARRAY (default) or DELTA_BYTE_ARRAY; per-column overrides via `ColumnEncodings`
- `KeyValueMetadata` — arbitrary key-value pairs in the footer
- `CreatedBy` — application identifier

---

## Compression (`EngineeredWood.Parquet.Compression`)

`Compressor` and `Decompressor` dispatch to codec-specific implementations:

| Codec | Library | Notes |
|---|---|---|
| Uncompressed | — | Direct copy |
| Snappy | Snappier | Pure managed, span-based |
| Gzip | System.IO.Compression | `CompressionLevel.Fastest` for write |
| Brotli | System.IO.Compression | Span-based `TryCompress` / `TryDecompress` |
| LZ4_RAW | K4os.Compression.LZ4 | Block codec, span-based |
| LZ4 (legacy) | K4os.Compression.LZ4 | Hadoop framing (big-endian length headers) with frame and raw fallback |
| Zstd | ZstdSharp.Port | Pure managed; `[ThreadStatic]` compressor/decompressor for reuse |

LZO is not supported.

---

## Nested column handling

### Read path (`NestedAssembler`)

After all leaf columns are decoded, `NestedAssembler.Assemble()` walks the schema tree and reconstructs Arrow nested arrays. List and map offsets are derived from repetition levels; validity bitmaps from definition levels at each nesting depth. Phantom entries (level markers for null or empty containers) are filtered from element arrays before recursing into inner structure.

### Write path (`NestedLevelWriter`)

`NestedLevelWriter.Decompose()` walks an Arrow nested array and produces flat `LeafColumn` results — each containing the dense non-null value array plus def/rep level arrays. Struct children inherit parent nullability; list/map elements get incremented rep levels.

---

## Arrow ↔ Parquet type mapping

### Read direction (`ArrowSchemaConverter`)

Three-stage fallthrough: LogicalType → ConvertedType → PhysicalType. Handles decimal precision routing (INT32→Decimal32, INT64→Decimal64, FLBA→Decimal128/256), timestamp units, and the three BYTE_ARRAY output modes.

### Write direction (`ArrowToSchemaConverter`)

Converts Arrow `Schema` fields into a flat list of `SchemaElement` in pre-order traversal. Nested types (struct, list, map) produce the standard Parquet group annotations with correct `NumChildren` and repetition types.

---

## Decimal handling

Parquet stores decimals in big-endian; Arrow uses little-endian. The read path reverses FLBA bytes and sign-extends to the target Arrow width (Decimal32/64/128/256). The write path reverses Arrow bytes back to big-endian before encoding.

Pattern match ordering matters: `Decimal32/64/128/256Type` all inherit from `FixedSizeBinaryType`, so decimal cases must come before `FixedSizeBinaryType` in switch expressions.

---

## Performance techniques

- **Ref struct decoders** — `ThriftCompactReader`, `RleBitPackedDecoder`, `DeltaBinaryPackedDecoder` operate over spans with no heap allocation.
- **SIMD** — `ByteStreamSplitDecoder` has AVX2 fast paths for 4-byte and 8-byte unsplit. `ArrowArrayBuilder` uses `Vector256`/`Vector128` for validity bitmap construction.
- **Thread-static buffers** — `ColumnChunkWriter` reuses compression and value encoding buffers across pages via `[ThreadStatic]` fields.
- **ArrayPool / NativeBuffer** — Pooled managed arrays for levels; native memory for Arrow buffer construction (transferred to Arrow without copying).
- **In-place null scatter** — `ArrowArrayBuilder` scatters non-null values right-to-left to open gaps, avoiding a temporary copy.
- **Dictionary stats shortcut** — When dictionary encoding succeeds, statistics are computed over dictionary entries (O(unique)) rather than all values.
- **Open-addressing hash** — `DictionaryEncoder.BytesHashTable` uses FNV-1a + linear probing for ByteArray deduplication, avoiding managed `Dictionary<>` overhead.

---

## Test infrastructure

- `TestData.cs` locates the `parquet-testing/data/` submodule by walking up from `AppContext.BaseDirectory`.
- Sweep tests iterate all sample files, skip encrypted/malformed, collect failures, assert none.
- Cross-validation: a companion `EngineeredWood.Compatibility` project downloads 92 files from fastparquet, parquet-dotnet, parquet-tools, and HuggingFace and validates all row groups.
- Decoder/encoder tests use both unit-level checks (known byte sequences) and round-trip verification against ParquetSharp.

## Benchmarks

`EngineeredWood.Benchmarks` contains BenchmarkDotNet suites comparing against ParquetSharp and Parquet.Net for metadata parsing, row group reads, row group writes, and individual encodings.
