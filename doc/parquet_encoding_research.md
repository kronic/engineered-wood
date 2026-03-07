# Parquet Write-Path Decision-Making: Java, C++, Rust, Go, and DuckDB Implementations

## Executive Summary

This report examines how five Parquet implementations — **Java** (`apache/parquet-java`, the reference implementation), **C++** (`apache/arrow`), **Rust** (`apache/arrow-rs`), **Go** (`apache/arrow-go`), and **DuckDB** (`duckdb/duckdb`) — decide how to encode column data and size row groups during writes. The first four are general-purpose Parquet libraries, while DuckDB is an analytical database with an embedded Parquet writer optimized for its COPY TO workflow. All five share a common architecture rooted in the Parquet specification, but differ meaningfully in their defaults, the sophistication of their encoding selection, the adaptiveness of their row group sizing, and the degree of automatic vs. manual control they offer.

---

## 1. Encoding Selection

### 1.1 Overview of Available Encodings

All five implementations support the core Parquet encodings:

| Encoding | Purpose |
|---|---|
| **PLAIN** | Raw values; universal fallback |
| **PLAIN_DICTIONARY / RLE_DICTIONARY** | Dictionary-encodes values; indices stored with RLE |
| **RLE** | Run-length encoding (used for Boolean values, definition/repetition levels) |
| **DELTA_BINARY_PACKED** | Delta + bit-packing for integers |
| **DELTA_LENGTH_BYTE_ARRAY** | Delta-encoded lengths + byte arrays |
| **DELTA_BYTE_ARRAY** | Incremental/prefix-encoded byte arrays |
| **BYTE_STREAM_SPLIT** | Column-byte interleaving for floating-point |

### 1.2 Java Encoding Selection (`apache/parquet-java`) — Reference Implementation

**Source**: `DefaultV1ValuesWriterFactory.java`, `DefaultV2ValuesWriterFactory.java`, `DefaultValuesWriterFactory.java`, `FallbackValuesWriter.java`, `DictionaryValuesWriter.java`

As the **reference implementation**, Java has the most mature and nuanced encoding selection. It uses a **ValuesWriterFactory** pattern with separate factories for Parquet V1 and V2 writer versions, and a `FallbackValuesWriter` wrapper that adds adaptive dictionary-to-fallback transitions.

**V1 Writer (`DefaultV1ValuesWriterFactory`) — Default:**

| Type | Dictionary Encoding | Fallback Encoding |
|---|---|---|
| **BOOLEAN** | ❌ (not supported) | `BooleanPlainValuesWriter` (PLAIN) |
| **INT32** | ✅ `PLAIN_DICTIONARY` | `PlainValuesWriter` or `ByteStreamSplit` (if enabled) |
| **INT64** | ✅ `PLAIN_DICTIONARY` | `PlainValuesWriter` or `ByteStreamSplit` (if enabled) |
| **INT96** | ✅ `PLAIN_DICTIONARY` | `FixedLenByteArrayPlainValuesWriter` |
| **FLOAT** | ✅ `PLAIN_DICTIONARY` | `PlainValuesWriter` or `ByteStreamSplit` (if enabled) |
| **DOUBLE** | ✅ `PLAIN_DICTIONARY` | `PlainValuesWriter` or `ByteStreamSplit` (if enabled) |
| **BINARY** | ✅ `PLAIN_DICTIONARY` | `PlainValuesWriter` |
| **FIXED_LEN_BYTE_ARRAY** | ❌ (not in V1) | `PlainValuesWriter` or `ByteStreamSplit` (if enabled) |

**V2 Writer (`DefaultV2ValuesWriterFactory`):**

| Type | Dictionary Encoding | Fallback Encoding |
|---|---|---|
| **BOOLEAN** | ❌ (not supported) | `RunLengthBitPackingHybridValuesWriter` (**RLE**) |
| **INT32** | ✅ `RLE_DICTIONARY` | `DeltaBinaryPackingValuesWriterForInteger` or `ByteStreamSplit` |
| **INT64** | ✅ `RLE_DICTIONARY` | `DeltaBinaryPackingValuesWriterForLong` or `ByteStreamSplit` |
| **INT96** | ✅ `RLE_DICTIONARY` | `FixedLenByteArrayPlainValuesWriter` |
| **FLOAT** | ✅ `RLE_DICTIONARY` | `PlainValuesWriter` or `ByteStreamSplit` |
| **DOUBLE** | ✅ `RLE_DICTIONARY` | `PlainValuesWriter` or `ByteStreamSplit` |
| **BINARY** | ✅ `RLE_DICTIONARY` | `DeltaByteArrayWriter` (**DELTA_BYTE_ARRAY**) |
| **FIXED_LEN_BYTE_ARRAY** | ✅ `RLE_DICTIONARY` | `DeltaByteArrayWriter` or `ByteStreamSplit` |

**Key design**: Each type gets a `FallbackValuesWriter` that wraps a dictionary writer and a fallback writer. The fallback writer is **type-specific and version-specific** — this is the encoding that the Rust implementation mirrors with its `fallback_encoding()` function.

**Dictionary fallback — two-phase adaptive logic**:

Java's `FallbackValuesWriter` implements a uniquely sophisticated two-phase fallback:

1. **Size-based fallback** (`DictionaryValuesWriter.shouldFallBack()`): Checked after **every value** write. Triggers when:
   - `dictionaryByteSize > maxDictionaryByteSize` (dictionary page size limit, default 1 MB), OR
   - `getDictionarySize() > MAX_DICTIONARY_ENTRIES` (Integer.MAX_VALUE - 1, ~2.1 billion)
   
   When triggered, `fallBackAllValuesTo()` re-encodes all buffered values using the fallback writer.

2. **Compression-effectiveness fallback** (`isCompressionSatisfying()`): Checked at the **end of the first page**. If the encoded+dictionary size is **not smaller** than the raw data size (`encodedSize + dictionaryByteSize >= rawSize`), dictionary encoding is abandoned for all subsequent pages. This is a unique heuristic that no other implementation has — it detects when dictionary encoding is actually *hurting* rather than helping.

**BYTE_STREAM_SPLIT support**: Java uniquely offers per-column `ByteStreamSplit` encoding as an alternative fallback for numeric types (INT32, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY). When enabled via `isByteStreamSplitEnabled(column)`, it replaces the default fallback (PLAIN or DELTA) with `ByteStreamSplitValuesWriter`. This is particularly effective for floating-point data combined with compression.

### 1.3 C++ Encoding Selection (`apache/arrow`)

**Source**: `cpp/src/parquet/column_writer.cc`, `properties.h`

The C++ implementation uses a **simple, conservative** approach to encoding selection:

```
Default encoding = Encoding::UNKNOWN (sentinel value)
```

When a column writer is created (`ColumnWriter::Make`), the logic is:

1. If the user has set a specific encoding via `WriterProperties`, use that.
2. If encoding is `UNKNOWN` (the default):
   - **Boolean** columns with Parquet version > 1.0 **and** DataPageV2 → `RLE`
   - **Everything else** → `PLAIN`
3. If dictionary encoding is enabled (the default for all non-Boolean types), the encoding is overridden to the dictionary index encoding (`RLE_DICTIONARY` for Parquet 2.x, `PLAIN_DICTIONARY` for 1.0).

**Key insight**: The C++ implementation does **not** use type-aware encoding defaults. It does not automatically select `DELTA_BINARY_PACKED` for integers or `DELTA_BYTE_ARRAY` for byte arrays. The non-dictionary fallback encoding is always `PLAIN` unless the user explicitly specifies otherwise. The only exception is Boolean columns with V2 data pages, which use `RLE`.

**Dictionary encoding behavior**:
- Enabled by default for all types except `BOOLEAN`.
- Dictionary indices are encoded with `RLE_DICTIONARY` (Parquet 2.x) or `PLAIN_DICTIONARY` (1.0).
- The dictionary page itself is encoded with `PLAIN` (2.x) or `PLAIN_DICTIONARY` (1.0).
- When the dictionary's encoded size reaches the `dictionary_pagesize_limit` (default: 1 MB), the writer **falls back to PLAIN** encoding — not to the user-configured fallback encoding. This is hardcoded:
  ```cpp
  // Only PLAIN encoding is supported for fallback
  current_encoder_ = MakeEncoder(ParquetType::type_num, Encoding::PLAIN, false, ...);
  ```
- The user-specified encoding via `Builder::encoding()` is described as "used when we don't utilise dictionary encoding" but in practice, dictionary fallback always goes to PLAIN. The user-specified encoding is only used when dictionary encoding is *disabled* from the start.

**Content-Defined Chunking (CDC)**: The C++ implementation has an **experimental** feature for smarter data page boundary selection. Instead of splitting pages at fixed size thresholds, CDC uses a rolling hash (Gear hash) to create content-aware page boundaries. This can improve compression ratios because page boundaries align with data patterns rather than arbitrary byte offsets.

CDC options include:
- `min_chunk_size`: 256 KB default
- `max_chunk_size`: 1 MB default (same as default page size)
- `sensitivity`: controls how aggressively boundaries cluster around the average size (range: roughly -3 to 3, default 0)

### 1.4 Rust Encoding Selection (`apache/arrow-rs`)

**Source**: `parquet/src/column/writer/mod.rs`, `parquet/src/column/writer/encoder.rs`, `parquet/src/file/properties.rs`

The Rust implementation has the **most sophisticated** encoding selection, implementing a `fallback_encoding()` function that mirrors the parquet-mr (Java) approach:

```rust
fn fallback_encoding(kind: Type, props: &WriterProperties) -> Encoding {
    match (kind, props.writer_version()) {
        (Type::BOOLEAN, WriterVersion::PARQUET_2_0)              => Encoding::RLE,
        (Type::INT32, WriterVersion::PARQUET_2_0)                => Encoding::DELTA_BINARY_PACKED,
        (Type::INT64, WriterVersion::PARQUET_2_0)                => Encoding::DELTA_BINARY_PACKED,
        (Type::BYTE_ARRAY, WriterVersion::PARQUET_2_0)           => Encoding::DELTA_BYTE_ARRAY,
        (Type::FIXED_LEN_BYTE_ARRAY, WriterVersion::PARQUET_2_0) => Encoding::DELTA_BYTE_ARRAY,
        _ => Encoding::PLAIN,
    }
}
```

This means that with `PARQUET_2_0` writer version:
- **INT32/INT64** → `DELTA_BINARY_PACKED` (efficient for sorted/sequential integers)
- **BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY** → `DELTA_BYTE_ARRAY` (prefix-encoded; good for sorted strings)
- **BOOLEAN** → `RLE`
- **FLOAT/DOUBLE/INT96** → `PLAIN`

With `PARQUET_1_0`, everything falls back to `PLAIN`.

**Dictionary support rules**:
```rust
fn has_dictionary_support(kind: Type, props: &WriterProperties) -> bool {
    match (kind, props.writer_version()) {
        (Type::BOOLEAN, _) => false,
        (Type::FIXED_LEN_BYTE_ARRAY, WriterVersion::PARQUET_1_0) => false,
        (Type::FIXED_LEN_BYTE_ARRAY, WriterVersion::PARQUET_2_0) => true,
        _ => true,
    }
}
```

**Dictionary fallback behavior**: Unlike C++, Rust's dictionary fallback transitions to the **type-appropriate fallback encoding** (the one selected by `fallback_encoding()`). When the dictionary's encoded size reaches `dictionary_page_size_limit`, the encoder switches:
- Dictionary data pages already buffered get flushed with dictionary indices
- The dictionary page is written
- Subsequent data is encoded with the fallback encoder (which could be `DELTA_BINARY_PACKED`, etc.)

**Encoder initialization** (`ColumnValueEncoderImpl::try_new`):
```rust
// Set either main encoder or fallback encoder.
let encoder = get_encoder(
    props.encoding(descr.path())
        .unwrap_or_else(|| fallback_encoding(T::get_physical_type(), props)),
    descr,
)?;
```

If the user has set an encoding per-column, it's used; otherwise the type-appropriate default is computed. The dictionary encoder is created separately and used preferentially when available.

### 1.5 Go Encoding Selection (`apache/arrow-go`)

**Source**: `parquet/writer_properties.go`, `parquet/file/column_writer.go`, `parquet/file/column_writer_types.gen.go.tmpl`

The Go implementation uses a **template-generated** approach (Go code generation via `.tmpl` files) for type-specific column writers.

**Default encoding**: `Encodings.Plain` (explicitly set in `DefaultColumnProperties()`).

The column writer creation logic (`NewColumnChunkWriter`) determines encoding:
```go
useDict := props.DictionaryEnabledFor(descr.Path()) &&
           descr.PhysicalType() != parquet.Types.Boolean &&
           descr.PhysicalType() != parquet.Types.Int96
enc := props.EncodingFor(descr.Path())
if useDict {
    enc = props.DictionaryIndexEncoding()
}
```

- Dictionary is enabled by default for all types **except Boolean and Int96**.
- When dictionary is enabled, the encoding is set to the dictionary index encoding (`RLE_DICTIONARY` for Parquet 2.x, `PLAIN_DICTIONARY` for 1.0).
- The user-configured encoding via `WithEncoding()` / `WithEncodingFor()` serves as the fallback when dictionary encoding is disabled or falls back.
- **Unlike Rust**, Go does **not** have type-aware fallback encoding. It always falls back to `PLAIN` (or whatever the user specified).
- Dictionary/non-dictionary encoding cannot be set to `PLAIN_DICTIONARY` or `RLE_DICTIONARY` — this panics.

**Dictionary fallback behavior** (from template):
```go
func (w *{{.Name}}ColumnChunkWriter) FallbackToPlain() {
    if w.currentEncoder.Encoding() == parquet.Encodings.PlainDict {
        w.WriteDictionaryPage()
        w.FlushBufferedDataPages()
        w.fallbackToNonDict = true
        w.currentEncoder = encoding.{{.Name}}EncoderTraits.Encoder(
            format.Encoding(parquet.Encodings.Plain), false, w.descr, w.mem)
        w.encoding = parquet.Encodings.Plain
    }
}
```

Note: The function is named `FallbackToPlain()` and is hardcoded to switch to `Encodings.Plain`, regardless of the user's configured fallback encoding. This is the same behavior as C++.

### 1.6 DuckDB Encoding Selection (`duckdb/duckdb`)

**Source**: `extension/parquet/include/writer/templated_column_writer.hpp`, `extension/parquet/writer/primitive_column_writer.cpp`, `extension/parquet/include/writer/boolean_column_writer.hpp`

DuckDB takes a fundamentally different approach from the other four implementations. It uses a **two-phase analyze-then-write architecture** where encoding decisions are made *before* any data is written, based on a full analysis pass over the row group data. This contrasts with all other implementations, which select encodings at writer initialization time and may switch during writing (dictionary fallback).

**Architecture**: DuckDB's column writers follow a `Analyze() → FinalizeAnalyze() → Prepare() → BeginWrite() → Write() → FinalizeWrite()` pipeline. The `Analyze()` phase scans all values in the row group, building a dictionary of distinct values. `FinalizeAnalyze()` then makes the encoding decision based on what was observed.

**Encoding decision logic** (`StandardColumnWriter::FinalizeAnalyze()`):

First, dictionary encoding is attempted. During `Analyze()`, all values are inserted into a `PrimitiveDictionary`. Dictionary encoding is abandoned if either:
- The dictionary is empty (all nulls), OR
- The dictionary is "full" — exceeds its size limit

The **dictionary size limit** defaults to `row_group.num_rows / 5` (i.e., 20% of row group size), or the user-configured `dictionary_size_limit`. There is also a `string_dictionary_page_size_limit` (default: 1 GB) for byte-based limits on string dictionaries.

When dictionary encoding is not viable, DuckDB selects encoding based on **Parquet version and physical type**:

**Parquet V1:**
| Type | Encoding |
|---|---|
| All types | `PLAIN` |

**Parquet V2:**
| Type | Encoding |
|---|---|
| **INT32** | `DELTA_BINARY_PACKED` |
| **INT64** | `DELTA_BINARY_PACKED` |
| **BYTE_ARRAY** | `DELTA_LENGTH_BYTE_ARRAY` |
| **FLOAT** | `BYTE_STREAM_SPLIT` |
| **DOUBLE** | `BYTE_STREAM_SPLIT` |
| **BOOLEAN** | `PLAIN` (bit-packed, not RLE) |
| **All others** | `PLAIN` |

**Notable differences from other implementations:**

1. **BYTE_STREAM_SPLIT is automatic for floats/doubles** in V2 mode. DuckDB is the only implementation that automatically uses BYTE_STREAM_SPLIT without requiring explicit per-column configuration. This encoding is particularly effective for floating-point data combined with compression (e.g., ZSTD).

2. **DELTA_LENGTH_BYTE_ARRAY instead of DELTA_BYTE_ARRAY**: For BYTE_ARRAY types, DuckDB uses `DELTA_LENGTH_BYTE_ARRAY` (delta-encodes string lengths, then concatenates raw bytes), while Rust and Java use `DELTA_BYTE_ARRAY` (incremental/prefix encoding). DLBA is simpler and works better for unsorted strings; DBA is more effective for sorted/prefix-heavy strings.

3. **Boolean uses direct bit-packing (PLAIN), not RLE**: Unlike Java, Rust, and C++ (V2), DuckDB's boolean column writer writes booleans as packed bits and reports `Encoding::PLAIN`. There is no RLE encoding for booleans.

4. **Analyze-before-write eliminates mid-stream fallback**: Because the entire row group is analyzed before writing begins, DuckDB never needs to switch encodings mid-stream. There is no equivalent of the `FallbackToPlain()` mechanism seen in C++/Go, or Java's two-phase adaptive fallback. The dictionary either fits or it doesn't, and the decision is final before any data is serialized.

**Dictionary encoding specifics**:
- Version V1: `PLAIN_DICTIONARY` index encoding
- Version V2: `RLE_DICTIONARY` index encoding
- Dictionary pages are always encoded with `PLAIN`
- Dictionary key bit-width is computed as `RleBpDecoder::ComputeBitWidth(dictionarySize)`
- When dictionary encoding is used, row sizes are estimated as `(key_bit_width + 7) / 8` bytes per value

**Bloom filter support**: When bloom filters are enabled, they are created during `FlushDictionary()` with the user-configured false positive ratio. The bloom filter is populated from the dictionary values, not individual rows, making it efficient.

### 1.7 Encoding Selection Comparison

| Aspect | Java | C++ | Rust | Go | DuckDB |
|---|---|---|---|---|---|
| **Default non-dict encoding** | Type-aware (both V1 & V2) | PLAIN (always) | Type-aware (V2 only) | PLAIN (always) | Type-aware (V2 only) |
| **Dictionary default** | Enabled (except Bool; FLBA only in V2) | Enabled (except Boolean) | Enabled (except Bool; FLBA only in V2) | Enabled (except Bool, Int96) | Enabled (all types via analyze phase) |
| **Dict fallback encoding** | **Type-appropriate** (per factory) | Always PLAIN (hardcoded) | Type-appropriate fallback | Always PLAIN (hardcoded) | Type-appropriate (decided pre-write) |
| **Compression-effectiveness check** | **Yes** (first page) | No | No | No | No (full analysis replaces this) |
| **DELTA_BINARY_PACKED auto** | Yes (INT32/INT64 in V2) | No | Yes (INT32/INT64 in V2) | No | Yes (INT32/INT64 in V2) |
| **DELTA_BYTE_ARRAY auto** | Yes (BINARY/FLBA in V2) | No | Yes (BYTE_ARRAY/FLBA in V2) | No | No (uses DELTA_LENGTH_BYTE_ARRAY) |
| **BYTE_STREAM_SPLIT auto** | Per-column toggle | No (user must set encoding) | No (user must set encoding) | No | **Yes** (FLOAT/DOUBLE in V2, automatic) |
| **RLE for Boolean auto** | Yes (V2) | Only with V2 data pages | Yes (in V2) | No | No (bit-packed PLAIN) |
| **Content-Defined Chunking** | No | Yes (experimental) | No | No | No |
| **Analyze-before-write** | No | No | No | No | **Yes** (full row group scan) |
| **Per-column encoding override** | Via dictionary enable/disable | Yes | Yes | Yes | No (encoding is auto-selected) |

---

## 2. Row Group Sizing

### 2.1 Java Row Group Sizing (`apache/parquet-java`)

**Default row group size**: `134,217,728` bytes (128 MB) — `ParquetWriter.DEFAULT_BLOCK_SIZE`
**Default row count limit**: `Integer.MAX_VALUE` (effectively unlimited) — `DEFAULT_ROW_GROUP_ROW_COUNT_LIMIT`

Java has the **most sophisticated adaptive row group sizing** of all four implementations, using a **byte-based** primary threshold (not row-count) with intelligent memory sampling.

**Adaptive memory checking** (`InternalParquetRecordWriter.checkBlockSizeReached()`):

Rather than checking memory usage on every row (expensive), Java uses an adaptive sampling strategy:

1. **First check**: At `MIN_RECORD_COUNT_FOR_CHECK` rows (default: 100)
2. **Estimate average record size**: `recordSize = memSize / recordCount`
3. **Flush trigger**: `memSize > (nextRowGroupSize - 2 * recordSize)` — flushes when within ~2 records of the limit, preferring to be slightly under rather than over
4. **Next check prediction**: Schedules the next memory check at approximately the halfway point between current position and estimated fill point: `(recordCount + nextRowGroupSize / recordSize) / 2`
5. **Check frequency bounds**: Between `MIN_RECORD_COUNT_FOR_CHECK` (100) and `MAX_RECORD_COUNT_FOR_CHECK` (10,000)

Additionally, after each flush, `nextRowGroupSize` is updated to `min(parquetFileWriter.getNextRowGroupSize(), rowGroupSizeThreshold)`, which accounts for HDFS block alignment — if the file writer knows a smaller row group would align better with the underlying storage block boundaries, it uses that smaller size.

**Row count threshold**: Also flushes if `recordCount >= rowGroupRecordCountThreshold`, but the default is `Integer.MAX_VALUE`, making this effectively inactive unless explicitly configured.

### 2.2 C++ Row Group Sizing

**Default max row group length**: `1,048,576` rows (1M) — `DEFAULT_MAX_ROW_GROUP_LENGTH`

The C++ implementation controls row groups **by row count only** at the `WriterProperties` level. There is no built-in byte-based row group limit in the core writer. The higher-level Arrow-to-Parquet writer (in `arrow/parquet`) may add additional logic.

User control:
- `WriterProperties::Builder::max_row_group_length(int64_t)` — sets max rows per row group
- Row groups are created explicitly via `AppendRowGroup()` or `AppendBufferedRowGroup()` on the file writer

### 2.3 Rust Row Group Sizing

**Default max row group row count**: `1,048,576` rows (1M) — `DEFAULT_MAX_ROW_GROUP_ROW_COUNT`
**Default max row group bytes**: `None` (no byte limit by default)

Rust has the **most automatic** row group management. The `ArrowWriter` (high-level API) automatically flushes row groups based on **both** criteria:

```rust
let should_flush = self
    .max_row_group_row_count
    .is_some_and(|max| in_progress.buffered_rows >= max)
    || self
        .max_row_group_bytes
        .is_some_and(|max| in_progress.get_estimated_total_bytes() >= max);
```

When writing Arrow `RecordBatch`es, the writer will:
1. Split batches across row group boundaries if needed
2. Auto-flush when row count **or** estimated byte count exceeds the configured limits

User control:
- `WriterPropertiesBuilder::set_max_row_group_row_count(Option<usize>)` — row limit
- `WriterPropertiesBuilder::set_max_row_group_bytes(Option<usize>)` — byte limit *(unique to Rust)*
- When both are set, whichever limit is reached first triggers a flush

### 2.4 Go Row Group Sizing

**Default max row group length**: `67,108,864` rows (64M) — `DefaultMaxRowGroupLen`

This is **dramatically larger** than C++ and Rust (64× larger).

Go's row group management depends on the API level:

**Low-level API** (`file.Writer`): Row groups are **entirely manual**. The user calls `AppendRowGroup()` or `AppendBufferedRowGroup()` to create row groups. There is no automatic flushing.

**High-level API** (`pqarrow.FileWriter`): Provides two methods with different row group behavior:
- `Write(rec)`: Creates **at least one row group per record**. Splits records exceeding `MaxRowGroupLength` into multiple row groups.
- `WriteBuffered(rec)`: Appends to the current row group, splitting only when `MaxRowGroupLength` would be exceeded. More memory-efficient for many small records.
- `WriteTable(tbl, chunkSize)`: Splits into row groups of `chunkSize` rows, capped at `MaxRowGroupLength`.

Go does **not** have automatic byte-based row group flushing. Users who want byte-based control must check `RowGroupTotalBytesWritten()` or `RowGroupTotalCompressedBytes()` manually and call `NewBufferedRowGroup()`.

### 2.5 DuckDB Row Group Sizing

**Default max row group size**: `122,880` rows — inherited from DuckDB's internal `Storage::ROW_GROUP_SIZE` constant
**Default max row group bytes**: None (unlimited by default via `max uint64_t`)

DuckDB's row group sizing is driven by its **COPY TO** execution pipeline rather than the Parquet writer itself. The Parquet writer receives pre-buffered `ColumnDataCollection` chunks and writes them as row groups. The decision of when to flush is made in the execution layer.

**Key behavior**:
- Data is buffered in a `ColumnDataCollection` in `ParquetWriteLocalState`
- When the buffer reaches the configured `row_group_size` (row count), it is flushed as a row group
- The `row_group_size_bytes` parameter can optionally trigger byte-based flushing, but this requires `preserve_insertion_order` to be disabled (DuckDB throws an error if both are set)
- Row groups are flushed via `PrepareRowGroup()` → `FlushRowGroup()`, with an optional `LogFlushingRowGroup()` for debugging

**User control via COPY TO options**:
- `ROW_GROUP_SIZE` (alias `CHUNK_SIZE`): Number of rows per row group (default: 122,880)
- `ROW_GROUP_SIZE_BYTES`: Byte-based row group limit (accepts human-readable sizes like `'256MB'` via `DBConfig::ParseMemoryLimit`; default: unlimited)

The 122,880 default is notably smaller than any other implementation and is tuned for DuckDB's internal vector/chunk size (2048 rows × 60 = 122,880). This makes row groups align with DuckDB's execution engine rather than conforming to Parquet conventions.

### 2.6 Row Group Sizing Comparison

| Aspect | Java | C++ | Rust | Go | DuckDB |
|---|---|---|---|---|---|
| **Default max rows** | MAX_INT (unlimited) | 1M | 1M | **64M** | **122,880** |
| **Default max bytes** | **128 MB** | None | None | None | None (unlimited) |
| **Byte-based limit** | **Yes** (primary mechanism) | No | **Yes** (`max_row_group_bytes`) | No (manual check only) | **Yes** (`ROW_GROUP_SIZE_BYTES`) |
| **Row-count limit** | Yes (default unlimited) | Yes (primary) | Yes (1M default) | Yes (64M default) | Yes (122,880 default) |
| **Adaptive memory sampling** | **Yes** (variable check frequency) | No | No | No | No |
| **HDFS block alignment** | **Yes** (`getNextRowGroupSize`) | No | No | No | No |
| **Auto-flush in high-level API** | **Yes** (byte + row + adaptive) | Partial | Yes (row + byte) | Row-based only | Yes (row + optional byte) |
| **Manual row group control** | Yes | Yes | Yes | Yes | No (driven by COPY TO pipeline) |

---

## 3. Data Page Sizing

### 3.1 Comparison

| Setting | Java | C++ | Rust | Go | DuckDB |
|---|---|---|---|---|---|
| **Default data page size** | 1 MB | 1 MB | 1 MB | 1 MB | **100 MB** |
| **Default max rows per page** | 20,000 | 20,000 | 20,000 | N/A (not configurable separately) | N/A (not configurable) |
| **Page flush trigger** | `getBufferedSize() >= pageSize` (uses rawDataByteSize for dict columns) | `estimated_encoded_size >= pagesize` OR `buffered_rows >= max_rows_per_page` | `estimated_data_page_size >= page_size_limit` OR `num_buffered_rows >= row_count_limit` | `estimated_encoded_size >= page_size` | `estimated_page_size >= MAX_UNCOMPRESSED_PAGE_SIZE` (100 MB) |
| **Per-column page size** | No | No | **Yes** (`column_data_page_size_limit`) | No | No |
| **Content-defined chunking** | No | **Yes** (experimental) | No | No | No |
| **Page write checksum (CRC)** | **Yes** (default enabled) | Yes | No | No | No |

Java uniquely uses `rawDataByteSize` (unencoded size) for page flush decisions when using dictionary encoding. This prevents dictionary-encoded pages from growing excessively large in memory, since the encoded size may appear small while the raw data is large.

DuckDB's 100 MB page size limit is **100× larger** than the other implementations' 1 MB default. This is a hardcoded constant (`MAX_UNCOMPRESSED_PAGE_SIZE = 104857600`) and is not user-configurable. DuckDB's design favors fewer, larger pages — since the entire row group is available during the `Prepare()` phase, page boundaries are determined by size estimation before any encoding occurs. The Parquet spec allows pages up to 2 GB; DuckDB's comment notes it chose a "more conservative" 100 MB limit.

---

## 4. Dictionary Page Sizing

| Setting | Java | C++ | Rust | Go | DuckDB |
|---|---|---|---|---|---|
| **Default dictionary page size limit** | 1 MB | 1 MB | 1 MB | 1 MB | **1 GB** (string dict page); **rows/5** (entry count) |
| **Per-column dictionary page size** | No | No | **Yes** (`column_dictionary_page_size_limit`) | No | No |
| **Fallback trigger** | `dictionaryByteSize > limit` OR `dictSize > MAX_INT-1` | `dict_encoded_size >= limit` | `dict_encoded_size >= limit` OR `estimated_data_page_size >= data_page_size_limit` | `DictEncodedSize >= limit` | Dictionary full during `Analyze()` (pre-write) |
| **Compression-effectiveness check** | **Yes** (first page: `encodedSize + dictSize < rawSize`) | No | No | No | No (pre-write analysis instead) |
| **Fallback encoding** | **Type-appropriate** (per factory) | PLAIN (hardcoded) | Type-appropriate | PLAIN (hardcoded) | Type-appropriate (decided pre-write) |
| **Fallback checked** | **Every value write** | Every write batch | After each write batch | Every write batch | **Once** (after full row group analysis) |

Java's dictionary fallback is the most granular — it checks `shouldFallBack()` after every single value write, and uniquely checks dictionary encoding effectiveness after the first complete page. Rust has an additional fallback trigger (data page size overflow) that other implementations lack.

DuckDB's approach is architecturally distinct: the dictionary viability check happens **once**, during the `Analyze()` phase before any data is encoded. The `PrimitiveDictionary` tracks both entry count (limit: `num_rows / 5` or user-configured) and byte size (limit: `string_dictionary_page_size_limit`, default 1 GB). If the dictionary exceeds either limit during analysis, it is abandoned entirely and a type-appropriate encoding is selected. There is no mid-stream fallback. The dictionary page itself has a hard cap of 1 GB (`MAX_UNCOMPRESSED_DICT_PAGE_SIZE`).

---

## 5. User Configuration Surface

### 5.1 WriterProperties Comparison

| Property | Java | C++ | Rust | Go | DuckDB |
|---|---|---|---|---|---|
| **Parquet version** | PARQUET_1_0, PARQUET_2_0 | V1.0, V2.4, V2.6 | PARQUET_1_0, PARQUET_2_0 | V1_0, V2_4, V2_6, V2_LATEST | V1, V2 |
| **Data page version** | V1, V2 | V1, V2 | V1, V2 | V1, V2 | N/A (not configurable) |
| **Global encoding** | Via writer version | ✅ | ✅ | ✅ | Via `PARQUET_VERSION` only |
| **Per-column encoding** | Via dict + BSS toggles | ✅ | ✅ | ✅ | ❌ (auto-selected per-type) |
| **Global dictionary enable/disable** | ✅ | ✅ | ✅ | ✅ | Via `DICTIONARY_SIZE_LIMIT` (0 to disable) |
| **Per-column dictionary enable/disable** | ✅ | ✅ | ✅ | ✅ | ❌ |
| **Per-column BYTE_STREAM_SPLIT** | ✅ (dedicated toggle) | Via encoding override | Via encoding override | Via encoding override | ❌ (automatic for FLOAT/DOUBLE in V2) |
| **Global compression** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Per-column compression** | ✅ | ✅ | ✅ | ✅ | ❌ |
| **Compression level** | ✅ | ✅ | ✅ | ✅ | ✅ (ZSTD only) |
| **Data page size limit** | ✅ | ✅ | ✅ (+ per-column) | ✅ | ❌ (hardcoded 100 MB) |
| **Dictionary page size limit** | ✅ | ✅ | ✅ (+ per-column) | ✅ | ✅ (`STRING_DICTIONARY_PAGE_SIZE_LIMIT`, `DICTIONARY_SIZE_LIMIT`) |
| **Max row group bytes** | ✅ (128 MB default) | ❌ | ✅ | ❌ | ✅ (`ROW_GROUP_SIZE_BYTES`) |
| **Max row group rows** | ✅ (MAX_INT default) | ✅ (1M default) | ✅ (1M default) | ✅ (64M default) | ✅ (122,880 default) |
| **Max rows per data page** | ✅ (20K) | ✅ (20K) | ✅ (20K) | ❌ | ❌ |
| **Write batch size** | ❌ | ✅ | ✅ | ✅ | ❌ |
| **Statistics enable/disable** | ✅ | ✅ | ✅ | ✅ | ✅ (always enabled) |
| **Max statistics size** | ✅ | ✅ | ✅ | ✅ | ❌ |
| **Page index (column index/offset index)** | ✅ | ✅ | ✅ | ✅ | ❌ |
| **Bloom filter** | ✅ (per-column + adaptive) | ✅ (per-column) | ✅ (per-column) | ✅ (per-column + adaptive) | ✅ (global toggle + FP ratio) |
| **Content-defined chunking** | ❌ | ✅ | ❌ | ❌ | ❌ |
| **Sorting columns metadata** | ✅ | ✅ | ✅ | ❌ | ❌ |
| **Created-by string** | ✅ | ✅ | ✅ | ✅ | ✅ (automatic) |
| **Page checksum (CRC)** | ✅ (default enabled) | ✅ | ❌ | ❌ | ❌ |
| **HDFS block alignment/padding** | ✅ (8 MB default padding) | ❌ | ❌ | ❌ | ❌ |
| **Encryption** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Custom ValuesWriterFactory** | ✅ (pluggable) | ❌ | ❌ | ❌ | ❌ |
| **Min/max record count for size check** | ✅ (100 / 10,000) | ❌ | ❌ | ❌ | ❌ |
| **Key-value metadata** | ✅ | ✅ | ✅ | ✅ | ✅ (`KV_METADATA`) |
| **Field IDs** | ✅ | ✅ | ✅ | ❌ | ✅ (`FIELD_IDS`, auto-generated) |
| **GeoParquet** | ❌ | ❌ | ❌ | ❌ | ✅ (V1, V2, BOTH, NONE) |
| **Row groups per file** | ❌ | ❌ | ❌ | ❌ | ✅ (`ROW_GROUPS_PER_FILE`) |

### 5.2 Notable Unique Features

- **Java only**: Pluggable `ValuesWriterFactory` (users can supply entirely custom encoding logic); per-column `ByteStreamSplit` toggle; compression-effectiveness check on first page; adaptive row group memory sampling with configurable min/max check intervals; HDFS block alignment with padding
- **C++ only**: Content-Defined Chunking (CDC) for smarter page boundaries
- **Rust only**: Per-column data page size limits; per-column dictionary page size limits; byte-based row group limits (without the adaptive sampling of Java)
- **Go only**: Adaptive bloom filters; explicitly named `FallbackToPlain()` as a public API allowing user-triggered fallback
- **DuckDB only**: Analyze-before-write architecture (full row group scan determines encoding before any data is serialized); automatic BYTE_STREAM_SPLIT for FLOAT/DOUBLE in V2; GeoParquet support; `ROW_GROUPS_PER_FILE` for file rotation control; VARIANT column shredding; SQL-native `COPY TO` parameter syntax with human-readable memory sizes

---

## 6. Key Findings and Implications

### 6.1 Encoding Intelligence

**Java is the gold standard** for encoding selection, with Rust and DuckDB closely following. All three automatically select type-appropriate encodings: `DELTA_BINARY_PACKED` for integers, `DELTA_BYTE_ARRAY` (Java/Rust) or `DELTA_LENGTH_BYTE_ARRAY` (DuckDB) for binary data, and `RLE` (Java/Rust) for booleans when using V2 writer version. Java goes further with a dedicated `ByteStreamSplit` toggle for floating-point types and a pluggable `ValuesWriterFactory` for entirely custom encoding strategies. DuckDB uniquely auto-enables `BYTE_STREAM_SPLIT` for FLOAT/DOUBLE columns in V2 mode without any user configuration.

C++ and Go both default to PLAIN for all non-dictionary data, meaning users must **explicitly opt in** to more efficient encodings. This is simpler and more predictable but leaves performance on the table for users who don't tune their settings.

### 6.2 Dictionary Fallback

A critical distinction across all five implementations:

- **Java**: Falls back to type-appropriate encoding (DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY, etc.) AND checks on the first page whether dictionary encoding is actually saving space — if not, falls back immediately. The fallback writer is pre-configured at column writer creation time.
- **Rust**: Falls back to type-appropriate encoding (mirrors Java's V2 factory). No compression-effectiveness check.
- **DuckDB**: Makes a single, definitive encoding decision during the `Analyze()` phase by scanning all row group values. If the dictionary is viable (≤ rows/5 entries and within byte limit), dictionary encoding is used; otherwise, a type-appropriate alternative is chosen. There is no mid-stream fallback because the decision precedes encoding.
- **C++ and Go**: Always fall back to PLAIN, regardless of what encoding the user configured or what the type is. This means high-cardinality INT64 columns get PLAIN instead of DELTA_BINARY_PACKED.

### 6.3 Row Group Sizing Sophistication

Java's row group sizing is the most sophisticated by a wide margin:
- **Byte-based** primary threshold (128 MB default) rather than row-count
- **Adaptive memory sampling** that avoids checking buffered size on every row
- **HDFS block alignment** awareness
- **Prediction-based scheduling** for when to next check memory

Rust offers the next most automated approach with combined row-count and byte-count limits but without adaptive sampling. DuckDB uses a notably small default (122,880 rows) aligned to its internal execution engine chunk sizes, with an optional byte-based limit. C++ uses simple row-count limits. Go is the most manual.

### 6.4 Runtime Adaptation

Java is the only implementation with mid-stream runtime data analysis: its `isCompressionSatisfying()` check examines whether dictionary encoding is actually reducing size after the first page. If `encodedSize + dictionaryByteSize >= rawSize`, it abandons dictionary encoding entirely.

DuckDB takes a different approach to adaptation: its `Analyze()` phase performs a **pre-write scan** of the entire row group to build a dictionary and determine encoding viability. While this isn't "runtime" adaptation in the streaming sense (it requires buffering the full row group), it's more informed than any other implementation because it has access to the complete data before making decisions.

Beyond these, **none of the five implementations perform deep runtime data analysis** to select encodings. There is no logic that examines value distributions, cardinality beyond dictionary size, run lengths, or sortedness to choose between encodings. The primary "adaptive" behavior across all implementations is dictionary encoding with fallback: start with dictionary, and if the dictionary gets too large, fall back to a simpler encoding. This is a one-way switch per column chunk.

### 6.5 Architectural Philosophies

The five implementations represent three distinct architectural approaches:

1. **Library-first** (C++, Rust, Go): Designed as general-purpose Parquet libraries offering maximum control via `WriterProperties` builders. Users are expected to understand encoding tradeoffs. The library provides sensible defaults but doesn't aggressively optimize.

2. **Reference implementation** (Java): The most feature-complete, with the deepest integration into the Hadoop ecosystem (HDFS alignment, pluggable factories). Prioritizes correctness and adaptability over simplicity.

3. **Database-embedded** (DuckDB): Designed for a specific use case (analytical SQL `COPY TO`). The analyze-before-write architecture enables better encoding decisions at the cost of requiring full row group buffering. Offers fewer knobs but makes smarter automatic choices. Trades per-column configurability for whole-table optimization.

### 6.6 Practical Recommendations for Users

1. **For best out-of-the-box encoding**: Use DuckDB with `PARQUET_VERSION 'V2'` for automatic BYTE_STREAM_SPLIT on floats and DELTA encodings on integers; or Java/Rust with `PARQUET_2_0` writer version
2. **For maximum encoding control**: Java's pluggable `ValuesWriterFactory` allows completely custom encoding strategies; C++/Rust/Go offer fine-grained per-column encoding overrides
3. **For maximum page-level control**: C++ offers CDC and the most page-sizing configuration knobs; Rust offers per-column page size limits
4. **For explicit, manual control over row groups**: Go gives users direct control over row group boundaries
5. **For storage-aligned row groups**: Java's HDFS block alignment and adaptive memory sampling produce the best-fit row groups
6. **To improve encoding for specific columns**: Java, C++, Rust, and Go support per-column encoding overrides — use `DELTA_BINARY_PACKED` for integer columns and `DELTA_BYTE_ARRAY` for string columns with common prefixes. DuckDB selects these automatically in V2 mode.
7. **For floating-point data with compression**: Enable `ByteStreamSplit` — DuckDB does this automatically in V2; Java has a dedicated toggle; others require explicit encoding override
8. **Dictionary page size**: The 1 MB default (C++/Rust/Go/Java) works well for low-to-medium cardinality columns. DuckDB's rows/5 heuristic is cardinality-aware. For very high cardinality, consider disabling dictionary encoding entirely to avoid the overhead of dictionary encoding followed by fallback
9. **For SQL workflows**: DuckDB's `COPY TO` syntax provides the simplest configuration surface (e.g., `COPY t TO 'f.parquet' (COMPRESSION 'ZSTD', PARQUET_VERSION 'V2', ROW_GROUP_SIZE 1000000)`)
