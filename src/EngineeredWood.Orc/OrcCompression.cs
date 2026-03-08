using System.Buffers;
using System.IO.Compression;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc;

/// <summary>
/// Handles ORC stream decompression. ORC uses a block-based compression scheme
/// where each compressed block has a 3-byte header.
/// </summary>
internal static class OrcCompression
{
    /// <summary>
    /// Decompresses an ORC compressed stream (which may contain multiple blocks).
    /// Each block has a 3-byte header:
    ///   - byte 0 bit 0: 0 = original (uncompressed), 1 = compressed
    ///   - remaining 23 bits: length of the chunk (original or compressed)
    /// </summary>
    [ThreadStatic]
    private static MemoryStream? t_outputBuffer;

    public static byte[] Decompress(CompressionKind kind, ReadOnlySpan<byte> input, int compressionBlockSize)
    {
        if (kind == CompressionKind.None)
            return input.ToArray();

        var output = t_outputBuffer ??= new MemoryStream();
        output.SetLength(0);
        var offset = 0;

        while (offset < input.Length)
        {
            if (offset + 3 > input.Length)
                throw new InvalidDataException("Truncated ORC compression block header.");

            // Read 3-byte header (little-endian)
            int header = input[offset] | (input[offset + 1] << 8) | (input[offset + 2] << 16);
            offset += 3;

            bool isOriginal = (header & 1) == 1;
            int chunkLength = header >> 1;

            if (offset + chunkLength > input.Length)
                throw new InvalidDataException($"ORC compression block exceeds input bounds: offset={offset}, chunk={chunkLength}, total={input.Length}");

            var chunk = input.Slice(offset, chunkLength);
            offset += chunkLength;

            if (isOriginal)
            {
                output.Write(chunk);
            }
            else
            {
                DecompressBlock(kind, chunk, output, compressionBlockSize);
            }
        }

        return output.ToArray();
    }

    /// <summary>
    /// Decompresses like <see cref="Decompress"/>, but returns a buffer rented from
    /// <see cref="ArrayPool{T}.Shared"/>. The caller must return the buffer when done.
    /// </summary>
    public static (byte[] buffer, int length) DecompressPooled(CompressionKind kind, ReadOnlySpan<byte> input, int compressionBlockSize)
    {
        if (kind == CompressionKind.None)
        {
            var copy = ArrayPool<byte>.Shared.Rent(input.Length);
            input.CopyTo(copy);
            return (copy, input.Length);
        }

        var output = t_outputBuffer ??= new MemoryStream();
        output.SetLength(0);
        var offset = 0;

        while (offset < input.Length)
        {
            if (offset + 3 > input.Length)
                throw new InvalidDataException("Truncated ORC compression block header.");

            int header = input[offset] | (input[offset + 1] << 8) | (input[offset + 2] << 16);
            offset += 3;

            bool isOriginal = (header & 1) == 1;
            int chunkLength = header >> 1;

            if (offset + chunkLength > input.Length)
                throw new InvalidDataException($"ORC compression block exceeds input bounds: offset={offset}, chunk={chunkLength}, total={input.Length}");

            var chunk = input.Slice(offset, chunkLength);
            offset += chunkLength;

            if (isOriginal)
                output.Write(chunk);
            else
                DecompressBlock(kind, chunk, output, compressionBlockSize);
        }

        int length = (int)output.Length;
        var pooled = ArrayPool<byte>.Shared.Rent(length);
        output.Position = 0;
        output.Read(pooled, 0, length);
        return (pooled, length);
    }

    private static void DecompressBlock(CompressionKind kind, ReadOnlySpan<byte> compressed, MemoryStream output, int maxDecompressedSize)
    {
        switch (kind)
        {
            case CompressionKind.Zlib:
                DecompressZlib(compressed, output);
                break;
            case CompressionKind.Snappy:
                DecompressSnappy(compressed, output);
                break;
            case CompressionKind.Lz4:
                DecompressLz4(compressed, output, maxDecompressedSize);
                break;
            case CompressionKind.Zstd:
                DecompressZstd(compressed, output);
                break;
            default:
                throw new NotSupportedException($"Compression kind {kind} is not yet supported.");
        }
    }

    private static void DecompressZlib(ReadOnlySpan<byte> compressed, MemoryStream output)
    {
        // ORC uses raw deflate (RFC 1951) without zlib or gzip headers
        // Use unsafe to pin the span and wrap in UnmanagedMemoryStream to avoid ToArray() copy
        unsafe
        {
            fixed (byte* ptr = compressed)
            {
                using var input = new UnmanagedMemoryStream(ptr, compressed.Length);
                using var deflate = new DeflateStream(input, CompressionMode.Decompress);
                deflate.CopyTo(output);
            }
        }
    }

    private static void DecompressSnappy(ReadOnlySpan<byte> compressed, MemoryStream output)
    {
        var decompressed = new byte[Snappier.Snappy.GetUncompressedLength(compressed)];
        int written = Snappier.Snappy.Decompress(compressed, decompressed);
        output.Write(decompressed, 0, written);
    }

    private static void DecompressLz4(ReadOnlySpan<byte> compressed, MemoryStream output, int maxDecompressedSize)
    {
        var decompressed = new byte[maxDecompressedSize];
        int written = K4os.Compression.LZ4.LZ4Codec.Decode(compressed, decompressed);
        if (written < 0)
            throw new InvalidDataException("LZ4 decompression failed.");
        output.Write(decompressed, 0, written);
    }

    private static void DecompressZstd(ReadOnlySpan<byte> compressed, MemoryStream output)
    {
        using var decompressor = new ZstdSharp.Decompressor();
        var decompressed = decompressor.Unwrap(compressed);
        output.Write(decompressed);
    }

    /// <summary>
    /// Compresses data using ORC's block-based compression scheme.
    /// Each block has a 3-byte header: bit0=isOriginal, bits1-23=chunkLength.
    /// </summary>
    public static byte[] Compress(CompressionKind kind, ReadOnlySpan<byte> input, int compressionBlockSize)
    {
        if (kind == CompressionKind.None)
            return input.ToArray();

        var output = t_outputBuffer ??= new MemoryStream();
        output.SetLength(0);

        int offset = 0;
        while (offset < input.Length)
        {
            int blockSize = Math.Min(compressionBlockSize, input.Length - offset);
            var block = input.Slice(offset, blockSize);
            offset += blockSize;

            var compressed = CompressBlock(kind, block);
            if (compressed != null && compressed.Length < blockSize)
            {
                // Write compressed block header: isOriginal=0, length=compressed.Length
                int header = compressed.Length << 1;
                output.WriteByte((byte)(header & 0xFF));
                output.WriteByte((byte)((header >> 8) & 0xFF));
                output.WriteByte((byte)((header >> 16) & 0xFF));
                output.Write(compressed);
            }
            else
            {
                // Write original (uncompressed) block header: isOriginal=1, length=blockSize
                int header = (blockSize << 1) | 1;
                output.WriteByte((byte)(header & 0xFF));
                output.WriteByte((byte)((header >> 8) & 0xFF));
                output.WriteByte((byte)((header >> 16) & 0xFF));
                output.Write(block);
            }
        }

        return output.ToArray();
    }

    /// <summary>
    /// Translates an uncompressed stream offset to compressed stream positions.
    /// Appends [compressed_block_start, decompressed_offset_within_block] to positions.
    /// </summary>
    public static void TranslatePosition(
        long uncompressedOffset, ReadOnlySpan<byte> uncompressedData,
        CompressionKind kind, int compressionBlockSize, IList<ulong> positions)
    {
        if (kind == CompressionKind.None)
        {
            positions.Add((ulong)uncompressedOffset);
            return;
        }

        // Walk through compression blocks to find which block contains our offset
        long inputOffset = 0;
        long compressedOffset = 0;

        while (inputOffset < uncompressedData.Length)
        {
            int blockSize = (int)Math.Min(compressionBlockSize, uncompressedData.Length - inputOffset);
            long nextInputOffset = inputOffset + blockSize;

            if (uncompressedOffset < nextInputOffset || nextInputOffset >= uncompressedData.Length)
            {
                // Our target is in this block
                positions.Add((ulong)compressedOffset);
                positions.Add((ulong)(uncompressedOffset - inputOffset));
                return;
            }

            // Compute compressed size of this block
            var block = uncompressedData.Slice((int)inputOffset, blockSize);
            var compressed = CompressBlock(kind, block);
            int outputBlockSize = (compressed != null && compressed.Length < blockSize)
                ? compressed.Length : blockSize;

            compressedOffset += 3 + outputBlockSize; // 3-byte header + data
            inputOffset = nextInputOffset;
        }

        // Offset is at or past the end
        positions.Add((ulong)compressedOffset);
        positions.Add(0);
    }

    private static byte[]? CompressBlock(CompressionKind kind, ReadOnlySpan<byte> data)
    {
        return kind switch
        {
            CompressionKind.Zlib => CompressZlib(data),
            CompressionKind.Snappy => CompressSnappy(data),
            CompressionKind.Lz4 => CompressLz4(data),
            CompressionKind.Zstd => CompressZstd(data),
            _ => null,
        };
    }

    private static byte[]? CompressZlib(ReadOnlySpan<byte> data)
    {
        using var ms = new MemoryStream();
        using (var deflate = new DeflateStream(ms, CompressionLevel.Optimal, leaveOpen: true))
        {
            deflate.Write(data);
        }
        return ms.ToArray();
    }

    private static byte[]? CompressSnappy(ReadOnlySpan<byte> data)
    {
        int maxLen = Snappier.Snappy.GetMaxCompressedLength(data.Length);
        var output = new byte[maxLen];
        int written = Snappier.Snappy.Compress(data, output);
        return output.AsSpan(0, written).ToArray();
    }

    private static byte[]? CompressLz4(ReadOnlySpan<byte> data)
    {
        var output = new byte[K4os.Compression.LZ4.LZ4Codec.MaximumOutputSize(data.Length)];
        int written = K4os.Compression.LZ4.LZ4Codec.Encode(data, output);
        if (written <= 0) return null;
        return output.AsSpan(0, written).ToArray();
    }

    private static byte[]? CompressZstd(ReadOnlySpan<byte> data)
    {
        using var compressor = new ZstdSharp.Compressor();
        var compressed = compressor.Wrap(data);
        return compressed.ToArray();
    }
}
