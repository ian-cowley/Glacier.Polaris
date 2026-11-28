using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Glacier.Polaris.IO
{
    public static class CsvParser
    {
        public static void ParseRow(ReadOnlySpan<byte> line, IColumnBuilder?[] builders)
        {
            int colIndex = 0;
            int i = 0;
            int rowLength = line.Length;

            while (i < rowLength && colIndex < builders.Length)
            {
                if (line[i] == (byte)'"')
                {
                    // Quoted field (RFC-4180: supports escaped "" and embedded newlines)
                    i++; // Skip opening quote
                    int start = i;
                    bool hasEscapedQuote = false;

                    while (i < rowLength)
                    {
                        if (line[i] == (byte)'"')
                        {
                            if (i + 1 < rowLength && line[i + 1] == (byte)'"')
                            {
                                hasEscapedQuote = true;
                                i += 2;
                                continue;
                            }
                            break;
                        }
                        i++;
                    }

                    int rawLength = i - start;
                    if (i < rowLength && line[i] == (byte)'"')
                    {
                        i++; // Skip closing quote
                    }

                    if (builders[colIndex] != null)
                    {
                        var rawField = line.Slice(start, rawLength);
                        if (!hasEscapedQuote)
                        {
                            builders[colIndex]!.AddToken(rawField);
                        }
                        else
                        {
                            AddUnescapedToken(rawField, builders[colIndex]!);
                        }
                    }
                    colIndex++;

                    if (i < rowLength && line[i] == (byte)'\r')
                    {
                        i++;
                    }
                    if (i < rowLength && line[i] == (byte)',')
                    {
                        i++;
                    }
                }
                else
                {
                    // Unquoted field
                    int start = i;
                    int commaIndex = line.Slice(start).IndexOf((byte)',');

                    if (commaIndex >= 0)
                    {
                        if (builders[colIndex] != null)
                        {
                            var token = line.Slice(start, commaIndex);
                            if (token.Length > 0 && token[^1] == '\r')
                                token = token.Slice(0, token.Length - 1);

                            builders[colIndex]!.AddToken(token);
                        }
                        colIndex++;
                        i = start + commaIndex + 1; // Skip comma
                    }
                    else
                    {
                        // Last field
                        if (builders[colIndex] != null)
                        {
                            var token = line.Slice(start);
                            if (token.Length > 0 && token[^1] == '\r')
                                token = token.Slice(0, token.Length - 1);

                            builders[colIndex]!.AddToken(token);
                        }
                        colIndex++;
                        break;
                    }
                }
            }

            while (colIndex < builders.Length)
            {
                builders[colIndex]?.AddToken(ReadOnlySpan<byte>.Empty);
                colIndex++;
            }
        }

        public static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line, bool isFinalBlock = false)
        {
            if (buffer.IsEmpty)
            {
                line = default;
                return false;
            }

            bool inQuotes = false;
            SequencePosition pos = buffer.Start;
            while (buffer.TryGet(ref pos, out ReadOnlyMemory<byte> mem, advance: false))
            {
                var span = mem.Span;
                int offset = 0;
                while (offset < span.Length)
                {
                    if (!inQuotes)
                    {
                        int idx = span.Slice(offset).IndexOfAny((byte)'"', (byte)'\n');
                        if (idx < 0) break;

                        offset += idx;
                        if (span[offset] == (byte)'\n')
                        {
                            SequencePosition nlPos = buffer.GetPosition(offset, pos);
                            line = buffer.Slice(0, nlPos);
                            buffer = buffer.Slice(buffer.GetPosition(1, nlPos));
                            return true;
                        }

                        inQuotes = true;
                        offset++;
                    }
                    else
                    {
                        int idx = span.Slice(offset).IndexOf((byte)'"');
                        if (idx < 0) break;

                        offset += idx + 1;
                        inQuotes = false;
                    }
                }

                buffer.TryGet(ref pos, out _, advance: true);
            }

            if (isFinalBlock && buffer.Length > 0)
            {
                line = buffer;
                buffer = buffer.Slice(buffer.End);
                return true;
            }

            line = default;
            return false;
        }

        // Extracts tokens for schema inference/headers
        public static List<string> ParseHeader(ReadOnlySpan<byte> line)
        {
            var list = new List<string>();
            int i = 0;

            while (i < line.Length)
            {
                if (line[i] == (byte)'"')
                {
                    i++;
                    int start = i;
                    bool hasEscapedQuote = false;
                    while (i < line.Length)
                    {
                        if (line[i] == (byte)'"')
                        {
                            if (i + 1 < line.Length && line[i + 1] == (byte)'"')
                            {
                                hasEscapedQuote = true;
                                i += 2;
                                continue;
                            }
                            break;
                        }
                        i++;
                    }

                    int rawLength = i - start;
                    var rawField = line.Slice(start, rawLength);
                    if (!hasEscapedQuote)
                    {
                        list.Add(Encoding.UTF8.GetString(rawField));
                    }
                    else
                    {
                        list.Add(DecodeUnescapedString(rawField));
                    }

                    if (i < line.Length && line[i] == (byte)'"') i++; // skip closing quote
                    if (i < line.Length && line[i] == (byte)'\r') i++;
                    if (i < line.Length && line[i] == (byte)',') i++;
                }
                else
                {
                    int start = i;
                    int commaIndex = line.Slice(start).IndexOf((byte)',');
                    if (commaIndex >= 0)
                    {
                        var token = line.Slice(start, commaIndex);
                        if (token.Length > 0 && token[^1] == '\r') token = token.Slice(0, token.Length - 1);
                        list.Add(Encoding.UTF8.GetString(token));
                        i = start + commaIndex + 1;
                    }
                    else
                    {
                        var token = line.Slice(start);
                        if (token.Length > 0 && token[^1] == '\r') token = token.Slice(0, token.Length - 1);
                        list.Add(Encoding.UTF8.GetString(token));
                        break;
                    }
                }
            }
            return list;
        }

        private static void AddUnescapedToken(ReadOnlySpan<byte> rawField, IColumnBuilder builder)
        {
            byte[]? rented = null;
            Span<byte> dest = rawField.Length <= 1024
                ? stackalloc byte[1024]
                : (rented = ArrayPool<byte>.Shared.Rent(rawField.Length));
            try
            {
                int written = UnescapeDoubleQuotes(rawField, dest);
                builder.AddToken(dest.Slice(0, written));
            }
            finally
            {
                if (rented != null)
                    ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private static string DecodeUnescapedString(ReadOnlySpan<byte> rawField)
        {
            byte[]? rented = null;
            Span<byte> dest = rawField.Length <= 1024
                ? stackalloc byte[1024]
                : (rented = ArrayPool<byte>.Shared.Rent(rawField.Length));
            try
            {
                int written = UnescapeDoubleQuotes(rawField, dest);
                return Encoding.UTF8.GetString(dest.Slice(0, written));
            }
            finally
            {
                if (rented != null)
                    ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private static int UnescapeDoubleQuotes(ReadOnlySpan<byte> src, Span<byte> dst)
        {
            int write = 0;
            for (int j = 0; j < src.Length; j++)
            {
                byte b = src[j];
                if (b == (byte)'"' && j + 1 < src.Length && src[j + 1] == (byte)'"')
                {
                    dst[write++] = (byte)'"';
                    j++;
                }
                else
                {
                    dst[write++] = b;
                }
            }
            return write;
        }
    }
}
