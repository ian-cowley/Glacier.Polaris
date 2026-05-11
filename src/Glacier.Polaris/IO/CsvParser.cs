using System;
using System.Buffers;
using System.Collections.Generic;

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
                if (line[i] == (byte)'\"')
                {
                    // Quoted field
                    i++; // Skip opening quote
                    int start = i;
                    int parsedLength = 0;

                    while (i < rowLength)
                    {
                        if (line[i] == (byte)'\"')
                        {
                            if (i + 1 < rowLength && line[i + 1] == (byte)'\"')
                            {
                                i += 2;
                                parsedLength += 2;
                                continue;
                            }
                            else
                            {
                                i++; // Skip closing quote
                                break;
                            }
                        }
                        i++;
                        parsedLength++;
                    }

                    if (builders[colIndex] != null)
                    {
                        builders[colIndex]!.AddToken(line.Slice(start, parsedLength));
                    }
                    colIndex++;

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
            // Look for a newline in the buffer
            SequencePosition? position = buffer.PositionOf((byte)'\n');

            if (position == null)
            {
                if (isFinalBlock && buffer.Length > 0)
                {
                    line = buffer;
                    buffer = buffer.Slice(buffer.End);
                    return true;
                }
                line = default;
                return false;
            }

            // Slice the line from the start to the newline
            line = buffer.Slice(0, position.Value);
            
            // Advance the buffer past the newline
            buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
            return true;
        }

        // Extracts tokens for schema inference/headers
        public static List<string> ParseHeader(ReadOnlySpan<byte> line)
        {
            var list = new List<string>();
            int i = 0;

            while (i < line.Length)
            {
                if (line[i] == (byte)'\"')
                {
                    i++;
                    int start = i;
                    while (i < line.Length && line[i] != (byte)'\"') i++;
                    list.Add(System.Text.Encoding.UTF8.GetString(line.Slice(start, i - start)));
                    i++; // skip quote
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
                        list.Add(System.Text.Encoding.UTF8.GetString(token));
                        i = start + commaIndex + 1;
                    }
                    else
                    {
                        var token = line.Slice(start);
                        if (token.Length > 0 && token[^1] == '\r') token = token.Slice(0, token.Length - 1);
                        list.Add(System.Text.Encoding.UTF8.GetString(token));
                        break;
                    }
                }
            }
            return list;
        }
    }
}
