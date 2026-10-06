using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class StringKernels
    {
        /// <summary>Split each string by a separator, returning a ListSeries.</summary>
        public static ListSeries Split(Utf8StringSeries source, string separator, int? maxSplits = null)
        {
            int rowCount = source.Length;
            int totalElements = 0;
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) continue;
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (maxSplits.HasValue)
                {
                    var parts = s.Split(separator, maxSplits.Value, StringSplitOptions.None);
                    totalElements += parts.Length;
                }
                else
                {
                    var parts = s.Split(new[] { separator }, StringSplitOptions.None);
                    totalElements += parts.Length;
                }
            }

            var values = new string[totalElements];
            var offsets = new int[rowCount + 1];
            int idx = 0;
            for (int i = 0; i < rowCount; i++)
            {
                offsets[i] = idx;
                if (source.ValidityMask.IsNull(i)) continue;
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                string[] parts;
                if (maxSplits.HasValue)
                    parts = s.Split(separator, maxSplits.Value, StringSplitOptions.None);
                else
                    parts = s.Split(new[] { separator }, StringSplitOptions.None);
                foreach (var part in parts) values[idx++] = part;
            }
            offsets[rowCount] = idx;

            var valueSeries = Utf8StringSeries.FromStrings(source.Name + "_split", values);
            var offsetSeries = new Int32Series(source.Name + "_offsets", offsets);
            return new ListSeries(source.Name, offsetSeries, valueSeries);
        }

        /// <summary>Parse strings to DateSeries using optional format.</summary>
        public static DateSeries ParseDate(Utf8StringSeries source, string? format = null)
        {
            int rowCount = source.Length;
            var result = new DateSeries(source.Name + "_date", rowCount);
            var span = result.Memory.Span;
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (DateTime.TryParseExact(s, format ?? "yyyy-MM-dd", null, System.Globalization.DateTimeStyles.None, out var dt))
                {
                    span[i] = (int)(dt.Date - epoch).TotalDays;
                }
                else if (format == null && DateTime.TryParse(s, out dt))
                {
                    span[i] = (int)(dt.Date - epoch).TotalDays;
                }
                else
                {
                    result.ValidityMask.SetNull(i);
                }
            }
            return result;
        }

        /// <summary>Parse strings to DatetimeSeries using optional format.</summary>
        public static DatetimeSeries ParseDatetime(Utf8StringSeries source, string? format = null)
        {
            int rowCount = source.Length;
            var result = new DatetimeSeries(source.Name + "_datetime", rowCount);
            var span = result.Memory.Span;
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (DateTime.TryParseExact(s, format ?? "yyyy-MM-dd HH:mm:ss", null, System.Globalization.DateTimeStyles.AssumeUniversal, out var dt))
                {
                    span[i] = (dt.Ticks - epoch.Ticks) * 100; // Convert to nanoseconds
                }
                else if (format == null && DateTime.TryParse(s, out dt))
                {
                    span[i] = (dt.ToUniversalTime().Ticks - epoch.Ticks) * 100;
                }
                else
                {
                    result.ValidityMask.SetNull(i);
                }
            }
            return result;
        }

        /// <summary>Extract the first match of a regex pattern from each string.</summary>
        public static Utf8StringSeries Extract(Utf8StringSeries source, string pattern)
        {
            var regex = GetOrAddRegex(pattern).netRegex;
            int rowCount = source.Length;
            var strings = new string[rowCount];

            int numThreads = Math.Max(1, Environment.ProcessorCount);
            if (rowCount < numThreads * 2)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                    var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                    var match = regex.Match(s);
                    strings[i] = match.Success ? match.Value : string.Empty;
                }
            }
            else
            {
                System.Threading.Tasks.Parallel.For(0, numThreads, t =>
                {
                    int startRow = (int)((long)t * rowCount / numThreads);
                    int endRow = (int)((long)(t + 1) * rowCount / numThreads);
                    for (int i = startRow; i < endRow; i++)
                    {
                        if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                        var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                        var match = regex.Match(s);
                        strings[i] = match.Success ? match.Value : string.Empty;
                    }
                });
            }

            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Extract all matches of a regex pattern from each string, returning a ListSeries.</summary>
        public static ListSeries ExtractAll(Utf8StringSeries source, string pattern)
        {
            var regex = GetOrAddRegex(pattern).netRegex;
            int rowCount = source.Length;
            int numThreads = Math.Max(1, Environment.ProcessorCount);

            if (rowCount < numThreads * 2)
            {
                var allMatches = new List<string>();
                var offsets = new int[rowCount + 1];
                int total = 0;
                for (int i = 0; i < rowCount; i++)
                {
                    offsets[i] = total;
                    if (!source.ValidityMask.IsNull(i))
                    {
                        var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                        var matches = regex.Matches(s);
                        foreach (Match m in matches)
                        {
                            allMatches.Add(m.Value);
                            total++;
                        }
                    }
                }
                offsets[rowCount] = total;

                var smallValueSeries = Utf8StringSeries.FromStrings(source.Name + "_extract_all", allMatches.ToArray());
                var smallOffsetSeries = new Int32Series(source.Name + "_offsets", offsets);
                return new ListSeries(source.Name, smallOffsetSeries, smallValueSeries);
            }

            var threadMatches = new List<string>[numThreads];
            var offsetsArr = new int[rowCount + 1];

            System.Threading.Tasks.Parallel.For(0, numThreads, t =>
            {
                int startRow = (int)((long)t * rowCount / numThreads);
                int endRow = (int)((long)(t + 1) * rowCount / numThreads);
                var localList = new List<string>();
                threadMatches[t] = localList;

                for (int i = startRow; i < endRow; i++)
                {
                    if (source.ValidityMask.IsNull(i))
                    {
                        offsetsArr[i + 1] = 0;
                        continue;
                    }
                    var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                    var matches = regex.Matches(s);
                    int count = 0;
                    foreach (Match m in matches)
                    {
                        localList.Add(m.Value);
                        count++;
                    }
                    offsetsArr[i + 1] = count;
                }
            });

            int totalCount = 0;
            offsetsArr[0] = 0;
            for (int i = 0; i < rowCount; i++)
            {
                int count = offsetsArr[i + 1];
                offsetsArr[i] = totalCount;
                totalCount += count;
            }
            offsetsArr[rowCount] = totalCount;

            var valueArray = new string[totalCount];
            int[] threadDestOffsets = new int[numThreads];
            int currentDestOffset = 0;
            for (int t = 0; t < numThreads; t++)
            {
                threadDestOffsets[t] = currentDestOffset;
                currentDestOffset += threadMatches[t].Count;
            }

            System.Threading.Tasks.Parallel.For(0, numThreads, t =>
            {
                var localList = threadMatches[t];
                int destOffset = threadDestOffsets[t];
                for (int j = 0; j < localList.Count; j++)
                {
                    valueArray[destOffset + j] = localList[j];
                }
            });

            var valueSeries = Utf8StringSeries.FromStrings(source.Name + "_extract_all", valueArray);
            var offsetSeries = new Int32Series(source.Name + "_offsets", offsetsArr);
            return new ListSeries(source.Name, offsetSeries, valueSeries);
        }

        /// <summary>Decode JSON strings into a StructSeries. Each string must be a JSON object.</summary>
        public static StructSeries JsonDecode(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var options = new JsonDocumentOptions { AllowTrailingCommas = true };

            var propertyNames = new HashSet<string>(StringComparer.Ordinal);
            var parsedDocs = new JsonDocument[rowCount];

            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) continue;
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (string.IsNullOrWhiteSpace(s)) continue;
                try
                {
                    parsedDocs[i] = JsonDocument.Parse(s, options);
                    foreach (var prop in parsedDocs[i].RootElement.EnumerateObject())
                    {
                        propertyNames.Add(prop.Name);
                    }
                }
                catch
                {
                }
            }

            var names = propertyNames.ToArray();
            var fields = new List<ISeries>();

            foreach (var propName in names)
            {
                Type inferredType = typeof(string);
                for (int i = 0; i < rowCount; i++)
                {
                    if (parsedDocs[i] == null) continue;
                    if (parsedDocs[i].RootElement.TryGetProperty(propName, out var prop))
                    {
                        inferredType = prop.ValueKind switch
                        {
                            JsonValueKind.Number => typeof(double),
                            JsonValueKind.True or JsonValueKind.False => typeof(bool),
                            JsonValueKind.String => typeof(string),
                            _ => typeof(string)
                        };
                        break;
                    }
                }

                if (inferredType == typeof(double))
                {
                    var field = new Float64Series(propName, rowCount);
                    var span = field.Memory.Span;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (parsedDocs[i] == null)
                        {
                            field.ValidityMask.SetNull(i);
                        }
                        else if (parsedDocs[i].RootElement.TryGetProperty(propName, out var prop))
                        {
                            if (prop.ValueKind == JsonValueKind.Number)
                                span[i] = prop.GetDouble();
                            else if (prop.ValueKind == JsonValueKind.String && double.TryParse(prop.GetString(), out var d))
                                span[i] = d;
                            else
                                field.ValidityMask.SetNull(i);
                        }
                        else
                        {
                            field.ValidityMask.SetNull(i);
                        }
                    }
                    fields.Add(field);
                }
                else if (inferredType == typeof(bool))
                {
                    var field = new BooleanSeries(propName, rowCount);
                    var span = field.Memory.Span;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (parsedDocs[i] == null)
                        {
                            field.ValidityMask.SetNull(i);
                        }
                        else if (parsedDocs[i].RootElement.TryGetProperty(propName, out var prop))
                        {
                            if (prop.ValueKind == JsonValueKind.True || prop.ValueKind == JsonValueKind.False)
                                span[i] = prop.GetBoolean();
                            else
                                field.ValidityMask.SetNull(i);
                        }
                        else
                        {
                            field.ValidityMask.SetNull(i);
                        }
                    }
                    fields.Add(field);
                }
                else
                {
                    var strings = new string[rowCount];
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (parsedDocs[i] == null)
                        {
                            strings[i] = null!;
                        }
                        else if (parsedDocs[i].RootElement.TryGetProperty(propName, out var prop))
                        {
                            strings[i] = prop.ValueKind == JsonValueKind.String
                                ? prop.GetString()!
                                : prop.GetRawText();
                        }
                        else
                        {
                            strings[i] = null!;
                        }
                    }
                    var field = Utf8StringSeries.FromStrings(propName, strings);
                    fields.Add(field);
                }
            }

            for (int i = 0; i < rowCount; i++)
            {
                parsedDocs[i]?.Dispose();
            }

            var result = new StructSeries(source.Name + "_decoded", fields.ToArray());
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i))
                    result.ValidityMask.SetNull(i);
            }
            return result;
        }

        /// <summary>Encode each string as a JSON string value (wraps in quotes with proper escaping).</summary>
        public static Utf8StringSeries JsonEncode(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            int totalBytes = 0;
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i))
                {
                    strings[i] = "null";
                    totalBytes += 4;
                }
                else
                {
                    var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                    strings[i] = JsonSerializer.Serialize(s);
                    totalBytes += Encoding.UTF8.GetByteCount(strings[i]);
                }
            }

            var result = new Utf8StringSeries(source.Name + "_json", rowCount, totalBytes);
            int offset = 0;
            for (int i = 0; i < rowCount; i++)
            {
                result.Offsets.Span[i] = offset;
                var bytes = Encoding.UTF8.GetBytes(strings[i]);
                bytes.CopyTo(result.DataBytes.Span.Slice(offset));
                offset += bytes.Length;
                if (source.ValidityMask.IsNull(i))
                    result.ValidityMask.SetNull(i);
            }
            result.Offsets.Span[rowCount] = offset;
            return result;
        }
    }
}
