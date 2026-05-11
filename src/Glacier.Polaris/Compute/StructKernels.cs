using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute;

internal static class StructKernels
{
    /// <summary>
    /// Rename the fields of a StructSeries.
    /// </summary>
    public static StructSeries RenameFields(StructSeries structSeries, string[] newNames)
    {
        if (newNames.Length != structSeries.Fields.Length)
            throw new ArgumentException(
                $"Expected {structSeries.Fields.Length} field names but got {newNames.Length}.");

        var renamedFields = new ISeries[structSeries.Fields.Length];
        for (int i = 0; i < structSeries.Fields.Length; i++)
        {
            renamedFields[i] = structSeries.Fields[i];
            renamedFields[i].Rename(newNames[i]);
        }

        var result = new StructSeries(structSeries.Name, renamedFields);
        structSeries.ValidityMask.CopyTo(result.ValidityMask, 0);
        return result;
    }

    /// <summary>
    /// Serialize each row of a StructSeries to a JSON string.
    /// </summary>
    public static Utf8StringSeries JsonEncode(StructSeries structSeries)
    {
        int totalBytesEstimate = 0;
        var strings = new string[structSeries.Length];
        for (int i = 0; i < structSeries.Length; i++)
        {
            if (structSeries.ValidityMask.IsNull(i))
            {
                strings[i] = "null";
                totalBytesEstimate += 4;
                continue;
            }

            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);
            writer.WriteStartObject();
            for (int f = 0; f < structSeries.Fields.Length; f++)
            {
                var field = structSeries.Fields[f];
                writer.WritePropertyName(field.Name);
                WriteValue(writer, field, i);
            }
            writer.WriteEndObject();
            writer.Flush();
            strings[i] = Encoding.UTF8.GetString(ms.ToArray());
            totalBytesEstimate += strings[i].Length;
        }

        var result = new Utf8StringSeries(structSeries.Name + "_json", structSeries.Length, totalBytesEstimate);
        int offset = 0;
        for (int i = 0; i < structSeries.Length; i++)
        {
            var bytes = Encoding.UTF8.GetBytes(strings[i]);
            bytes.CopyTo(result.DataBytes.Span.Slice(offset));
            offset += bytes.Length;
            result.Offsets.Span[i + 1] = offset;

            if (structSeries.ValidityMask.IsNull(i))
                result.ValidityMask.SetNull(i);
        }
        return result;
    }

    /// <summary>
    /// Add or overwrite fields in a StructSeries with new field series.
    /// Existing fields with matching names are overwritten; new fields are appended.
    /// </summary>
    public static StructSeries WithFields(StructSeries structSeries, ISeries[] newFields)
    {
        var fieldDict = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < structSeries.Fields.Length; i++)
            fieldDict[structSeries.Fields[i].Name] = i;

        var updatedFields = new List<ISeries>(structSeries.Fields);

        foreach (var newField in newFields)
        {
            if (fieldDict.TryGetValue(newField.Name, out int existingIndex))
            {
                // Overwrite
                updatedFields[existingIndex] = newField;
                // Update the dictionary key reference (no change needed, same name)
            }
            else
            {
                // Append
                updatedFields.Add(newField);
                fieldDict[newField.Name] = updatedFields.Count - 1;
            }
        }

        var result = new StructSeries(structSeries.Name, updatedFields.ToArray());
        structSeries.ValidityMask.CopyTo(result.ValidityMask, 0);
        return result;
    }

    private static void WriteValue(Utf8JsonWriter writer, ISeries field, int rowIndex)
    {
        if (field.ValidityMask.IsNull(rowIndex))
        {
            writer.WriteNullValue();
            return;
        }

        switch (field)
        {
            case Int32Series i32:
                writer.WriteNumberValue(i32.Memory.Span[rowIndex]);
                break;
            case Int64Series i64:
                writer.WriteNumberValue(i64.Memory.Span[rowIndex]);
                break;
            case Float64Series f64:
                writer.WriteNumberValue(f64.Memory.Span[rowIndex]);
                break;
            case BooleanSeries bs:
                writer.WriteBooleanValue(bs.Memory.Span[rowIndex]);
                break;
            case Utf8StringSeries u8:
                writer.WriteStringValue(u8.GetStringSpan(rowIndex));
                break;
            case DateSeries ds:
                writer.WriteStringValue(ds.Get(rowIndex)?.ToString() ?? "");
                break;
            case DatetimeSeries dts:
                writer.WriteStringValue(dts.Get(rowIndex)?.ToString() ?? "");
                break;
            default:
                writer.WriteStringValue(field.Get(rowIndex)?.ToString() ?? "");
                break;
        }
    }
}
