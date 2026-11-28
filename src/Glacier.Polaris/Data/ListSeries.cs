using System;
using Glacier.Formats.Arrow;

namespace Glacier.Polaris.Data
{
    public sealed class ListSeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(Array);
        public int Length { get; }

        public Int32Series Offsets { get; }
        public ISeries Values { get; private set; }
        public Memory.ValidityMask ValidityMask { get; }

        public ListSeries(string name, Int32Series offsets, ISeries values)
        {
            Name = name;
            Length = offsets.Length - 1;
            Offsets = offsets;
            Values = values;
            ValidityMask = new Memory.ValidityMask(Length);
        }

        public void CopyTo(ISeries target, int offset)
        {
            if (target is not ListSeries t)
                throw new ArgumentException("Target series type mismatch.", nameof(target));

            ArgumentOutOfRangeException.ThrowIfNegative(offset);
            if (offset > t.Length - Length)
                throw new ArgumentOutOfRangeException(nameof(offset), "Target ListSeries is smaller than source offset + length.");

            // Upfront atomicity check before mutating target offsets, validity mask, or values.
            if (!AreValuesCompatible(Values, t.Values))
                throw new InvalidOperationException($"Target values series type mismatch: expected {Values.GetType().Name}, got {t.Values.GetType().Name}.");

            if (Length == 0)
                return;

            var srcOffsets = Offsets.Memory.Span;
            var targetOffsets = t.Offsets.Memory.Span;

            int baseValueOffset = offset == 0 ? 0 : targetOffsets[offset];
            int oldEnd = targetOffsets[offset + Length];
            int totalTargetValues = targetOffsets[t.Length];

            if (baseValueOffset < 0 || oldEnd < baseValueOffset || totalTargetValues < oldEnd || totalTargetValues > t.Values.Length)
                throw new InvalidOperationException("Target ListSeries offsets are out of range of target values.");

            int srcValueStart = srcOffsets[0];
            if (srcValueStart < 0 || srcOffsets[Length] < srcValueStart || srcOffsets[Length] > Values.Length)
                throw new InvalidOperationException("Source ListSeries offsets are out of range of source values.");

            int totalSrcValues = 0;
            bool hasNonEmptyNullRow = false;
            for (int i = 0; i < Length; i++)
            {
                int rowStart = srcOffsets[i];
                int rowEnd = srcOffsets[i + 1];
                if (rowEnd < rowStart)
                    throw new InvalidOperationException($"Corrupt source list offsets at row {i}: start={rowStart}, end={rowEnd}.");

                int rowLen = rowEnd - rowStart;
                if (ValidityMask.IsNull(i))
                {
                    if (rowLen != 0)
                        hasNonEmptyNullRow = true;
                }
                else
                {
                    totalSrcValues = checked(totalSrcValues + rowLen);
                }
            }

            if (ReferenceEquals(this, t) && !hasNonEmptyNullRow)
                return;

            int suffixCount = totalTargetValues - oldEnd;
            int newEnd = checked(baseValueOffset + totalSrcValues);
            int delta = checked(newEnd - oldEnd);
            int newTotalValues = checked(totalTargetValues + delta);

            int[]? nonContiguousSrcIndices = null;
            if (hasNonEmptyNullRow && totalSrcValues > 0)
            {
                nonContiguousSrcIndices = new int[totalSrcValues];
                int writePos = 0;
                for (int i = 0; i < Length; i++)
                {
                    if (ValidityMask.IsNull(i)) continue;
                    int rowStart = srcOffsets[i];
                    int rowEnd = srcOffsets[i + 1];
                    for (int j = rowStart; j < rowEnd; j++)
                        nonContiguousSrcIndices[writePos++] = j;
                }
            }

            // Capture source row lengths before mutating offsets in case of self-copy with non-empty null rows.
            int[]? selfRowLengths = null;
            bool[]? selfRowNulls = null;
            if (ReferenceEquals(this, t))
            {
                selfRowLengths = new int[Length];
                selfRowNulls = new bool[Length];
                for (int i = 0; i < Length; i++)
                {
                    selfRowNulls[i] = ValidityMask.IsNull(i);
                    selfRowLengths[i] = selfRowNulls[i] ? 0 : srcOffsets[i + 1] - srcOffsets[i];
                }
            }

            // Phase 1: Update/splice t.Values BEFORE mutating t.Offsets or t.ValidityMask (exception-safe atomicity).
            if (totalSrcValues > 0 || (suffixCount > 0 && delta != 0))
            {
                if (Values is Utf8StringSeries srcUtf8)
                {
                    CopyUtf8Values(srcUtf8, t, baseValueOffset, oldEnd, suffixCount, srcValueStart, totalSrcValues, nonContiguousSrcIndices, newTotalValues);
                }
                else if (Values is BinarySeries srcBin)
                {
                    CopyBinaryValues(srcBin, t, baseValueOffset, oldEnd, suffixCount, srcValueStart, totalSrcValues, nonContiguousSrcIndices, newTotalValues);
                }
                else
                {
                    CopyGenericValues(t, baseValueOffset, oldEnd, suffixCount, srcValueStart, totalSrcValues, delta, nonContiguousSrcIndices, newTotalValues);
                }
            }

            // Phase 2: Update t.Offsets and t.ValidityMask now that t.Values succeeded.
            if (offset == 0)
                targetOffsets[0] = 0;

            int currentTargetOffset = baseValueOffset;
            if (selfRowLengths != null && selfRowNulls != null)
            {
                for (int i = 0; i < Length; i++)
                {
                    if (selfRowNulls[i])
                    {
                        t.ValidityMask.SetNull(offset + i);
                        targetOffsets[offset + i + 1] = currentTargetOffset;
                    }
                    else
                    {
                        t.ValidityMask.SetValid(offset + i);
                        currentTargetOffset = checked(currentTargetOffset + selfRowLengths[i]);
                        targetOffsets[offset + i + 1] = currentTargetOffset;
                    }
                }
            }
            else
            {
                for (int i = 0; i < Length; i++)
                {
                    if (ValidityMask.IsNull(i))
                    {
                        t.ValidityMask.SetNull(offset + i);
                        targetOffsets[offset + i + 1] = currentTargetOffset;
                    }
                    else
                    {
                        t.ValidityMask.SetValid(offset + i);
                        currentTargetOffset = checked(currentTargetOffset + (srcOffsets[i + 1] - srcOffsets[i]));
                        targetOffsets[offset + i + 1] = currentTargetOffset;
                    }
                }
            }

            if (delta != 0)
            {
                for (int k = offset + Length + 1; k <= t.Length; k++)
                {
                    targetOffsets[k] = checked(targetOffsets[k] + delta);
                }
            }
        }

        internal static bool AreValuesCompatible(ISeries src, ISeries dst)
        {
            if (src.GetType() != dst.GetType())
                return false;
            if (src is ListSeries sl && dst is ListSeries dl)
                return AreValuesCompatible(sl.Values, dl.Values);
            if (src is ArraySeries sa && dst is ArraySeries da)
                return sa.Width == da.Width && AreValuesCompatible(sa.Values, da.Values);
            if (src is StructSeries ss && dst is StructSeries ds)
            {
                if (ss.Fields.Length != ds.Fields.Length)
                    return false;
                for (int i = 0; i < ss.Fields.Length; i++)
                {
                    if (!AreValuesCompatible(ss.Fields[i], ds.Fields[i]))
                        return false;
                }
                return true;
            }
            if (src is EnumSeries se && dst is EnumSeries de)
                return System.Linq.Enumerable.SequenceEqual(se.Categories, de.Categories);
            if (src is CategoricalSeries sc && dst is CategoricalSeries dc)
                return System.Linq.Enumerable.SequenceEqual(sc.RevMap, dc.RevMap);
            return true;
        }

        private void CopyGenericValues(
            ListSeries t,
            int baseValueOffset,
            int oldEnd,
            int suffixCount,
            int srcValueStart,
            int totalSrcValues,
            int delta,
            int[]? nonContiguousSrcIndices,
            int newTotalValues)
        {
            // Fast-path in-place copy when target has capacity, no suffix shift is needed, and source values are contiguous & full-length.
            if (!ReferenceEquals(Values, t.Values) &&
                totalSrcValues > 0 &&
                (suffixCount == 0 || (delta == 0 && Values is not ListSeries)) &&
                nonContiguousSrcIndices == null &&
                srcValueStart == 0 &&
                Values.Length == totalSrcValues &&
                t.Values.Length >= newTotalValues)
            {
                Values.CopyTo(t.Values, baseValueOffset);
                return;
            }

            ISeries newValues = t.Values.CloneEmpty(newTotalValues);
            try
            {
                if (baseValueOffset > 0)
                {
                    int[] prefixIndices = new int[baseValueOffset];
                    for (int i = 0; i < baseValueOffset; i++)
                        prefixIndices[i] = i;

                    using ISeries prefixCopy = t.Values.CloneEmpty(baseValueOffset);
                    t.Values.Take(prefixCopy, prefixIndices);
                    prefixCopy.CopyTo(newValues, 0);
                }

                if (totalSrcValues > 0)
                {
                    int[] srcIndices;
                    if (nonContiguousSrcIndices != null)
                    {
                        srcIndices = nonContiguousSrcIndices;
                    }
                    else
                    {
                        srcIndices = new int[totalSrcValues];
                        for (int i = 0; i < totalSrcValues; i++)
                            srcIndices[i] = srcValueStart + i;
                    }

                    if (baseValueOffset == 0 && suffixCount == 0)
                    {
                        Values.Take(newValues, srcIndices);
                    }
                    else
                    {
                        using ISeries srcCopy = Values.CloneEmpty(totalSrcValues);
                        Values.Take(srcCopy, srcIndices);
                        srcCopy.CopyTo(newValues, baseValueOffset);
                    }
                }

                if (suffixCount > 0)
                {
                    int[] suffixIndices = new int[suffixCount];
                    for (int i = 0; i < suffixCount; i++)
                        suffixIndices[i] = oldEnd + i;

                    using ISeries suffixCopy = t.Values.CloneEmpty(suffixCount);
                    t.Values.Take(suffixCopy, suffixIndices);
                    suffixCopy.CopyTo(newValues, checked(baseValueOffset + totalSrcValues));
                }
            }
            catch
            {
                newValues.Dispose();
                throw;
            }

            ISeries oldValues = t.Values;
            t.Values = newValues;
            oldValues.Dispose();
        }

        private static void CopyUtf8Values(
            Utf8StringSeries src,
            ListSeries target,
            int baseValueOffset,
            int oldEnd,
            int suffixCount,
            int srcValueStart,
            int totalSrcValues,
            int[]? nonContiguousSrcIndices,
            int newTotalValues)
        {
            if (target.Values is not Utf8StringSeries dst)
                throw new InvalidOperationException($"Target values series type mismatch: expected Utf8StringSeries, got {target.Values.GetType().Name}.");

            var replacement = SpliceUtf8Series(
                src,
                dst,
                baseValueOffset,
                oldEnd,
                suffixCount,
                srcValueStart,
                totalSrcValues,
                nonContiguousSrcIndices,
                newTotalValues,
                allowInPlace: true);

            if (replacement != null)
            {
                ISeries oldValues = target.Values;
                target.Values = replacement;
                oldValues.Dispose();
            }
        }

        internal static Utf8StringSeries? SpliceUtf8Series(
            Utf8StringSeries src,
            Utf8StringSeries dst,
            int baseValueOffset,
            int oldEnd,
            int suffixCount,
            int srcValueStart,
            int totalSrcValues,
            int[]? nonContiguousSrcIndices,
            int newTotalValues,
            bool allowInPlace = true)
        {
            if (baseValueOffset < 0 || oldEnd < baseValueOffset || suffixCount < 0 || baseValueOffset > dst.Length || oldEnd > dst.Length || checked(oldEnd + suffixCount) > dst.Length)
                throw new InvalidOperationException($"Target value range [{baseValueOffset}..{oldEnd}) with suffix {suffixCount} exceeds target values length {dst.Length}.");

            int srcBytes = 0;
            for (int i = 0; i < totalSrcValues; i++)
            {
                int idx = nonContiguousSrcIndices != null ? nonContiguousSrcIndices[i] : srcValueStart + i;
                if (!src.ValidityMask.IsNull(idx))
                    srcBytes = checked(srcBytes + src.GetStringSpan(idx).Length);
            }

            var dstOffsets = dst.Offsets.Span;
            int existingPrefixEndByte = baseValueOffset > 0 ? dstOffsets[baseValueOffset] : 0;
            int existingEndByte = dstOffsets[dst.Length];

            // Fast-path in-place copy only when there are no suffix elements and target has element + byte capacity
            if (allowInPlace &&
                suffixCount == 0 &&
                !ReferenceEquals(src, dst) &&
                dst.Length >= newTotalValues &&
                existingEndByte == existingPrefixEndByte &&
                checked(existingPrefixEndByte + srcBytes) <= dst.DataBytes.Length)
            {
                var dstData = dst.DataBytes.Span;
                int curByte = existingPrefixEndByte;
                for (int i = 0; i < totalSrcValues; i++)
                {
                    int srcIdx = nonContiguousSrcIndices != null ? nonContiguousSrcIndices[i] : srcValueStart + i;
                    int dstIdx = baseValueOffset + i;
                    dstOffsets[dstIdx] = curByte;
                    if (src.ValidityMask.IsNull(srcIdx))
                    {
                        dst.ValidityMask.SetNull(dstIdx);
                    }
                    else
                    {
                        dst.ValidityMask.SetValid(dstIdx);
                        var span = src.GetStringSpan(srcIdx);
                        span.CopyTo(dstData.Slice(curByte));
                        curByte += span.Length;
                    }
                }
                for (int k = newTotalValues; k <= dst.Length; k++)
                {
                    dstOffsets[k] = curByte;
                }
                return null;
            }

            int prefixBytes = 0;
            for (int i = 0; i < baseValueOffset; i++)
            {
                if (!dst.ValidityMask.IsNull(i))
                    prefixBytes = checked(prefixBytes + dst.GetStringSpan(i).Length);
            }

            int suffixBytes = 0;
            for (int i = 0; i < suffixCount; i++)
            {
                int idx = oldEnd + i;
                if (!dst.ValidityMask.IsNull(idx))
                    suffixBytes = checked(suffixBytes + dst.GetStringSpan(idx).Length);
            }

            int totalBytes = checked(prefixBytes + srcBytes + suffixBytes);
            var newValues = new Utf8StringSeries(dst.Name, newTotalValues, totalBytes);
            try
            {
                var newOffsets = newValues.Offsets.Span;
                var newData = newValues.DataBytes.Span;
                int writeByte = 0;

                for (int i = 0; i < baseValueOffset; i++)
                {
                    newOffsets[i] = writeByte;
                    if (dst.ValidityMask.IsNull(i))
                    {
                        newValues.ValidityMask.SetNull(i);
                    }
                    else
                    {
                        newValues.ValidityMask.SetValid(i);
                        var span = dst.GetStringSpan(i);
                        span.CopyTo(newData.Slice(writeByte));
                        writeByte += span.Length;
                    }
                }

                for (int i = 0; i < totalSrcValues; i++)
                {
                    int srcIdx = nonContiguousSrcIndices != null ? nonContiguousSrcIndices[i] : srcValueStart + i;
                    int dstIdx = baseValueOffset + i;
                    newOffsets[dstIdx] = writeByte;
                    if (src.ValidityMask.IsNull(srcIdx))
                    {
                        newValues.ValidityMask.SetNull(dstIdx);
                    }
                    else
                    {
                        newValues.ValidityMask.SetValid(dstIdx);
                        var span = src.GetStringSpan(srcIdx);
                        span.CopyTo(newData.Slice(writeByte));
                        writeByte += span.Length;
                    }
                }

                for (int i = 0; i < suffixCount; i++)
                {
                    int srcIdx = oldEnd + i;
                    int dstIdx = baseValueOffset + totalSrcValues + i;
                    newOffsets[dstIdx] = writeByte;
                    if (dst.ValidityMask.IsNull(srcIdx))
                    {
                        newValues.ValidityMask.SetNull(dstIdx);
                    }
                    else
                    {
                        newValues.ValidityMask.SetValid(dstIdx);
                        var span = dst.GetStringSpan(srcIdx);
                        span.CopyTo(newData.Slice(writeByte));
                        writeByte += span.Length;
                    }
                }

                newOffsets[newTotalValues] = writeByte;
                return newValues;
            }
            catch
            {
                newValues.Dispose();
                throw;
            }
        }

        private static void CopyBinaryValues(
            BinarySeries src,
            ListSeries target,
            int baseValueOffset,
            int oldEnd,
            int suffixCount,
            int srcValueStart,
            int totalSrcValues,
            int[]? nonContiguousSrcIndices,
            int newTotalValues)
        {
            if (target.Values is not BinarySeries dst)
                throw new InvalidOperationException($"Target values series type mismatch: expected BinarySeries, got {target.Values.GetType().Name}.");

            var replacement = SpliceBinarySeries(
                src,
                dst,
                baseValueOffset,
                oldEnd,
                suffixCount,
                srcValueStart,
                totalSrcValues,
                nonContiguousSrcIndices,
                newTotalValues,
                allowInPlace: true);

            if (replacement != null)
            {
                ISeries oldValues = target.Values;
                target.Values = replacement;
                oldValues.Dispose();
            }
        }

        internal static BinarySeries? SpliceBinarySeries(
            BinarySeries src,
            BinarySeries dst,
            int baseValueOffset,
            int oldEnd,
            int suffixCount,
            int srcValueStart,
            int totalSrcValues,
            int[]? nonContiguousSrcIndices,
            int newTotalValues,
            bool allowInPlace = true)
        {
            if (baseValueOffset < 0 || oldEnd < baseValueOffset || suffixCount < 0 || baseValueOffset > dst.Length || oldEnd > dst.Length || checked(oldEnd + suffixCount) > dst.Length)
                throw new InvalidOperationException($"Target value range [{baseValueOffset}..{oldEnd}) with suffix {suffixCount} exceeds target values length {dst.Length}.");

            int srcBytes = 0;
            for (int i = 0; i < totalSrcValues; i++)
            {
                int idx = nonContiguousSrcIndices != null ? nonContiguousSrcIndices[i] : srcValueStart + i;
                if (!src.ValidityMask.IsNull(idx))
                    srcBytes = checked(srcBytes + src.GetSpan(idx).Length);
            }

            var dstOffsets = dst.Offsets.Span;
            int existingPrefixEndByte = baseValueOffset > 0 ? dstOffsets[baseValueOffset] : 0;
            int existingEndByte = dstOffsets[dst.Length];

            if (allowInPlace &&
                suffixCount == 0 &&
                !ReferenceEquals(src, dst) &&
                dst.Length >= newTotalValues &&
                existingEndByte == existingPrefixEndByte &&
                checked(existingPrefixEndByte + srcBytes) <= dst.DataBytes.Length)
            {
                var dstData = dst.DataBytes.Span;
                int curByte = existingPrefixEndByte;
                for (int i = 0; i < totalSrcValues; i++)
                {
                    int srcIdx = nonContiguousSrcIndices != null ? nonContiguousSrcIndices[i] : srcValueStart + i;
                    int dstIdx = baseValueOffset + i;
                    dstOffsets[dstIdx] = curByte;
                    if (src.ValidityMask.IsNull(srcIdx))
                    {
                        dst.ValidityMask.SetNull(dstIdx);
                    }
                    else
                    {
                        dst.ValidityMask.SetValid(dstIdx);
                        var span = src.GetSpan(srcIdx);
                        span.CopyTo(dstData.Slice(curByte));
                        curByte += span.Length;
                    }
                }
                for (int k = newTotalValues; k <= dst.Length; k++)
                {
                    dstOffsets[k] = curByte;
                }
                return null;
            }

            int prefixBytes = 0;
            for (int i = 0; i < baseValueOffset; i++)
            {
                if (!dst.ValidityMask.IsNull(i))
                    prefixBytes = checked(prefixBytes + dst.GetSpan(i).Length);
            }

            int suffixBytes = 0;
            for (int i = 0; i < suffixCount; i++)
            {
                int idx = oldEnd + i;
                if (!dst.ValidityMask.IsNull(idx))
                    suffixBytes = checked(suffixBytes + dst.GetSpan(idx).Length);
            }

            int totalBytes = checked(prefixBytes + srcBytes + suffixBytes);
            var newValues = new BinarySeries(dst.Name, newTotalValues, totalBytes);
            try
            {
                var newOffsets = newValues.Offsets.Span;
                var newData = newValues.DataBytes.Span;
                int writeByte = 0;

                for (int i = 0; i < baseValueOffset; i++)
                {
                    newOffsets[i] = writeByte;
                    if (dst.ValidityMask.IsNull(i))
                    {
                        newValues.ValidityMask.SetNull(i);
                    }
                    else
                    {
                        newValues.ValidityMask.SetValid(i);
                        var span = dst.GetSpan(i);
                        span.CopyTo(newData.Slice(writeByte));
                        writeByte += span.Length;
                    }
                }

                for (int i = 0; i < totalSrcValues; i++)
                {
                    int srcIdx = nonContiguousSrcIndices != null ? nonContiguousSrcIndices[i] : srcValueStart + i;
                    int dstIdx = baseValueOffset + i;
                    newOffsets[dstIdx] = writeByte;
                    if (src.ValidityMask.IsNull(srcIdx))
                    {
                        newValues.ValidityMask.SetNull(dstIdx);
                    }
                    else
                    {
                        newValues.ValidityMask.SetValid(dstIdx);
                        var span = src.GetSpan(srcIdx);
                        span.CopyTo(newData.Slice(writeByte));
                        writeByte += span.Length;
                    }
                }

                for (int i = 0; i < suffixCount; i++)
                {
                    int srcIdx = oldEnd + i;
                    int dstIdx = baseValueOffset + totalSrcValues + i;
                    newOffsets[dstIdx] = writeByte;
                    if (dst.ValidityMask.IsNull(srcIdx))
                    {
                        newValues.ValidityMask.SetNull(dstIdx);
                    }
                    else
                    {
                        newValues.ValidityMask.SetValid(dstIdx);
                        var span = dst.GetSpan(srcIdx);
                        span.CopyTo(newData.Slice(writeByte));
                        writeByte += span.Length;
                    }
                }

                newOffsets[newTotalValues] = writeByte;
                return newValues;
            }
            catch
            {
                newValues.Dispose();
                throw;
            }
        }

        internal static Utf8StringSeries? TakeUtf8Into(Utf8StringSeries src, Utf8StringSeries dst, ReadOnlySpan<int> indices)
        {
            int totalBytes = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                int idx = indices[i];
                if (idx != -1 && !src.ValidityMask.IsNull(idx))
                    totalBytes = checked(totalBytes + src.GetStringSpan(idx).Length);
            }

            if (!ReferenceEquals(src, dst) && dst.Length >= indices.Length && dst.DataBytes.Length >= totalBytes)
            {
                src.Take(dst, indices);
                return null;
            }

            var newDst = new Utf8StringSeries(dst.Name, indices.Length, totalBytes);
            try
            {
                src.Take(newDst, indices);
                return newDst;
            }
            catch
            {
                newDst.Dispose();
                throw;
            }
        }

        internal static BinarySeries? TakeBinaryInto(BinarySeries src, BinarySeries dst, ReadOnlySpan<int> indices)
        {
            int totalBytes = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                int idx = indices[i];
                if (idx != -1 && !src.ValidityMask.IsNull(idx))
                    totalBytes = checked(totalBytes + src.GetSpan(idx).Length);
            }

            if (!ReferenceEquals(src, dst) && dst.Length >= indices.Length && dst.DataBytes.Length >= totalBytes)
            {
                src.Take(dst, indices);
                return null;
            }

            var newDst = new BinarySeries(dst.Name, indices.Length, totalBytes);
            try
            {
                src.Take(newDst, indices);
                return newDst;
            }
            catch
            {
                newDst.Dispose();
                throw;
            }
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is ListSeries t)
            {
                var srcOffsets = Offsets.Memory.Span;
                var targetOffsets = t.Offsets.Memory.Span;

                int totalValues = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    int row = indices[i];
                    if (row >= 0 && !ValidityMask.IsNull(row))
                    {
                        totalValues += srcOffsets[row + 1] - srcOffsets[row];
                    }
                }

                int[] allValueIndices = new int[totalValues];
                int currentTargetOffset = 0;
                targetOffsets[0] = 0;

                for (int i = 0; i < indices.Length; i++)
                {
                    int row = indices[i];
                    if (row < 0 || ValidityMask.IsNull(row))
                    {
                        t.ValidityMask.SetNull(i);
                        targetOffsets[i + 1] = currentTargetOffset;
                        continue;
                    }

                    int start = srcOffsets[row];
                    int len = srcOffsets[row + 1] - start;
                    for (int j = 0; j < len; j++)
                    {
                        allValueIndices[currentTargetOffset + j] = start + j;
                    }

                    currentTargetOffset += len;
                    targetOffsets[i + 1] = currentTargetOffset;
                    t.ValidityMask.SetValid(i);
                }

                ISeries newValues;
                if (Values is Utf8StringSeries u8)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < allValueIndices.Length; i++)
                    {
                        if (!u8.ValidityMask.IsNull(allValueIndices[i]))
                            totalBytes += u8.GetStringSpan(allValueIndices[i]).Length;
                    }
                    newValues = new Utf8StringSeries(Values.Name, totalValues, totalBytes);
                }
                else if (Values is BinarySeries bin)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < allValueIndices.Length; i++)
                    {
                        if (!bin.ValidityMask.IsNull(allValueIndices[i]))
                            totalBytes += bin.GetSpan(allValueIndices[i]).Length;
                    }
                    newValues = new BinarySeries(Values.Name, totalValues, totalBytes);
                }
                else
                {
                    newValues = Values.CloneEmpty(totalValues);
                }

                try
                {
                    if (totalValues > 0)
                    {
                        Values.Take(newValues, allValueIndices);
                    }
                }
                catch
                {
                    newValues.Dispose();
                    throw;
                }

                t.Values.Dispose();
                t.Values = newValues;
            }
            else throw new ArgumentException("Target series type mismatch.");
        }

        public ISeries Explode()
        {
            // Simply returns the underlying flat values.
            // The engine must handle row repetition for other columns.
            return Values;
        }
        public void Dispose()
        {
            Offsets.Dispose();
            Values.Dispose();
        }
        public object? Get(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            var start = Offsets.Memory.Span[i];
            var end = Offsets.Memory.Span[i + 1];
            var len = end - start;
            var result = new List<object?>(len);
            for (int j = start; j < end; j++)
            {
                result.Add(Values.Get(j));
            }
            return result.ToArray();
        }
        public ISeries CloneEmpty(int length)
        {
            var emptyOffsets = new Int32Series(Offsets.Name, length + 1);
            var emptyValues = Values.CloneEmpty(0); // Start with 0 values
            return new ListSeries(Name, emptyOffsets, emptyValues);
        }

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            throw new NotSupportedException("ListSeries.Take(int,int) is not supported.");
        }

        public ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Binary, isNullable: ValidityMask.HasNulls);
            var offsetBytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(Offsets.Memory.Span[..(Length + 1)]).ToArray();
            return new ArrowColumn(field, Length, ValidityMask.NullCount, ValidityMask.GetNullBitmapMemory(), offsetBytes, Values.ToArrowColumn().DataBuffer);
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);
    }
}
