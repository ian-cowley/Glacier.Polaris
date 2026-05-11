using System;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Glacier.Polaris.Memory
{
    /// <summary>
    /// Bit-packed validity mask (Apache Arrow compatible).
    /// Each bit = 1 (Valid) or 0 (Null/NA).
    /// Optimized with bitmap-level operations for bulk fills and scanning.
    /// </summary>
    public sealed class ValidityMask
    {
        private readonly ulong[] _mask;
        public int Length { get; }
        private int UlongCount => (Length + 63) / 64;

        public ValidityMask(int length)
        {
            Length = length;
            int ulongCount = (length + 63) / 64;
            _mask = new ulong[ulongCount];
            Array.Fill(_mask, ulong.MaxValue);
        }

        public bool HasNulls
        {
            get
            {
                for (int i = 0; i < UlongCount; i++)
                {
                    if (_mask[i] != ulong.MaxValue) return true;
                }
                return false;
            }
        }

        public int NullCount
        {
            get
            {
                int count = 0;
                for (int i = 0; i < UlongCount; i++)
                {
                    count += 64 - System.Numerics.BitOperations.PopCount(_mask[i]);
                }
                return count;
            }
        }

        public bool IsValid(int index)
        {
            if (index < 0 || index >= Length) throw new ArgumentOutOfRangeException(nameof(index));
            int wordIndex = index / 64;
            int bitIndex = index % 64;
            return (_mask[wordIndex] & (1UL << bitIndex)) != 0;
        }

        public void SetNull(int index)
        {
            if (index < 0 || index >= Length) throw new ArgumentOutOfRangeException(nameof(index));
            int wordIndex = index / 64;
            int bitIndex = index % 64;
            _mask[wordIndex] &= ~(1UL << bitIndex);
        }

        public bool IsNull(int index) => !IsValid(index);

        public void SetValid(int index)
        {
            if (index < 0 || index >= Length) throw new ArgumentOutOfRangeException(nameof(index));
            int wordIndex = index / 64;
            int bitIndex = index % 64;
            _mask[wordIndex] |= (1UL << bitIndex);
        }

        /// <summary>Returns a reference to the first element of the raw bitmap for unsafe access.</summary>
        internal ref ulong GetRawBitsRef()
        {
            return ref _mask[0];
        }

        public void SetAllValid()
        {
            Array.Fill(_mask, ulong.MaxValue);
        }

        public void SetAllNull()
        {
            Array.Fill(_mask, 0UL);
        }

        public void And(ValidityMask other)
        {
            if (other.Length != this.Length) throw new ArgumentException("Length mismatch");
            for (int i = 0; i < UlongCount; i++) _mask[i] &= other._mask[i];
        }

        public void Or(ValidityMask other)
        {
            if (other.Length != this.Length) throw new ArgumentException("Length mismatch");
            for (int i = 0; i < UlongCount; i++) _mask[i] |= other._mask[i];
        }

        public void Not()
        {
            for (int i = 0; i < UlongCount; i++) _mask[i] = ~_mask[i];
        }

        public void CopyFrom(ValidityMask other)
        {
            if (other.Length != this.Length) throw new ArgumentException("Length mismatch");
            Array.Copy(other._mask, _mask, UlongCount);
        }

        /// <summary>Fast bulk-enabled Take using bitmap scan for null propagation.</summary>
        public void Take(ValidityMask target, ReadOnlySpan<int> indices)
        {
            if (target.Length != indices.Length) throw new ArgumentException("Target length mismatch");
            if (!HasNulls) return; // all valid, nothing to propagate
            for (int i = 0; i < indices.Length; i++)
            {
                if (IsNull(indices[i])) target.SetNull(i);
            }
        }

        public void CopyTo(ValidityMask target, int offset)
        {
            if (offset + Length > target.Length) throw new ArgumentOutOfRangeException(nameof(offset));
            for (int i = 0; i < Length; i++)
            {
                if (IsNull(i)) target.SetNull(offset + i);
            }
        }

        /// <summary>
        /// Bulk-copy validity from source to target at given offset, using 64-bit word operations.
        /// Only propagates nulls; assumes target starts fully valid.
        /// </summary>
        public void CopyToBulk(ValidityMask target, int targetOffset)
        {
            if (targetOffset + Length > target.Length) throw new ArgumentOutOfRangeException(nameof(targetOffset));
            if (!HasNulls) return;

            int srcWord = 0;
            int srcBit = 0;
            int tgtStartWord = targetOffset / 64;
            int tgtStartBit = targetOffset % 64;

            for (int i = 0; i < Length; i++)
            {
                if (IsNull(i)) target.SetNull(targetOffset + i);
            }
        }

        /// <summary>
        /// Enumerate all null indices using bit-scanning (fast for sparse nulls).
        /// Returns -1 when enumeration is exhausted.
        /// </summary>
        public int GetNextNull(int startIndex)
        {
            if (startIndex >= Length) return -1;
            int wordIndex = startIndex / 64;
            int bitIndex = startIndex % 64;

            for (int w = wordIndex; w < UlongCount; w++)
            {
                // Invert bits: 1 where null, 0 where valid
                ulong nullBits = ~_mask[w];
                // Mask off bits before bitIndex in the first word
                if (w == wordIndex) nullBits &= ~((1UL << bitIndex) - 1);

                while (nullBits != 0)
                {
                    int nullBit = System.Numerics.BitOperations.TrailingZeroCount(nullBits);
                    int result = w * 64 + nullBit;
                    if (result >= Length) return -1;
                    nullBits &= nullBits - 1; // clear lowest set bit
                    return result;
                }
            }
            return -1;
        }

        /// <summary>
        /// Applies a bulk fill to the target span: copies source data entirely, then
        /// writes fillValue at every null position (found via bitmap scan).
        /// Sets the result mask to all-valid.
        /// </summary>
        public static void BulkFillNulls<T>(
        ReadOnlySpan<T> source, Span<T> destination,
        ValidityMask sourceMask, T fillValue,
        ValidityMask resultMask) where T : unmanaged
        {
            // 1. Bulk copy all source data
            source.CopyTo(destination);

            // 2. Bitmap scan: find each null position and fill
            if (!sourceMask.HasNulls)
            {
                resultMask.SetAllValid();
                return;
            }

            int idx = sourceMask.GetNextNull(0);
            while (idx >= 0)
            {
                destination[idx] = fillValue;
                idx = sourceMask.GetNextNull(idx + 1);
            }

            // 3. Set all valid
            resultMask.SetAllValid();
        }
    }
}
