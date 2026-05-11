using System;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public enum FilterOperation
    {
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual
    }

    public static class FilterKernels
    {
        public static (int[] rentedIndices, int count) Filter<T>(ReadOnlySpan<T> data, T threshold, FilterOperation op) where T : unmanaged, IComparable<T>
        {
            int[] indices = System.Buffers.ArrayPool<int>.Shared.Rent(data.Length);
            int count = 0;
            int i = 0;

            // SIMD is harder to make generic for all types because Vector256.GreaterThan is not on INumber but on specific types.
            // For now, let's use the optimized paths for Int32 and Float64, and scalar for others.

            if (typeof(T) == typeof(int))
                return FilterInt32(MemoryMarshal.Cast<T, int>(data), (int)(object)threshold, op);
            if (typeof(T) == typeof(double))
                return FilterFloat64(MemoryMarshal.Cast<T, double>(data), (double)(object)threshold, op);

            // Scalar fallback for other types
            for (; i < data.Length; i++)
            {
                int cmp = data[i].CompareTo(threshold);
                bool match = op switch
                {
                    FilterOperation.Equal => cmp == 0,
                    FilterOperation.NotEqual => cmp != 0,
                    FilterOperation.GreaterThan => cmp > 0,
                    FilterOperation.GreaterThanOrEqual => cmp >= 0,
                    FilterOperation.LessThan => cmp < 0,
                    FilterOperation.LessThanOrEqual => cmp <= 0,
                    _ => false
                };
                if (match) indices[count++] = i;
            }

            return (indices, count);
        }
        private static unsafe (int[] rentedIndices, int count) FilterInt32(ReadOnlySpan<int> data, int threshold, FilterOperation op)
        {
            if (data.Length >= 256 * 1024)
            {
                int numThreads = Environment.ProcessorCount;
                int chunkSize = (data.Length + numThreads - 1) / numThreads;
                int[] counts = new int[numThreads];
                int dataLen = data.Length;

                fixed (int* pData = data)
                {
                    nint ptrData = (nint)pData;

                    System.Threading.Tasks.Parallel.For(0, numThreads, p =>
                    {
                        int* localData = (int*)ptrData;
                        int start = p * chunkSize;
                        int end = Math.Min(start + chunkSize, dataLen);
                        if (start >= end) return;

                        int localCount = 0;
                        int i = start;

                        if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<int>.Count)
                        {
                            int step = Vector256<int>.Count;
                            var vThreshold = Vector256.Create(threshold);
                            for (; i <= end - step; i += step)
                            {
                                var vData = Vector256.Load(localData + i);
                                Vector256<int> vMask = op switch
                                {
                                    FilterOperation.Equal => Vector256.Equals(vData, vThreshold),
                                    FilterOperation.NotEqual => ~Vector256.Equals(vData, vThreshold),
                                    FilterOperation.GreaterThan => Vector256.GreaterThan(vData, vThreshold),
                                    FilterOperation.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(vData, vThreshold),
                                    FilterOperation.LessThan => Vector256.LessThan(vData, vThreshold),
                                    FilterOperation.LessThanOrEqual => Vector256.LessThanOrEqual(vData, vThreshold),
                                    _ => throw new NotImplementedException()
                                };
                                localCount += BitOperations.PopCount(vMask.ExtractMostSignificantBits());
                            }
                        }
                        for (; i < end; i++)
                        {
                            if (Matches(localData[i].CompareTo(threshold), op)) localCount++;
                        }
                        counts[p] = localCount;
                    });

                    int totalCount = 0;
                    int[] offsets = new int[numThreads];
                    for (int p = 0; p < numThreads; p++)
                    {
                        offsets[p] = totalCount;
                        totalCount += counts[p];
                    }

                    int[] pIndices = System.Buffers.ArrayPool<int>.Shared.Rent(totalCount > 0 ? totalCount : 1);

                    System.Threading.Tasks.Parallel.For(0, numThreads, p =>
                    {
                        int* localData = (int*)ptrData;
                        int start = p * chunkSize;
                        int end = Math.Min(start + chunkSize, dataLen);
                        if (start >= end) return;

                        int destOffset = offsets[p];
                        int i = start;

                        if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<int>.Count)
                        {
                            int step = Vector256<int>.Count;
                            var vThreshold = Vector256.Create(threshold);
                            for (; i <= end - step; i += step)
                            {
                                var vData = Vector256.Load(localData + i);
                                Vector256<int> vMask = op switch
                                {
                                    FilterOperation.Equal => Vector256.Equals(vData, vThreshold),
                                    FilterOperation.NotEqual => ~Vector256.Equals(vData, vThreshold),
                                    FilterOperation.GreaterThan => Vector256.GreaterThan(vData, vThreshold),
                                    FilterOperation.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(vData, vThreshold),
                                    FilterOperation.LessThan => Vector256.LessThan(vData, vThreshold),
                                    FilterOperation.LessThanOrEqual => Vector256.LessThanOrEqual(vData, vThreshold),
                                    _ => throw new NotImplementedException()
                                };
                                uint mask = vMask.ExtractMostSignificantBits();
                                while (mask != 0)
                                {
                                    int bit = BitOperations.TrailingZeroCount(mask);
                                    pIndices[destOffset++] = i + bit;
                                    mask &= (mask - 1);
                                }
                            }
                        }
                        for (; i < end; i++)
                        {
                            if (Matches(localData[i].CompareTo(threshold), op)) pIndices[destOffset++] = i;
                        }
                    });

                    return (pIndices, totalCount);
                }
            }

            int[] indices = System.Buffers.ArrayPool<int>.Shared.Rent(data.Length);
            int count = 0;
            int idx = 0;

            if (Vector256.IsHardwareAccelerated && data.Length >= Vector256<int>.Count)
            {
                int step = Vector256<int>.Count;
                var vThreshold = Vector256.Create(threshold);
                for (; idx <= data.Length - step; idx += step)
                {
                    var vData = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(data.Slice(idx)));
                    Vector256<int> vMask = op switch
                    {
                        FilterOperation.Equal => Vector256.Equals(vData, vThreshold),
                        FilterOperation.NotEqual => ~Vector256.Equals(vData, vThreshold),
                        FilterOperation.GreaterThan => Vector256.GreaterThan(vData, vThreshold),
                        FilterOperation.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(vData, vThreshold),
                        FilterOperation.LessThan => Vector256.LessThan(vData, vThreshold),
                        FilterOperation.LessThanOrEqual => Vector256.LessThanOrEqual(vData, vThreshold),
                        _ => throw new NotImplementedException()
                    };
                    uint mask = vMask.ExtractMostSignificantBits();
                    while (mask != 0)
                    {
                        int bit = BitOperations.TrailingZeroCount(mask);
                        indices[count++] = idx + bit;
                        mask &= (mask - 1);
                    }
                }
            }
            for (; idx < data.Length; idx++)
            {
                int cmp = data[idx].CompareTo(threshold);
                if (Matches(cmp, op)) indices[count++] = idx;
            }
            return (indices, count);
        }
        private static unsafe (int[] rentedIndices, int count) FilterFloat64(ReadOnlySpan<double> data, double threshold, FilterOperation op)
        {
            if (data.Length >= 256 * 1024)
            {
                int numThreads = Environment.ProcessorCount;
                int chunkSize = (data.Length + numThreads - 1) / numThreads;
                int[] counts = new int[numThreads];
                int dataLen = data.Length;

                fixed (double* pData = data)
                {
                    nint ptrData = (nint)pData;

                    System.Threading.Tasks.Parallel.For(0, numThreads, p =>
                    {
                        double* localData = (double*)ptrData;
                        int start = p * chunkSize;
                        int end = Math.Min(start + chunkSize, dataLen);
                        if (start >= end) return;

                        int localCount = 0;
                        int i = start;

                        if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<double>.Count)
                        {
                            int step = Vector256<double>.Count;
                            var vThreshold = Vector256.Create(threshold);
                            for (; i <= end - step; i += step)
                            {
                                var vData = Vector256.Load(localData + i);
                                Vector256<double> vMask = op switch
                                {
                                    FilterOperation.Equal => Vector256.Equals(vData, vThreshold),
                                    FilterOperation.NotEqual => ~Vector256.Equals(vData, vThreshold),
                                    FilterOperation.GreaterThan => Vector256.GreaterThan(vData, vThreshold),
                                    FilterOperation.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(vData, vThreshold),
                                    FilterOperation.LessThan => Vector256.LessThan(vData, vThreshold),
                                    FilterOperation.LessThanOrEqual => Vector256.LessThanOrEqual(vData, vThreshold),
                                    _ => throw new NotImplementedException()
                                };
                                localCount += BitOperations.PopCount(vMask.ExtractMostSignificantBits());
                            }
                        }
                        for (; i < end; i++)
                        {
                            if (Matches(localData[i].CompareTo(threshold), op)) localCount++;
                        }
                        counts[p] = localCount;
                    });

                    int totalCount = 0;
                    int[] offsets = new int[numThreads];
                    for (int p = 0; p < numThreads; p++)
                    {
                        offsets[p] = totalCount;
                        totalCount += counts[p];
                    }

                    int[] pIndices = System.Buffers.ArrayPool<int>.Shared.Rent(totalCount > 0 ? totalCount : 1);

                    System.Threading.Tasks.Parallel.For(0, numThreads, p =>
                    {
                        double* localData = (double*)ptrData;
                        int start = p * chunkSize;
                        int end = Math.Min(start + chunkSize, dataLen);
                        if (start >= end) return;

                        int destOffset = offsets[p];
                        int i = start;

                        if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<double>.Count)
                        {
                            int step = Vector256<double>.Count;
                            var vThreshold = Vector256.Create(threshold);
                            for (; i <= end - step; i += step)
                            {
                                var vData = Vector256.Load(localData + i);
                                Vector256<double> vMask = op switch
                                {
                                    FilterOperation.Equal => Vector256.Equals(vData, vThreshold),
                                    FilterOperation.NotEqual => ~Vector256.Equals(vData, vThreshold),
                                    FilterOperation.GreaterThan => Vector256.GreaterThan(vData, vThreshold),
                                    FilterOperation.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(vData, vThreshold),
                                    FilterOperation.LessThan => Vector256.LessThan(vData, vThreshold),
                                    FilterOperation.LessThanOrEqual => Vector256.LessThanOrEqual(vData, vThreshold),
                                    _ => throw new NotImplementedException()
                                };
                                uint mask = vMask.ExtractMostSignificantBits();
                                while (mask != 0)
                                {
                                    int bit = BitOperations.TrailingZeroCount(mask);
                                    pIndices[destOffset++] = i + bit;
                                    mask &= (mask - 1);
                                }
                            }
                        }
                        for (; i < end; i++)
                        {
                            if (Matches(localData[i].CompareTo(threshold), op)) pIndices[destOffset++] = i;
                        }
                    });

                    return (pIndices, totalCount);
                }
            }

            int[] indices = System.Buffers.ArrayPool<int>.Shared.Rent(data.Length);
            int count = 0;
            int idx = 0;

            if (Vector256.IsHardwareAccelerated && data.Length >= Vector256<double>.Count)
            {
                int step = Vector256<double>.Count;
                var vThreshold = Vector256.Create(threshold);
                for (; idx <= data.Length - step; idx += step)
                {
                    var vData = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(data.Slice(idx)));
                    Vector256<double> vMask = op switch
                    {
                        FilterOperation.Equal => Vector256.Equals(vData, vThreshold),
                        FilterOperation.NotEqual => ~Vector256.Equals(vData, vThreshold),
                        FilterOperation.GreaterThan => Vector256.GreaterThan(vData, vThreshold),
                        FilterOperation.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(vData, vThreshold),
                        FilterOperation.LessThan => Vector256.LessThan(vData, vThreshold),
                        FilterOperation.LessThanOrEqual => Vector256.LessThanOrEqual(vData, vThreshold),
                        _ => throw new NotImplementedException()
                    };
                    uint mask = vMask.ExtractMostSignificantBits();
                    while (mask != 0)
                    {
                        int bit = BitOperations.TrailingZeroCount(mask);
                        indices[count++] = idx + bit;
                        mask &= (mask - 1);
                    }
                }
            }
            for (; idx < data.Length; idx++)
            {
                int cmp = data[idx].CompareTo(threshold);
                if (Matches(cmp, op)) indices[count++] = idx;
            }
            return (indices, count);
        }
        private static bool Matches(int cmp, FilterOperation op)
        {
            return op switch
            {
                FilterOperation.Equal => cmp == 0,
                FilterOperation.NotEqual => cmp != 0,
                FilterOperation.GreaterThan => cmp > 0,
                FilterOperation.GreaterThanOrEqual => cmp >= 0,
                FilterOperation.LessThan => cmp < 0,
                FilterOperation.LessThanOrEqual => cmp <= 0,
                _ => false
            };
        }
    }
}
