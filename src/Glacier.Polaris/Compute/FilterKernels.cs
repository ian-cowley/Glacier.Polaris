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

    /// <summary>
    /// Implements high-performance SIMD vectorized and multi-threaded filter operations.
    /// Supports sbyte, byte, short, ushort, int, uint, long, ulong, float, and double.
    /// </summary>
    public static class FilterKernels
    {
        public static (int[] rentedIndices, int count) Filter<T>(ReadOnlySpan<T> data, T threshold, FilterOperation op) where T : unmanaged, IComparable<T>
        {
            if (typeof(T) == typeof(int) || typeof(T) == typeof(double) ||
                typeof(T) == typeof(float) || typeof(T) == typeof(long) ||
                typeof(T) == typeof(ulong) || typeof(T) == typeof(uint) ||
                typeof(T) == typeof(short) || typeof(T) == typeof(ushort) ||
                typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte))
            {
                return FilterGeneric<T>(data, threshold, op);
            }

            // Scalar fallback for other non-numeric unmanaged types (if any)
            int[] indices = System.Buffers.ArrayPool<int>.Shared.Rent(data.Length);
            int count = 0;
            for (int i = 0; i < data.Length; i++)
            {
                if (Matches(data[i], threshold, op))
                {
                    indices[count++] = i;
                }
            }
            return (indices, count);
        }

        private static unsafe (int[] rentedIndices, int count) FilterGeneric<T>(ReadOnlySpan<T> data, T threshold, FilterOperation op) where T : unmanaged, IComparable<T>
        {
            int parallelThreshold = ParallelThresholds.GetFilterParallelThreshold<T>();

            if (data.Length >= parallelThreshold)
            {
                int numThreads = Environment.ProcessorCount;
                int chunkSize = (data.Length + numThreads - 1) / numThreads;
                int[] counts = new int[numThreads];
                int dataLen = data.Length;

                fixed (T* pData = data)
                {
                    nint ptrData = (nint)pData;

                    System.Threading.Tasks.Parallel.For(0, numThreads, p =>
                    {
                        T* localData = (T*)ptrData;
                        int start = p * chunkSize;
                        int end = Math.Min(start + chunkSize, dataLen);
                        if (start >= end) return;

                        int localCount = 0;
                        int i = start;

                        if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<T>.Count)
                        {
                            int step = Vector256<T>.Count;
                            var vThreshold = Vector256.Create(threshold);
                            for (; i <= end - step; i += step)
                            {
                                var vData = Vector256.Load(localData + i);
                                Vector256<T> vMask = op switch
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
                            if (Matches(localData[i], threshold, op)) localCount++;
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
                        T* localData = (T*)ptrData;
                        int start = p * chunkSize;
                        int end = Math.Min(start + chunkSize, dataLen);
                        if (start >= end) return;

                        int destOffset = offsets[p];
                        int i = start;

                        if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<T>.Count)
                        {
                            int step = Vector256<T>.Count;
                            var vThreshold = Vector256.Create(threshold);
                            for (; i <= end - step; i += step)
                            {
                                var vData = Vector256.Load(localData + i);
                                Vector256<T> vMask = op switch
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
                            if (Matches(localData[i], threshold, op)) pIndices[destOffset++] = i;
                        }
                    });

                    return (pIndices, totalCount);
                }
            }

            int[] indices = System.Buffers.ArrayPool<int>.Shared.Rent(data.Length);
            int count = 0;
            int idx = 0;

            if (Vector256.IsHardwareAccelerated && data.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                var vThreshold = Vector256.Create(threshold);
                for (; idx <= data.Length - step; idx += step)
                {
                    var vData = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(data.Slice(idx)));
                    Vector256<T> vMask = op switch
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
                if (Matches(data[idx], threshold, op)) indices[count++] = idx;
            }
            return (indices, count);
        }

        private static bool Matches<T>(T val, T threshold, FilterOperation op) where T : IComparable<T>
        {
            int cmp = val.CompareTo(threshold);
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
