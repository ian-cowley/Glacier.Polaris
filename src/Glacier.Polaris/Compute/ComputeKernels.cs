using System;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class ComputeKernels
    {
        /// <summary>
        /// Maps each element of the source series through a function and returns a new series of the specified return type.
        /// </summary>
        public static ISeries MapElements(ISeries source, Func<object?, object?> mapping, Type returnType)
        {
            if (returnType == typeof(int))
            {
                var result = new Int32Series(source.Name, source.Length);
                for (int i = 0; i < source.Length; i++)
                {
                    if (source.ValidityMask.IsNull(i))
                    {
                        result.ValidityMask.SetNull(i);
                    }
                    else
                    {
                        var mapped = mapping(source.Get(i));
                        if (mapped == null) result.ValidityMask.SetNull(i);
                        else result[i] = (int)mapped;
                    }
                }
                return result;
            }
            if (returnType == typeof(double))
            {
                var result = new Float64Series(source.Name, source.Length);
                for (int i = 0; i < source.Length; i++)
                {
                    if (source.ValidityMask.IsNull(i))
                    {
                        result.ValidityMask.SetNull(i);
                    }
                    else
                    {
                        var mapped = mapping(source.Get(i));
                        if (mapped == null) result.ValidityMask.SetNull(i);
                        else result[i] = (double)mapped;
                    }
                }
                return result;
            }
            if (returnType == typeof(string))
            {
                string?[] values = new string?[source.Length];
                for (int i = 0; i < source.Length; i++)
                {
                    if (source.ValidityMask.IsNull(i))
                    {
                        values[i] = null;
                    }
                    else
                    {
                        var mapped = mapping(source.Get(i));
                        values[i] = mapped?.ToString();
                    }
                }
                return Utf8StringSeries.FromStrings(source.Name, values);
            }
            // Fallback: box everything into ObjectSeries
            object?[] objects = new object?[source.Length];
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i))
                {
                    objects[i] = null;
                }
                else
                {
                    objects[i] = mapping(source.Get(i));
                }
            }
            return new ObjectSeries(source.Name, objects);
        }

        /// <summary>
        /// Computes the sum of a Span of ints using AVX-512, AVX2, or scalar fallback.
        /// Demonstrates bounds check elimination and vectorization.
        /// </summary>
        public static long Sum(ReadOnlySpan<int> data)
        {
            long sum = 0;
            int i = 0;

            // AVX-512 Path (Vector512)
            if (Vector512.IsHardwareAccelerated && data.Length >= Vector512<int>.Count)
            {
                Vector512<long> vSum = Vector512<long>.Zero;
                int step = Vector512<int>.Count; // 16 elements

                // The JIT elides bounds checks here because the loop condition proves i + step <= data.Length
                for (; i <= data.Length - step; i += step)
                {
                    Vector512<int> vData = Vector512.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(data.Slice(i)));
                    // Expand and accumulate to prevent overflow
                    var (lower, upper) = Vector512.Widen(vData);
                    vSum = Vector512.Add(vSum, Vector512.Add(lower, upper));
                }

                sum += Vector512.Sum(vSum);
            }
            // AVX2 Path (Vector256)
            else if (Vector256.IsHardwareAccelerated && data.Length >= Vector256<int>.Count)
            {
                Vector256<long> vSum = Vector256<long>.Zero;
                int step = Vector256<int>.Count; // 8 elements

                for (; i <= data.Length - step; i += step)
                {
                    Vector256<int> vData = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(data.Slice(i)));
                    var (lower, upper) = Vector256.Widen(vData);
                    vSum = Vector256.Add(vSum, Vector256.Add(lower, upper));
                }

                sum += Vector256.Sum(vSum);
            }
            // SSE2 Path (Vector128)
            else if (Vector128.IsHardwareAccelerated && data.Length >= Vector128<int>.Count)
            {
                Vector128<long> vSum = Vector128<long>.Zero;
                int step = Vector128<int>.Count;

                for (; i <= data.Length - step; i += step)
                {
                    Vector128<int> vData = Vector128.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(data.Slice(i)));
                    var (lower, upper) = Vector128.Widen(vData);
                    vSum = Vector128.Add(vSum, Vector128.Add(lower, upper));
                }

                sum += Vector128.Sum(vSum);
            }

            // Scalar fallback and remainder
            for (; i < data.Length; i++)
            {
                sum += data[i];
            }

            return sum;
        }

        /// <summary>
        /// Demonstrates Branchless conditional selects for filtering/masking.
        /// </summary>
        public static void BranchlessMask(ReadOnlySpan<int> data, ReadOnlySpan<int> mask, Span<int> result)
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && data.Length >= Vector256<int>.Count && mask.Length >= Vector256<int>.Count && result.Length >= Vector256<int>.Count)
            {
                int step = Vector256<int>.Count;
                for (; i <= data.Length - step; i += step)
                {
                    var vData = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(data.Slice(i)));
                    var vMask = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(mask.Slice(i)));

                    // Creates a condition where vMask == 1
                    var condition = Vector256.Equals(vMask, Vector256<int>.One);

                    // Branchless conditional select: if condition true, take vData, else zero
                    var vResult = Vector256.ConditionalSelect(condition, vData, Vector256<int>.Zero);

                    vResult.StoreUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(result.Slice(i)));
                }
            }

            // Remainder
            for (; i < data.Length; i++)
            {
                result[i] = mask[i] == 1 ? data[i] : 0;
            }
        }

        public static ISeries Take(ISeries source, ReadOnlySpan<int> indices)
        {
            if (source is Int32Series i32)
            {
                var result = new Int32Series(source.Name, indices.Length);
                Take<int>(i32.Memory.Span, indices, result.Memory.Span);
                return result;
            }
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, indices.Length);
                Take<double>(f64.Memory.Span, indices, result.Memory.Span);
                return result;
            }
            if (source is Utf8StringSeries u8)
            {
                // String Take is more complex due to flat byte buffer.
                // For now, simpler implementation:
                string[] data = new string[indices.Length];
                for (int i = 0; i < indices.Length; i++)
                {
                    data[i] = System.Text.Encoding.UTF8.GetString(u8.GetStringSpan(indices[i]));
                }
                return new Utf8StringSeries(source.Name, data);
            }
            throw new NotSupportedException();
        }

        /// <summary>
        /// Highly optimized Take kernel for physical data materialization.
        /// Uses unrolled pointer access to completely bypass bounds checking.
        /// </summary>
        public static unsafe void Take<T>(ReadOnlySpan<T> source, ReadOnlySpan<int> indices, Span<T> destination) where T : unmanaged
        {
            if (indices.Length == 0) return;

            int length = indices.Length;
            
            // Parallelize if large enough (e.g., > 256k rows)
            if (length > 256_000)
            {
                var options = new System.Threading.Tasks.ParallelOptions
                {
                    MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1)
                };

                fixed (T* pSource = source)
                fixed (int* pIndices = indices)
                fixed (T* pDest = destination)
                {
                    T* pSrcLocal = pSource;
                    int* pIdxLocal = pIndices;
                    T* pDstLocal = pDest;

                    System.Threading.Tasks.Parallel.For(0, length / 1024 + 1, options, chunk =>
                    {
                        int start = chunk * 1024;
                        int end = Math.Min(start + 1024, length);
                        
                        int i = start;
                        for (; i <= end - 4; i += 4)
                        {
                            pDstLocal[i] = pSrcLocal[pIdxLocal[i]];
                            pDstLocal[i + 1] = pSrcLocal[pIdxLocal[i + 1]];
                            pDstLocal[i + 2] = pSrcLocal[pIdxLocal[i + 2]];
                            pDstLocal[i + 3] = pSrcLocal[pIdxLocal[i + 3]];
                        }

                        for (; i < end; i++)
                        {
                            pDstLocal[i] = pSrcLocal[pIdxLocal[i]];
                        }
                    });
                }
            }
            else
            {
                fixed (T* pSource = source)
                fixed (int* pIndices = indices)
                fixed (T* pDest = destination)
                {
                    T* src = pSource;
                    int* idx = pIndices;
                    T* dst = pDest;

                    int i = 0;
                    for (; i <= length - 4; i += 4)
                    {
                        dst[i] = src[idx[i]];
                        dst[i + 1] = src[idx[i + 1]];
                        dst[i + 2] = src[idx[i + 2]];
                        dst[i + 3] = src[idx[i + 3]];
                    }

                    for (; i < length; i++)
                    {
                        dst[i] = src[idx[i]];
                    }
                }
            }
        }

        /// <summary>
        /// Materializes data while safely handling -1 indices by writing default(T) 
        /// and updating the validity mask. Used for Left, Outer, and AsOf joins.
        /// </summary>
        public static unsafe void TakeWithNulls<T>(ReadOnlySpan<T> source, ReadOnlySpan<int> indices, Span<T> destination, Glacier.Polaris.Memory.ValidityMask mask) where T : unmanaged
        {
            int length = indices.Length;

            fixed (T* pSource = source)
            fixed (int* pIndices = indices)
            fixed (T* pDest = destination)
            {
                T* src = pSource;
                int* idx = pIndices;
                T* dst = pDest;

                for (int i = 0; i < length; i++)
                {
                    int index = idx[i];
                    if (index == -1)
                    {
                        dst[i] = default(T);
                        mask.SetNull(i);
                    }
                    else
                    {
                        dst[i] = src[index];
                    }
                }
            }
        }
    }
}
