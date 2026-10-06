using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading.Tasks;

namespace Glacier.Polaris.Compute
{
    public static partial class SortKernels
    {
        /// <summary>ArgSort for Float64 — returns new int[]. Ultra-fast parallel 8-bit LSD radix sort.</summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe int[] ArgSort(ReadOnlySpan<double> data)
        {
            if (data.Length == 0) return Array.Empty<int>();
            int n = data.Length;
            if (n == 1) return new int[] { 0 };

            int[] indices = new int[n];
            ArgSortCoreFloat64(data, indices.AsSpan(), descending: false, isSequential: true);
            return indices;
        }

        /// <summary>In-place ArgSort for Float64 (re-sorts existing indices). Ultra-fast parallel 8-bit LSD radix sort.</summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe void ArgSort(ReadOnlySpan<double> data, Span<int> indices, bool descending = false)
        {
            ArgSortCoreFloat64(data, indices, descending, isSequential: false);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void ArgSortCoreFloat64(ReadOnlySpan<double> data, Span<int> indices, bool descending, bool isSequential)
        {
            int n = data.Length;
            if (n <= 1) return;

            // Sequential fallback for small arrays (< 65,536 elements)
            int numThreads = n < 65536 ? 1 : Math.Min(Environment.ProcessorCount, 16);
            if (numThreads <= 1)
            {
                long[] sKeys = System.Buffers.ArrayPool<long>.Shared.Rent(n);
                try
                {
                    fixed (double* pData = data)
                    fixed (int* pIdx = indices)
                    fixed (long* pKeys = sKeys)
                    {
                        if (isSequential)
                        {
                            for (int i = 0; i < n; i++)
                            {
                                pIdx[i] = i;
                                long bits = BitConverter.DoubleToInt64Bits(pData[i]);
                                pKeys[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                            }
                        }
                        else
                        {
                            for (int i = 0; i < n; i++)
                            {
                                long bits = BitConverter.DoubleToInt64Bits(pData[pIdx[i]]);
                                pKeys[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                            }
                        }
                    }

                    LocalRadixSort8Bit(sKeys.AsSpan(0, n), indices);

                    if (descending)
                    {
                        for (int i = 0, j = n - 1; i < j; i++, j--)
                        {
                            int t = indices[i]; indices[i] = indices[j]; indices[j] = t;
                        }
                    }
                    return;
                }
                finally
                {
                    System.Buffers.ArrayPool<long>.Shared.Return(sKeys);
                }
            }

            int chunkSize = (n + numThreads - 1) / numThreads;
            long[] keys = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            long[] keysBuf = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            int[] indicesBuf = System.Buffers.ArrayPool<int>.Shared.Rent(n);

            int[] flatCounts = System.Buffers.ArrayPool<int>.Shared.Rent(numThreads * 256);
            int[] flatOffsets = System.Buffers.ArrayPool<int>.Shared.Rent(numThreads * 256);

            try
            {
                fixed (double* pData = data)
                fixed (long* pKeys = keys, pKeysBuf = keysBuf)
                fixed (int* pIdx = indices, pIdxBuf = indicesBuf)
                fixed (int* pCounts = flatCounts, pOffsets = flatOffsets)
                {
                    IntPtr pDataPtr = (IntPtr)pData;
                    IntPtr pSrcKeys = (IntPtr)pKeys;
                    IntPtr pDstKeys = (IntPtr)pKeysBuf;
                    IntPtr pSrcIdx = (IntPtr)pIdx;
                    IntPtr pDstIdx = (IntPtr)pIdxBuf;
                    IntPtr pC = (IntPtr)pCounts;
                    IntPtr pO = (IntPtr)pOffsets;

                    // Phase 0: Parallel Key Transform and Index Initialization
                    Parallel.For(0, numThreads, t =>
                    {
                        double* d = (double*)pDataPtr;
                        long* k = (long*)pSrcKeys;
                        int* idx = (int*)pSrcIdx;

                        int start = t * chunkSize;
                        int end = Math.Min(start + chunkSize, n);

                        if (isSequential)
                        {
                            int i = start;
                            if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<double>.Count)
                            {
                                int step = Vector256<double>.Count;
                                var vZero = Vector256<long>.Zero;
                                var vSignBit = Vector256.Create(long.MinValue);
                                var vAllOnes = Vector256.Create(-1L);

                                int limit = end - step;
                                for (; i <= limit; i += step)
                                {
                                    var vDouble = Vector256.Load(d + i);
                                    var vBits = vDouble.As<double, long>();
                                    var vNegMask = Vector256.LessThan(vBits, vZero);
                                    var vXorMask = Vector256.ConditionalSelect(vNegMask, vAllOnes, vSignBit);
                                    Vector256.Store(vBits ^ vXorMask, k + i);

                                    idx[i] = i;
                                    idx[i + 1] = i + 1;
                                    idx[i + 2] = i + 2;
                                    idx[i + 3] = i + 3;
                                }
                            }
                            for (; i < end; i++)
                            {
                                long bits = BitConverter.DoubleToInt64Bits(d[i]);
                                k[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                                idx[i] = i;
                            }
                        }
                        else
                        {
                            for (int i = start; i < end; i++)
                            {
                                long bits = BitConverter.DoubleToInt64Bits(d[idx[i]]);
                                k[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                            }
                        }
                    });

                    // 8-pass LSD Radix Sort
                    for (int pass = 0; pass < 8; pass++)
                    {
                        int shift = pass * 8;
                        Array.Clear(flatCounts, 0, numThreads * 256);

                        // Phase 1: Parallel Count
                        Parallel.For(0, numThreads, t =>
                        {
                            long* sKeys = (long*)pSrcKeys;
                            int* tCounts = (int*)pC + (t * 256);
                            int start = t * chunkSize;
                            int end = Math.Min(start + chunkSize, n);

                            int i = start;
                            for (; i <= end - 4; i += 4)
                            {
                                tCounts[(int)((sKeys[i] >> shift) & 0xFF)]++;
                                tCounts[(int)((sKeys[i + 1] >> shift) & 0xFF)]++;
                                tCounts[(int)((sKeys[i + 2] >> shift) & 0xFF)]++;
                                tCounts[(int)((sKeys[i + 3] >> shift) & 0xFF)]++;
                            }
                            for (; i < end; i++)
                            {
                                tCounts[(int)((sKeys[i] >> shift) & 0xFF)]++;
                            }
                        });

                        // Phase 1.5: Check skip pass
                        bool skip = false;
                        for (int b = 0; b < 256; b++)
                        {
                            int sum = 0;
                            for (int t = 0; t < numThreads; t++) sum += pCounts[t * 256 + b];
                            if (sum == n) { skip = true; break; }
                        }
                        if (skip) continue;

                        // Phase 2: Prefix sums
                        int prefix = 0;
                        for (int b = 0; b < 256; b++)
                        {
                            for (int t = 0; t < numThreads; t++)
                            {
                                pOffsets[t * 256 + b] = prefix;
                                prefix += pCounts[t * 256 + b];
                            }
                        }

                        // Phase 3: Parallel Scatter
                        Parallel.For(0, numThreads, t =>
                        {
                            long* sKeys = (long*)pSrcKeys;
                            long* dKeys = (long*)pDstKeys;
                            int* sIdx = (int*)pSrcIdx;
                            int* dIdx = (int*)pDstIdx;
                            int* tOffsets = (int*)pO + (t * 256);
                            int* offsets = stackalloc int[256];
                            System.Runtime.CompilerServices.Unsafe.CopyBlock(offsets, tOffsets, 256 * sizeof(int));

                            int start = t * chunkSize;
                            int end = Math.Min(start + chunkSize, n);

                            int i = start;
                            for (; i <= end - 4; i += 4)
                            {
                                long k0 = sKeys[i];
                                int b0 = (int)((k0 >> shift) & 0xFF);
                                int pos0 = offsets[b0]++;
                                dKeys[pos0] = k0;
                                dIdx[pos0] = sIdx[i];

                                long k1 = sKeys[i + 1];
                                int b1 = (int)((k1 >> shift) & 0xFF);
                                int pos1 = offsets[b1]++;
                                dKeys[pos1] = k1;
                                dIdx[pos1] = sIdx[i + 1];

                                long k2 = sKeys[i + 2];
                                int b2 = (int)((k2 >> shift) & 0xFF);
                                int pos2 = offsets[b2]++;
                                dKeys[pos2] = k2;
                                dIdx[pos2] = sIdx[i + 2];

                                long k3 = sKeys[i + 3];
                                int b3 = (int)((k3 >> shift) & 0xFF);
                                int pos3 = offsets[b3]++;
                                dKeys[pos3] = k3;
                                dIdx[pos3] = sIdx[i + 3];
                            }
                            for (; i < end; i++)
                            {
                                long k = sKeys[i];
                                int b = (int)((k >> shift) & 0xFF);
                                int pos = offsets[b]++;
                                dKeys[pos] = k;
                                dIdx[pos] = sIdx[i];
                            }
                        });

                        // Swap pointers
                        IntPtr tk = pSrcKeys; pSrcKeys = pDstKeys; pDstKeys = tk;
                        IntPtr ti = pSrcIdx; pSrcIdx = pDstIdx; pDstIdx = ti;
                    }

                    if (pSrcIdx != (IntPtr)pIdx)
                    {
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(pIdx, (void*)pSrcIdx, (uint)(n * sizeof(int)));
                    }

                    if (descending)
                    {
                        for (int i = 0, j = n - 1; i < j; i++, j--)
                        {
                            int t = pIdx[i]; pIdx[i] = pIdx[j]; pIdx[j] = t;
                        }
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<long>.Shared.Return(keys);
                System.Buffers.ArrayPool<long>.Shared.Return(keysBuf);
                System.Buffers.ArrayPool<int>.Shared.Return(indicesBuf);
                System.Buffers.ArrayPool<int>.Shared.Return(flatCounts);
                System.Buffers.ArrayPool<int>.Shared.Return(flatOffsets);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void LocalRadixSort8Bit(Span<long> keysSpan, Span<int> indices)
        {
            int n = keysSpan.Length;
            if (n <= 1) return;

            long[] keysBuf = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            int[] indicesBuf = System.Buffers.ArrayPool<int>.Shared.Rent(n);

            try
            {
                fixed (long* pKeys = keysSpan)
                fixed (long* pKeysBuf = keysBuf)
                fixed (int* pIdx = indices)
                fixed (int* pIdxBuf = indicesBuf)
                {
                    long* srcKeys = pKeys;
                    long* dstKeys = pKeysBuf;
                    int* srcIdx = pIdx;
                    int* dstIdx = pIdxBuf;

                    int* counts = stackalloc int[2048];
                    for (int i = 0; i < 2048; i++) counts[i] = 0;

                    for (int j = 0; j < n; j++)
                    {
                        long k = srcKeys[j];
                        counts[k & 0xFF]++;
                        counts[256 + ((k >> 8) & 0xFF)]++;
                        counts[512 + ((k >> 16) & 0xFF)]++;
                        counts[768 + ((k >> 24) & 0xFF)]++;
                        counts[1024 + ((k >> 32) & 0xFF)]++;
                        counts[1280 + ((k >> 40) & 0xFF)]++;
                        counts[1536 + ((k >> 48) & 0xFF)]++;
                        counts[1792 + ((k >> 56) & 0xFF)]++;
                    }

                    for (int p = 0; p < 8; p++)
                    {
                        int* currentCounts = counts + (p * 256);

                        bool skipPass = false;
                        for (int b = 0; b < 256; b++)
                        {
                            if (currentCounts[b] == n)
                            {
                                skipPass = true;
                                break;
                            }
                        }

                        if (skipPass) continue;

                        int shift = p * 8;
                        int offset = 0;
                        for (int i = 0; i < 256; i++)
                        {
                            int c = currentCounts[i];
                            currentCounts[i] = offset;
                            offset += c;
                        }

                        int j = 0;
                        for (; j <= n - 4; j += 4)
                        {
                            long k0 = srcKeys[j]; int i0 = srcIdx[j];
                            long k1 = srcKeys[j + 1]; int i1 = srcIdx[j + 1];
                            long k2 = srcKeys[j + 2]; int i2 = srcIdx[j + 2];
                            long k3 = srcKeys[j + 3]; int i3 = srcIdx[j + 3];

                            int p0 = currentCounts[(k0 >> shift) & 0xFF]++; dstKeys[p0] = k0; dstIdx[p0] = i0;
                            int p1 = currentCounts[(k1 >> shift) & 0xFF]++; dstKeys[p1] = k1; dstIdx[p1] = i1;
                            int p2 = currentCounts[(k2 >> shift) & 0xFF]++; dstKeys[p2] = k2; dstIdx[p2] = i2;
                            int p3 = currentCounts[(k3 >> shift) & 0xFF]++; dstKeys[p3] = k3; dstIdx[p3] = i3;
                        }
                        for (; j < n; j++)
                        {
                            long k = srcKeys[j];
                            int pos = currentCounts[(k >> shift) & 0xFF]++;
                            dstKeys[pos] = k;
                            dstIdx[pos] = srcIdx[j];
                        }

                        long* tKeys = srcKeys; srcKeys = dstKeys; dstKeys = tKeys;
                        int* tIdx = srcIdx; srcIdx = dstIdx; dstIdx = tIdx;
                    }

                    if (srcIdx != pIdx)
                    {
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(pIdx, srcIdx, (uint)(n * sizeof(int)));
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(pKeys, srcKeys, (uint)(n * sizeof(long)));
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<long>.Shared.Return(keysBuf);
                System.Buffers.ArrayPool<int>.Shared.Return(indicesBuf);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void DoRadixPass64_8bit_Parallel(
            int* srcIdx, int* dstIdx, long* srcKeys, long* dstKeys, int length, int shift)
        {
            if (length <= 1) return;

            int numThreads = ComputeThreadCount(length);

            if (numThreads <= 1)
            {
                int* counts = stackalloc int[256];
                for (int i = 0; i < 256; i++) counts[i] = 0;

                for (int j = 0; j < length; j++)
                {
                    counts[(int)((srcKeys[j] >> shift) & 0xFF)]++;
                }

                int offset = 0;
                for (int j = 0; j < 256; j++)
                {
                    int c = counts[j];
                    counts[j] = offset;
                    offset += c;
                }

                for (int j = 0; j < length; j++)
                {
                    long key = srcKeys[j];
                    int bucket = (int)((key >> shift) & 0xFF);
                    int pos = counts[bucket]++;
                    dstKeys[pos] = key;
                    dstIdx[pos] = srcIdx[j];
                }
                return;
            }

            int chunkSize = (length + numThreads - 1) / numThreads;

            int[][] localCounts = new int[numThreads][];
            for (int t = 0; t < numThreads; t++)
            {
                localCounts[t] = System.Buffers.ArrayPool<int>.Shared.Rent(256);
                Array.Clear(localCounts[t], 0, 256);
            }

            IntPtr srcIdxPtr = (IntPtr)srcIdx;
            IntPtr dstIdxPtr = (IntPtr)dstIdx;
            IntPtr srcKeysPtr = (IntPtr)srcKeys;
            IntPtr dstKeysPtr = (IntPtr)dstKeys;

            try
            {
                Parallel.For(0, numThreads, t =>
                {
                    long* sKeys = (long*)srcKeysPtr;
                    int start = t * chunkSize;
                    int end = Math.Min(start + chunkSize, length);
                    int[] local = localCounts[t];

                    fixed (int* pLocal = local)
                    {
                        for (int j = start; j < end; j++)
                        {
                            int bucket = (int)((sKeys[j] >> shift) & 0xFF);
                            pLocal[bucket]++;
                        }
                    }
                });

                int[][] threadOffsets = new int[numThreads][];
                for (int t = 0; t < numThreads; t++)
                {
                    threadOffsets[t] = System.Buffers.ArrayPool<int>.Shared.Rent(256);
                }

                try
                {
                    int prefix = 0;
                    for (int b = 0; b < 256; b++)
                    {
                        for (int t = 0; t < numThreads; t++)
                        {
                            threadOffsets[t][b] = prefix;
                            prefix += localCounts[t][b];
                        }
                    }

                    Parallel.For(0, numThreads, t =>
                    {
                        int* sIdx = (int*)srcIdxPtr;
                        int* dIdx = (int*)dstIdxPtr;
                        long* sKeys = (long*)srcKeysPtr;
                        long* dKeys = (long*)dstKeysPtr;

                        int start = t * chunkSize;
                        int end = Math.Min(start + chunkSize, length);

                        int* offsets = stackalloc int[256];
                        fixed (int* pThreadOffsets = threadOffsets[t])
                        {
                            System.Runtime.CompilerServices.Unsafe.CopyBlock(offsets, pThreadOffsets, 256 * sizeof(int));
                        }

                        for (int j = start; j < end; j++)
                        {
                            long key = sKeys[j];
                            int bucket = (int)((key >> shift) & 0xFF);
                            int pos = offsets[bucket]++;

                            dKeys[pos] = key;
                            dIdx[pos] = sIdx[j];
                        }
                    });
                }
                finally
                {
                    for (int t = 0; t < numThreads; t++)
                        System.Buffers.ArrayPool<int>.Shared.Return(threadOffsets[t]);
                }
            }
            finally
            {
                for (int t = 0; t < numThreads; t++)
                    System.Buffers.ArrayPool<int>.Shared.Return(localCounts[t]);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void ConvertDoublesToSortableLongs(
            ReadOnlySpan<double> data, Span<long> keys, int* pIdx = null)
        {
            int n = data.Length;
            int parallelThreshold = 128_000;

            fixed (double* pData = data)
            fixed (long* pKeys = keys)
            {
                if (n >= parallelThreshold)
                {
                    IntPtr dIntPtr = (IntPtr)pData;
                    IntPtr kIntPtr = (IntPtr)pKeys;
                    IntPtr idxIntPtr = (IntPtr)pIdx;

                    int numThreads = Environment.ProcessorCount;
                    int chunkSize = (n + numThreads - 1) / numThreads;

                    Parallel.For(0, numThreads, p =>
                    {
                        double* dPtr = (double*)dIntPtr;
                        long* kPtr = (long*)kIntPtr;
                        int* idxPtr = (int*)idxIntPtr;

                        int start = p * chunkSize;
                        int end = Math.Min(start + chunkSize, n);
                        if (start >= end) return;

                        if (idxPtr == null)
                        {
                            int i = start;
                            if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<double>.Count)
                            {
                                int step = Vector256<double>.Count;
                                var vZero = Vector256<long>.Zero;
                                var vSignBit = Vector256.Create(long.MinValue);
                                var vAllOnes = Vector256.Create(-1L);

                                for (; i <= end - step; i += step)
                                {
                                    var vDouble = Vector256.Load(dPtr + i);
                                    var vBits = vDouble.As<double, long>();
                                    var vNegMask = Vector256.LessThan(vBits, vZero);
                                    var vXorMask = Vector256.ConditionalSelect(vNegMask, vAllOnes, vSignBit);
                                    var vResult = vBits ^ vXorMask;
                                    Vector256.Store(vResult, kPtr + i);
                                }
                            }
                            for (; i < end; i++)
                            {
                                long bits = BitConverter.DoubleToInt64Bits(dPtr[i]);
                                kPtr[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                            }
                        }
                        else
                        {
                            for (int i = start; i < end; i++)
                            {
                                long bits = BitConverter.DoubleToInt64Bits(dPtr[idxPtr[i]]);
                                kPtr[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                            }
                        }
                    });
                }
                else
                {
                    if (pIdx == null)
                    {
                        int i = 0;
                        if (Vector256.IsHardwareAccelerated && n >= Vector256<double>.Count)
                        {
                            int step = Vector256<double>.Count;
                            var vZero = Vector256<long>.Zero;
                            var vSignBit = Vector256.Create(long.MinValue);
                            var vAllOnes = Vector256.Create(-1L);

                            for (; i <= n - step; i += step)
                            {
                                var vDouble = Vector256.Load(pData + i);
                                var vBits = vDouble.As<double, long>();
                                var vNegMask = Vector256.LessThan(vBits, vZero);
                                var vXorMask = Vector256.ConditionalSelect(vNegMask, vAllOnes, vSignBit);
                                var vResult = vBits ^ vXorMask;
                                Vector256.Store(vResult, pKeys + i);
                            }
                        }
                        for (; i < n; i++)
                        {
                            long bits = BitConverter.DoubleToInt64Bits(pData[i]);
                            pKeys[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < n; i++)
                        {
                            long bits = BitConverter.DoubleToInt64Bits(pData[pIdx[i]]);
                            pKeys[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// ArgSort using Array.Sort with double-to-long conversion for Float64.
        /// </summary>
        public static int[] ArgSortSystem(ReadOnlySpan<double> data, bool descending = false)
        {
            int n = data.Length;
            var idx = new int[n];
            var keys = new long[n];
            for (int i = 0; i < n; i++)
            {
                long val = BitConverter.DoubleToInt64Bits(data[i]);
                if (val < 0) val ^= long.MaxValue;
                else val ^= unchecked((long)0x8000000000000000);
                keys[i] = val;
            }
            for (int i = 0; i < n; i++) idx[i] = i;
            Array.Sort(keys, idx);
            if (descending) Array.Reverse(idx);
            return idx;
        }
    }
}
