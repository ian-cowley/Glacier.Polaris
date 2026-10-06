using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Glacier.Polaris.Compute
{
    public static partial class SortKernels
    {
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void ParallelBlockSortFloat64(ReadOnlySpan<double> data, Span<int> indices, bool isSequential)
        {
            int n = data.Length;
            if (n <= 1) return;

            // Use minimum 2 threads, maximum physical cores
            int numThreads = Math.Min(n / 100_000, Environment.ProcessorCount);
            if (numThreads <= 1)
            {
                long[] singleKeys = System.Buffers.ArrayPool<long>.Shared.Rent(n);
                try
                {
                    fixed (int* pIdx = indices)
                    {
                        ConvertDoublesToSortableLongs(data, singleKeys, isSequential ? null : pIdx);
                    }

                    if (isSequential)
                    {
                        for (int i = 0; i < n; i++) indices[i] = i;
                    }

                    LocalRadixSort8Bit(singleKeys.AsSpan(0, n), indices);
                }
                finally
                {
                    System.Buffers.ArrayPool<long>.Shared.Return(singleKeys);
                }
                return;
            }

            int blockSize = (n + numThreads - 1) / numThreads;

            long[] keys = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            long[] keysBuf = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            int[] indicesBuf = System.Buffers.ArrayPool<int>.Shared.Rent(n);

            try
            {
                fixed (int* pIndices = indices)
                {
                    IntPtr pIndicesPtr = (IntPtr)pIndices;

                    ConvertDoublesToSortableLongs(data, keys, null);

                    Task[] tasks = new Task[numThreads];
                    for (int t = 0; t < numThreads; t++)
                    {
                        int tCapture = t;
                        int start = tCapture * blockSize;
                        int length = Math.Min(blockSize, n - start);
                        if (length <= 0) continue;

                        tasks[tCapture] = Task.Run(() =>
                        {
                            var localKeys = keys.AsSpan(start, length);
                            var localIdx = new Span<int>((int*)pIndicesPtr + start, length);

                            for (int i = 0; i < length; i++) localIdx[i] = start + i;

                            LocalRadixSort8Bit(localKeys, localIdx);
                        });
                    }
                    Task.WaitAll(tasks);

                    int step = 1;
                    bool dataInBuf = false;

                    while (step < numThreads)
                    {
                        int mergeCount = (numThreads + (step * 2) - 1) / (step * 2);
                        Task[] mTasks = new Task[mergeCount];

                        for (int i = 0; i < mergeCount; i++)
                        {
                            int iCapture = i;
                            int stepCapture = step;

                            int leftStart = iCapture * stepCapture * 2 * blockSize;
                            int rightStart = leftStart + (stepCapture * blockSize);
                            int leftLen = Math.Min(stepCapture * blockSize, n - leftStart);
                            if (leftLen < 0) leftLen = 0;
                            int rightLen = rightStart < n ? Math.Min(stepCapture * blockSize, n - rightStart) : 0;

                            bool currentInBuf = dataInBuf;
                            mTasks[iCapture] = Task.Run(() =>
                            {
                                var srcK = currentInBuf ? keysBuf.AsSpan() : keys.AsSpan();
                                var srcI = currentInBuf ? indicesBuf.AsSpan() : new Span<int>((int*)pIndicesPtr, n);
                                var dstK = currentInBuf ? keys.AsSpan() : keysBuf.AsSpan();
                                var dstI = currentInBuf ? new Span<int>((int*)pIndicesPtr, n) : indicesBuf.AsSpan();

                                if (rightLen > 0)
                                {
                                    MergeBlocks(
                                        srcK.Slice(leftStart, leftLen), srcI.Slice(leftStart, leftLen),
                                        srcK.Slice(rightStart, rightLen), srcI.Slice(rightStart, rightLen),
                                        dstK.Slice(leftStart, leftLen + rightLen), dstI.Slice(leftStart, leftLen + rightLen)
                                    );
                                }
                                else if (leftLen > 0)
                                {
                                    srcK.Slice(leftStart, leftLen).CopyTo(dstK.Slice(leftStart, leftLen));
                                    srcI.Slice(leftStart, leftLen).CopyTo(dstI.Slice(leftStart, leftLen));
                                }
                            });
                        }
                        Task.WaitAll(mTasks);

                        step *= 2;
                        dataInBuf = !dataInBuf;
                    }

                    if (dataInBuf)
                    {
                        indicesBuf.AsSpan(0, n).CopyTo(new Span<int>((int*)pIndicesPtr, n));
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<long>.Shared.Return(keys);
                System.Buffers.ArrayPool<long>.Shared.Return(keysBuf);
                System.Buffers.ArrayPool<int>.Shared.Return(indicesBuf);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void MergeBlocks(
            ReadOnlySpan<long> leftKeys, ReadOnlySpan<int> leftIdx,
            ReadOnlySpan<long> rightKeys, ReadOnlySpan<int> rightIdx,
            Span<long> destKeys, Span<int> destIdx)
        {
            int l = 0, r = 0, d = 0;
            int leftLen = leftKeys.Length;
            int rightLen = rightKeys.Length;

            fixed (long* pLeftK = leftKeys, pRightK = rightKeys, pDestK = destKeys)
            fixed (int* pLeftI = leftIdx, pRightI = rightIdx, pDestI = destIdx)
            {
                while (l < leftLen && r < rightLen)
                {
                    if (pLeftK[l] <= pRightK[r])
                    {
                        pDestK[d] = pLeftK[l];
                        pDestI[d] = pLeftI[l];
                        l++;
                    }
                    else
                    {
                        pDestK[d] = pRightK[r];
                        pDestI[d] = pRightI[r];
                        r++;
                    }
                    d++;
                }

                if (l < leftLen)
                {
                    System.Runtime.CompilerServices.Unsafe.CopyBlock(
                        pDestK + d, pLeftK + l, (uint)((leftLen - l) * sizeof(long)));
                    System.Runtime.CompilerServices.Unsafe.CopyBlock(
                        pDestI + d, pLeftI + l, (uint)((leftLen - l) * sizeof(int)));
                }
                else if (r < rightLen)
                {
                    System.Runtime.CompilerServices.Unsafe.CopyBlock(
                        pDestK + d, pRightK + r, (uint)((rightLen - r) * sizeof(long)));
                    System.Runtime.CompilerServices.Unsafe.CopyBlock(
                        pDestI + d, pRightI + r, (uint)((rightLen - r) * sizeof(int)));
                }
            }
        }

        /// <summary>Fast 16-bit radix pass for 64-bit keys. Uses heap-allocated 65536 bucket table.</summary>
        private static unsafe void DoRadixPass64_16bit(
            int* src, int* dst, long* keys, int length, int shift)
        {
            if (length <= 1) return;

            int[] countsArr = System.Buffers.ArrayPool<int>.Shared.Rent(65536);
            countsArr.AsSpan(0, 65536).Clear();

            try
            {
                fixed (int* counts = countsArr)
                {
                    for (int j = 0; j < length; j++)
                        counts[(int)((keys[src[j]] >> shift) & 0xFFFF)]++;

                    int offset = 0;
                    for (int j = 0; j < 65536; j++)
                    {
                        int c = counts[j];
                        counts[j] = offset;
                        offset += c;
                    }

                    for (int j = 0; j < length; j++)
                    {
                        int idx = src[j];
                        dst[counts[(int)((keys[idx] >> shift) & 0xFFFF)]++] = idx;
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<int>.Shared.Return(countsArr);
            }
        }

        [StructLayout(LayoutKind.Sequential)]
        private readonly struct LongIndexPair : IComparable<LongIndexPair>
        {
            public readonly long Key;
            public readonly int Index;
            public LongIndexPair(long key, int index) { Key = key; Index = index; }
            public int CompareTo(LongIndexPair other) => Key.CompareTo(other.Key);
        }
    }
}
