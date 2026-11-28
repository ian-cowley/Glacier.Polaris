using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Data;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Implements vectorized hash join kernels.
    /// </summary>
    public static class JoinKernels
    {
        public sealed class JoinResult
        {
            public int[] LeftIndices { get; set; } = null!;
            public int[] RightIndices { get; set; } = null!;
        }
        /// <summary>
        /// Performs a multi-threaded Partitioned Hash Join.
        /// Scaling is achieved by bucketizing keys to avoid global lock contention.
        /// </summary>
        public static JoinResult InnerJoin(Int32Series left, Int32Series right)
        {
            bool hasNulls = left.ValidityMask.HasNulls || right.ValidityMask.HasNulls;
            // Fast path for small right-side tables (< 10K rows) without nulls: avoid partition overhead
            if (!hasNulls && right.Length < 10000)
                return InnerJoinSmallRight(left, right);
            const int Partitions = 64;
            var leftMask = left.ValidityMask;
            var rightMask = right.ValidityMask;
            var leftBuckets = Bucketize(left.Memory.Span, leftMask, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, rightMask, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBuckets(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span, leftMask, rightMask);
                if (res.left.Length > 0) results.Add(res);
            });

            var flattened = FlattenPairs(results);
            SortByLeftIndices(ref flattened);
            return flattened;
        }
        public static JoinResult LeftJoin(Int32Series left, Int32Series right)
        {
            bool hasNulls = left.ValidityMask.HasNulls || right.ValidityMask.HasNulls;
            // Fast path for small right-side tables (< 10K rows) without nulls: avoid partition overhead
            if (!hasNulls && right.Length < 10000)
                return LeftJoinSmallRight(left, right);
            const int Partitions = 64;
            var leftMask = left.ValidityMask;
            var rightMask = right.ValidityMask;
            var leftBuckets = Bucketize(left.Memory.Span, leftMask, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, rightMask, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBucketsLeft(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span, leftMask, rightMask);
                if (res.left.Length > 0) results.Add(res);
            });

            var flattened = FlattenPairs(results);
            // For LEFT join, preserve original left table order (no priority sorting).
            // Python Polars keeps unmatched left rows in their original position.
            SortByLeftIndicesPreservingOrder(ref flattened);
            return flattened;
        }

        public static JoinResult OuterJoin(Int32Series left, Int32Series right)
        {
            const int Partitions = 64;
            var leftMask = left.ValidityMask;
            var rightMask = right.ValidityMask;
            var leftBuckets = Bucketize(left.Memory.Span, leftMask, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, rightMask, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBucketsOuter(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span, leftMask, rightMask);
                if (res.left.Length > 0) results.Add(res);
            });

            var flattened = FlattenPairs(results);
            SortByLeftIndices(ref flattened);
            return flattened;
        }

        public static JoinResult CrossJoin(int lLen, int rLen)
        {
            int total = lLen * rLen;
            int[] leftIndices = new int[total];
            int[] rightIndices = new int[total];

            System.Threading.Tasks.Parallel.For(0, lLen, i =>
            {
                int offset = i * rLen;
                for (int j = 0; j < rLen; j++)
                {
                    leftIndices[offset + j] = i;
                    rightIndices[offset + j] = j;
                }
            });

            return new JoinResult
            {
                LeftIndices = leftIndices,
                RightIndices = rightIndices
            };
        }

        public static JoinResult SemiJoin(Int32Series left, Int32Series right)
        {
            const int Partitions = 64;
            var leftMask = left.ValidityMask;
            var rightMask = right.ValidityMask;
            var leftBuckets = Bucketize(left.Memory.Span, leftMask, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, rightMask, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBucketsSemi(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span, leftMask, rightMask);
                if (res.left.Length > 0) results.Add(res);
            });

            var flattened = FlattenPairs(results);
            SortByLeftIndices(ref flattened);
            return flattened;
        }

        public static JoinResult AntiJoin(Int32Series left, Int32Series right)
        {
            const int Partitions = 64;
            var leftMask = left.ValidityMask;
            var rightMask = right.ValidityMask;
            var leftBuckets = Bucketize(left.Memory.Span, leftMask, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, rightMask, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBucketsAnti(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span, leftMask, rightMask);
                if (res.left.Length > 0) results.Add(res);
            });

            var flattened = FlattenPairs(results);
            SortByLeftIndices(ref flattened);
            return flattened;
        }

        private static void SortByLeftIndices(ref JoinResult result)
        {
            // Sort by left index to ensure deterministic ordering across parallel partitions.
            // Put -1 (unmatched) rows after valid rows:
            //   Priority 0: matched (both >= 0), sorted by left index
            //   Priority 1: unmatched right (left < 0)
            //   Priority 2: unmatched left (right < 0)
            var pairs = new (int left, int right)[result.LeftIndices.Length];
            for (int i = 0; i < pairs.Length; i++)
            {
                pairs[i] = (result.LeftIndices[i], result.RightIndices[i]);
            }
            Array.Sort(pairs, (a, b) =>
            {
                int Priority(int l, int r)
                {
                    if (l >= 0 && r >= 0) return 0;
                    if (l >= 0) return 2;  // unmatched left
                    return 1;  // unmatched right
                }
                int cmp = Priority(a.left, a.right).CompareTo(Priority(b.left, b.right));
                if (cmp != 0) return cmp;
                cmp = a.left.CompareTo(b.left);
                if (cmp != 0) return cmp;
                return a.right.CompareTo(b.right);
            });
            for (int i = 0; i < pairs.Length; i++)
            {
                result.LeftIndices[i] = pairs[i].left;
                result.RightIndices[i] = pairs[i].right;
            }
        }

        private static void SortByLeftIndicesPreservingOrder(ref JoinResult result)
        {
            // For LEFT join, preserve original left table order.
            // Sort by left index only, keeping unmatched left rows in their original position.
            // This matches Python Polars behavior where unmatched left rows appear 
            // in their original position rather than being pushed to the end.
            var pairs = new (int left, int right)[result.LeftIndices.Length];
            for (int i = 0; i < pairs.Length; i++)
            {
                pairs[i] = (result.LeftIndices[i], result.RightIndices[i]);
            }
            Array.Sort(pairs, (a, b) =>
            {
                int cmp = a.left.CompareTo(b.left);
                if (cmp != 0) return cmp;
                return a.right.CompareTo(b.right);
            });
            for (int i = 0; i < pairs.Length; i++)
            {
                result.LeftIndices[i] = pairs[i].left;
                result.RightIndices[i] = pairs[i].right;
            }
        }

        private static List<int>[] Bucketize(ReadOnlySpan<int> data, Glacier.Polaris.Memory.ValidityMask mask, int count)
        {
            var buckets = new List<int>[count];
            for (int i = 0; i < count; i++) buckets[i] = new List<int>();

            bool hasNulls = mask.HasNulls;
            for (int i = 0; i < data.Length; i++)
            {
                int key = (hasNulls && !mask.IsValid(i)) ? int.MinValue : data[i];
                int bucket = (int)((uint)key.GetHashCode() % (uint)count);
                buckets[bucket].Add(i);
            }
            return buckets;
        }

        private static (int[] left, int[] right) JoinBuckets(
            List<int> leftIndices,
            List<int> rightIndices,
            ReadOnlySpan<int> leftData,
            ReadOnlySpan<int> rightData,
            Glacier.Polaris.Memory.ValidityMask leftMask,
            Glacier.Polaris.Memory.ValidityMask rightMask)
        {
            if (leftIndices.Count == 0 || rightIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            bool leftHasNulls = leftMask.HasNulls;
            bool rightHasNulls = rightMask.HasNulls;

            var map = new Dictionary<int, List<int>>(leftIndices.Count);
            foreach (var idx in leftIndices)
            {
                if (leftHasNulls && !leftMask.IsValid(idx)) continue;
                int val = leftData[idx];
                if (!map.TryGetValue(val, out var list)) map[val] = list = new List<int>();
                list.Add(idx);
            }

            if (map.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in rightIndices)
            {
                if (rightHasNulls && !rightMask.IsValid(idx)) continue;
                int val = rightData[idx];
                if (map.TryGetValue(val, out var matchingLeft))
                {
                    foreach (var lIdx in matchingLeft)
                    {
                        lResult.Add(lIdx);
                        rResult.Add(idx);
                    }
                }
            }

            return (lResult.ToArray(), rResult.ToArray());
        }

        private static (int[] left, int[] right) JoinBucketsLeft(
            List<int> leftIndices,
            List<int> rightIndices,
            ReadOnlySpan<int> leftData,
            ReadOnlySpan<int> rightData,
            Glacier.Polaris.Memory.ValidityMask leftMask,
            Glacier.Polaris.Memory.ValidityMask rightMask)
        {
            if (leftIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            bool leftHasNulls = leftMask.HasNulls;
            bool rightHasNulls = rightMask.HasNulls;

            var map = new Dictionary<int, List<int>>(rightIndices.Count);
            foreach (var idx in rightIndices)
            {
                if (rightHasNulls && !rightMask.IsValid(idx)) continue;
                int val = rightData[idx];
                if (!map.TryGetValue(val, out var list)) map[val] = list = new List<int>();
                list.Add(idx);
            }

            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in leftIndices)
            {
                if (!leftHasNulls || leftMask.IsValid(idx))
                {
                    int val = leftData[idx];
                    if (map.TryGetValue(val, out var matchingRight))
                    {
                        foreach (var rIdx in matchingRight)
                        {
                            lResult.Add(idx);
                            rResult.Add(rIdx);
                        }
                        continue;
                    }
                }
                lResult.Add(idx);
                rResult.Add(-1);
            }

            return (lResult.ToArray(), rResult.ToArray());
        }

        private static (int[] left, int[] right) JoinBucketsOuter(
            List<int> leftIndices,
            List<int> rightIndices,
            ReadOnlySpan<int> leftData,
            ReadOnlySpan<int> rightData,
            Glacier.Polaris.Memory.ValidityMask leftMask,
            Glacier.Polaris.Memory.ValidityMask rightMask)
        {
            if (leftIndices.Count == 0 && rightIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            bool leftHasNulls = leftMask.HasNulls;
            bool rightHasNulls = rightMask.HasNulls;

            var map = new Dictionary<int, List<int>>(rightIndices.Count);
            foreach (var idx in rightIndices)
            {
                if (rightHasNulls && !rightMask.IsValid(idx)) continue;
                int val = rightData[idx];
                if (!map.TryGetValue(val, out var list)) map[val] = list = new List<int>();
                list.Add(idx);
            }

            var rightVisited = new HashSet<int>();
            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in leftIndices)
            {
                if (!leftHasNulls || leftMask.IsValid(idx))
                {
                    int val = leftData[idx];
                    if (map.TryGetValue(val, out var matchingRight))
                    {
                        foreach (var rIdx in matchingRight)
                        {
                            lResult.Add(idx);
                            rResult.Add(rIdx);
                            rightVisited.Add(rIdx);
                        }
                        continue;
                    }
                }
                lResult.Add(idx);
                rResult.Add(-1);
            }

            foreach (var idx in rightIndices)
            {
                if (!rightVisited.Contains(idx))
                {
                    lResult.Add(-1);
                    rResult.Add(idx);
                }
            }

            return (lResult.ToArray(), rResult.ToArray());
        }

        private static (int[] left, int[] right) JoinBucketsSemi(
            List<int> leftIndices,
            List<int> rightIndices,
            ReadOnlySpan<int> leftData,
            ReadOnlySpan<int> rightData,
            Glacier.Polaris.Memory.ValidityMask leftMask,
            Glacier.Polaris.Memory.ValidityMask rightMask)
        {
            if (leftIndices.Count == 0 || rightIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            bool leftHasNulls = leftMask.HasNulls;
            bool rightHasNulls = rightMask.HasNulls;

            var rightKeys = new HashSet<int>(rightIndices.Count);
            foreach (var idx in rightIndices)
            {
                if (!rightHasNulls || rightMask.IsValid(idx)) rightKeys.Add(rightData[idx]);
            }
            if (rightKeys.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in leftIndices)
            {
                if ((!leftHasNulls || leftMask.IsValid(idx)) && rightKeys.Contains(leftData[idx]))
                {
                    lResult.Add(idx);
                    rResult.Add(-1);
                }
            }

            return (lResult.ToArray(), rResult.ToArray());
        }

        private static (int[] left, int[] right) JoinBucketsAnti(
            List<int> leftIndices,
            List<int> rightIndices,
            ReadOnlySpan<int> leftData,
            ReadOnlySpan<int> rightData,
            Glacier.Polaris.Memory.ValidityMask leftMask,
            Glacier.Polaris.Memory.ValidityMask rightMask)
        {
            if (leftIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());
            if (rightIndices.Count == 0) return (leftIndices.ToArray(), Enumerable.Repeat(-1, leftIndices.Count).ToArray());

            bool leftHasNulls = leftMask.HasNulls;
            bool rightHasNulls = rightMask.HasNulls;

            var rightKeys = new HashSet<int>(rightIndices.Count);
            foreach (var idx in rightIndices)
            {
                if (!rightHasNulls || rightMask.IsValid(idx)) rightKeys.Add(rightData[idx]);
            }

            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in leftIndices)
            {
                if ((leftHasNulls && !leftMask.IsValid(idx)) || !rightKeys.Contains(leftData[idx]))
                {
                    lResult.Add(idx);
                    rResult.Add(-1);
                }
            }

            return (lResult.ToArray(), rResult.ToArray());
        }



        private static JoinResult FlattenPairs(System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)> bags)
        {
            var array = bags.ToArray();
            int total = 0;
            foreach (var pair in array) total += pair.left.Length;

            var left = new int[total];
            var right = new int[total];
            int offset = 0;
            foreach (var pair in array)
            {
                pair.left.CopyTo(left, offset);
                pair.right.CopyTo(right, offset);
                offset += pair.left.Length;
            }
            return new JoinResult { LeftIndices = left, RightIndices = right };
        }


        public static JoinResult JoinAsof(Int32Series left, Int32Series right)
        {
            ArgumentNullException.ThrowIfNull(left);
            ArgumentNullException.ThrowIfNull(right);
            if (left.ValidityMask.HasNulls || right.ValidityMask.HasNulls)
                return JoinAsof((ISeries)left, (ISeries)right);

            var leftMem = left.Memory;
            var rightMem = right.Memory;

            int[] leftIndices = new int[leftMem.Length];
            int[] rightIndices = new int[leftMem.Length];

            System.Threading.Tasks.Parallel.For(0, leftMem.Length, i =>
            {
                leftIndices[i] = i;
                rightIndices[i] = BinarySearchAsof(rightMem.Span, leftMem.Span[i]);
            });

            return new JoinResult { LeftIndices = leftIndices, RightIndices = rightIndices };
        }

        private static int BinarySearchAsof(ReadOnlySpan<int> data, int value)
        {
            int low = 0, high = data.Length - 1, result = -1;
            while (low <= high)
            {
                int mid = low + (high - low) / 2;
                if (data[mid] <= value) { result = mid; low = mid + 1; }
                else { high = mid - 1; }
            }
            return result;
        }
        /// <summary>
        /// Fast-path inner join optimized for small right-side tables.
        /// Builds a single hash map from the right table (no partitioning),
        /// then probes with the left table. Avoids ConcurrentBag and flatten overhead.
        /// Automatically selected when right table is &lt; 10K rows.
        /// </summary>
        private static unsafe JoinResult InnerJoinSmallRight(Int32Series left, Int32Series right)
        {
            var rightSpan = right.Memory.Span;
            var leftSpan = left.Memory.Span;

            int rightLen = rightSpan.Length;
            int leftLen = leftSpan.Length;
            if (rightLen == 0 || leftLen == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            // Determine min and max in right table
            int minVal = rightSpan[0];
            int maxVal = rightSpan[0];
            for (int i = 1; i < rightLen; i++)
            {
                int v = rightSpan[i];
                if (v < minVal) minVal = v;
                if (v > maxVal) maxVal = v;
            }

            long range = (long)maxVal - minVal + 1;
            bool useDirect = range > 0 && range <= 131072;

            if (useDirect)
            {
                int directLen = (int)range;
                var directLookup = new int[directLen];
                Array.Fill(directLookup, -1);
                bool hasDuplicates = false;

                for (int i = 0; i < rightLen; i++)
                {
                    int offset = rightSpan[i] - minVal;
                    if (directLookup[offset] != -1) { hasDuplicates = true; break; }
                    directLookup[offset] = i;
                }

                if (!hasDuplicates)
                {
                    int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, leftLen / 32768));
                    int chunkSize = (leftLen + numChunks - 1) / numChunks;
                    int[] chunkCounts = new int[numChunks];

                    fixed (int* pLeft = leftSpan)
                    fixed (int* pDirect = directLookup)
                    {
                        int* leftPtr = pLeft;
                        int* directPtr = pDirect;
                        uint uDirectLen = (uint)directLen;
                        int min = minVal;

                        // Pass 1: Count matches in parallel
                        Parallel.For(0, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int end = Math.Min(leftLen, start + chunkSize);
                            int matches = 0;
                            for (int i = start; i < end; i++)
                            {
                                int offset = leftPtr[i] - min;
                                if ((uint)offset < uDirectLen && directPtr[offset] >= 0)
                                    matches++;
                            }
                            chunkCounts[c] = matches;
                        });

                        int totalMatches = 0;
                        int[] chunkOffsets = new int[numChunks];
                        for (int c = 0; c < numChunks; c++)
                        {
                            chunkOffsets[c] = totalMatches;
                            totalMatches += chunkCounts[c];
                        }

                        var leftResult = new int[totalMatches];
                        var rightResult = new int[totalMatches];

                        fixed (int* pLRes = leftResult)
                        fixed (int* pRRes = rightResult)
                        {
                            int* lResPtr = pLRes;
                            int* rResPtr = pRRes;

                            // Pass 2: Fill in parallel
                            Parallel.For(0, numChunks, c =>
                            {
                                int start = c * chunkSize;
                                int end = Math.Min(leftLen, start + chunkSize);
                                int pos = chunkOffsets[c];
                                for (int i = start; i < end; i++)
                                {
                                    int offset = leftPtr[i] - min;
                                    if ((uint)offset < uDirectLen)
                                    {
                                        int r = directPtr[offset];
                                        if (r >= 0)
                                        {
                                            lResPtr[pos] = i;
                                            rResPtr[pos] = r;
                                            pos++;
                                        }
                                    }
                                }
                            });
                        }

                        return new JoinResult { LeftIndices = leftResult, RightIndices = rightResult };
                    }
                }
            }

            // General path with chaining (handles duplicates and sparse keys)
            int bucketCount = 1;
            while (bucketCount <= rightLen) bucketCount <<= 1;
            bucketCount <<= 1;
            uint bucketMask = (uint)(bucketCount - 1);

            var buckets = new int[bucketCount];
            Array.Fill(buckets, -1);
            var next = new int[rightLen];

            for (int i = 0; i < rightLen; i++)
            {
                int val = rightSpan[i];
                uint bucket = ((uint)val * 2654435761u) & bucketMask;
                next[i] = buckets[bucket];
                buckets[bucket] = i;
            }

            int pNumChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, leftLen / 32768));
            int pChunkSize = (leftLen + pNumChunks - 1) / pNumChunks;
            int[] pChunkCounts = new int[pNumChunks];

            fixed (int* pLeft = leftSpan)
            fixed (int* pRight = rightSpan)
            fixed (int* pBuckets = buckets)
            fixed (int* pNext = next)
            {
                int* leftPtr = pLeft;
                int* rightPtr = pRight;
                int* bucketsPtr = pBuckets;
                int* nextPtr = pNext;

                Parallel.For(0, pNumChunks, c =>
                {
                    int start = c * pChunkSize;
                    int end = Math.Min(leftLen, start + pChunkSize);
                    int matches = 0;
                    for (int i = start; i < end; i++)
                    {
                        int val = leftPtr[i];
                        uint bucket = ((uint)val * 2654435761u) & bucketMask;
                        int rIdx = bucketsPtr[bucket];
                        while (rIdx >= 0)
                        {
                            if (rightPtr[rIdx] == val) matches++;
                            rIdx = nextPtr[rIdx];
                        }
                    }
                    pChunkCounts[c] = matches;
                });

                int totalMatches = 0;
                int[] chunkOffsets = new int[pNumChunks];
                for (int c = 0; c < pNumChunks; c++)
                {
                    chunkOffsets[c] = totalMatches;
                    totalMatches += pChunkCounts[c];
                }

                var leftResult = new int[totalMatches];
                var rightResult = new int[totalMatches];

                fixed (int* pLRes = leftResult)
                fixed (int* pRRes = rightResult)
                {
                    int* lResPtr = pLRes;
                    int* rResPtr = pRRes;

                    Parallel.For(0, pNumChunks, c =>
                    {
                        int start = c * pChunkSize;
                        int end = Math.Min(leftLen, start + pChunkSize);
                        int pos = chunkOffsets[c];
                        for (int i = start; i < end; i++)
                        {
                            int val = leftPtr[i];
                            uint bucket = ((uint)val * 2654435761u) & bucketMask;
                            int rIdx = bucketsPtr[bucket];
                            while (rIdx >= 0)
                            {
                                if (rightPtr[rIdx] == val)
                                {
                                    lResPtr[pos] = i;
                                    rResPtr[pos] = rIdx;
                                    pos++;
                                }
                                rIdx = nextPtr[rIdx];
                            }
                        }
                    });
                }

                return new JoinResult { LeftIndices = leftResult, RightIndices = rightResult };
            }
        }

        private static unsafe JoinResult LeftJoinSmallRight(Int32Series left, Int32Series right)
        {
            var rightSpan = right.Memory.Span;
            var leftSpan = left.Memory.Span;

            int rightLen = rightSpan.Length;
            int leftLen = leftSpan.Length;
            if (leftLen == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            if (rightLen == 0)
            {
                var lRes = new int[leftLen];
                var rRes = new int[leftLen];
                Array.Fill(rRes, -1);
                for (int i = 0; i < leftLen; i++) lRes[i] = i;
                return new JoinResult { LeftIndices = lRes, RightIndices = rRes };
            }

            int minVal = rightSpan[0];
            int maxVal = rightSpan[0];
            for (int i = 1; i < rightLen; i++)
            {
                int v = rightSpan[i];
                if (v < minVal) minVal = v;
                if (v > maxVal) maxVal = v;
            }

            long range = (long)maxVal - minVal + 1;
            bool useDirect = range > 0 && range <= 131072;

            if (useDirect)
            {
                int directLen = (int)range;
                var directLookup = new int[directLen];
                Array.Fill(directLookup, -1);
                bool hasDuplicates = false;

                for (int i = 0; i < rightLen; i++)
                {
                    int offset = rightSpan[i] - minVal;
                    if (directLookup[offset] != -1) { hasDuplicates = true; break; }
                    directLookup[offset] = i;
                }

                if (!hasDuplicates)
                {
                    // Unique right keys: Exactly 1 output row per left row!
                    var leftResult = new int[leftLen];
                    var rightResult = new int[leftLen];

                    int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, leftLen / 32768));
                    int chunkSize = (leftLen + numChunks - 1) / numChunks;

                    fixed (int* pLeft = leftSpan)
                    fixed (int* pDirect = directLookup)
                    fixed (int* pLRes = leftResult)
                    fixed (int* pRRes = rightResult)
                    {
                        int* leftPtr = pLeft;
                        int* directPtr = pDirect;
                        int* lResPtr = pLRes;
                        int* rResPtr = pRRes;
                        uint uDirectLen = (uint)directLen;
                        int min = minVal;

                        Parallel.For(0, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int end = Math.Min(leftLen, start + chunkSize);
                            for (int i = start; i < end; i++)
                            {
                                lResPtr[i] = i;
                                int offset = leftPtr[i] - min;
                                if ((uint)offset < uDirectLen)
                                    rResPtr[i] = directPtr[offset];
                                else
                                    rResPtr[i] = -1;
                            }
                        });
                    }

                    return new JoinResult { LeftIndices = leftResult, RightIndices = rightResult };
                }
            }

            // Fallback for duplicates / sparse right table
            var rightMap = new Dictionary<int, List<int>>(rightLen);
            for (int i = 0; i < rightLen; i++)
            {
                int val = rightSpan[i];
                if (!rightMap.TryGetValue(val, out var list))
                    rightMap[val] = new List<int> { i };
                else
                    list.Add(i);
            }

            int totalMatches = 0;
            for (int i = 0; i < leftLen; i++)
            {
                if (rightMap.TryGetValue(leftSpan[i], out var list))
                    totalMatches += list.Count;
                else
                    totalMatches++;
            }

            var lResult = new int[totalMatches];
            var rResult = new int[totalMatches];

            int pos = 0;
            for (int i = 0; i < leftLen; i++)
            {
                if (rightMap.TryGetValue(leftSpan[i], out var list))
                {
                    foreach (int rIdx in list)
                    {
                        lResult[pos] = i;
                        rResult[pos] = rIdx;
                        pos++;
                    }
                }
                else
                {
                    lResult[pos] = i;
                    rResult[pos] = -1;
                    pos++;
                }
            }

            return new JoinResult { LeftIndices = lResult, RightIndices = rResult };
        }

        public static JoinResult InnerJoin(ISeries[] leftKeys, ISeries[] rightKeys)
        {
            ValidateJoinKeys(leftKeys, rightKeys);
            if (leftKeys.Length == 1 && leftKeys[0] is Int32Series li && rightKeys[0] is Int32Series ri)
                return InnerJoin(li, ri);

            int leftLen = leftKeys[0].Length;
            int rightLen = rightKeys[0].Length;
            if (leftLen == 0 || rightLen == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            var rightMap = BuildRightHashMap(rightKeys, rightLen);
            if (rightMap.Count == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            var lResult = new List<int>();
            var rResult = new List<int>();

            for (int lIdx = 0; lIdx < leftLen; lIdx++)
            {
                if (HasAnyNullKey(leftKeys, lIdx)) continue;
                int hash = ComputeKeyHash(leftKeys, lIdx);
                if (!rightMap.TryGetValue(hash, out var candidates)) continue;

                for (int c = 0; c < candidates.Count; c++)
                {
                    int rIdx = candidates[c];
                    if (KeysEqual(leftKeys, lIdx, rightKeys, rIdx))
                    {
                        lResult.Add(lIdx);
                        rResult.Add(rIdx);
                    }
                }
            }

            return new JoinResult { LeftIndices = lResult.ToArray(), RightIndices = rResult.ToArray() };
        }

        public static JoinResult LeftJoin(ISeries[] leftKeys, ISeries[] rightKeys)
        {
            ValidateJoinKeys(leftKeys, rightKeys);
            if (leftKeys.Length == 1 && leftKeys[0] is Int32Series li && rightKeys[0] is Int32Series ri)
                return LeftJoin(li, ri);

            int leftLen = leftKeys[0].Length;
            int rightLen = rightKeys[0].Length;
            if (leftLen == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            var rightMap = BuildRightHashMap(rightKeys, rightLen);
            var lResult = new List<int>(leftLen);
            var rResult = new List<int>(leftLen);

            for (int lIdx = 0; lIdx < leftLen; lIdx++)
            {
                bool matched = false;
                if (!HasAnyNullKey(leftKeys, lIdx))
                {
                    int hash = ComputeKeyHash(leftKeys, lIdx);
                    if (rightMap.TryGetValue(hash, out var candidates))
                    {
                        for (int c = 0; c < candidates.Count; c++)
                        {
                            int rIdx = candidates[c];
                            if (KeysEqual(leftKeys, lIdx, rightKeys, rIdx))
                            {
                                lResult.Add(lIdx);
                                rResult.Add(rIdx);
                                matched = true;
                            }
                        }
                    }
                }

                if (!matched)
                {
                    lResult.Add(lIdx);
                    rResult.Add(-1);
                }
            }

            return new JoinResult { LeftIndices = lResult.ToArray(), RightIndices = rResult.ToArray() };
        }

        public static JoinResult OuterJoin(ISeries[] leftKeys, ISeries[] rightKeys)
        {
            ValidateJoinKeys(leftKeys, rightKeys);
            if (leftKeys.Length == 1 && leftKeys[0] is Int32Series li && rightKeys[0] is Int32Series ri)
                return OuterJoin(li, ri);

            int leftLen = leftKeys[0].Length;
            int rightLen = rightKeys[0].Length;
            if (leftLen == 0 && rightLen == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            var rightMap = BuildRightHashMap(rightKeys, rightLen);
            var rightVisited = new bool[rightLen];
            var lResult = new List<int>(leftLen + rightLen);
            var rResult = new List<int>(leftLen + rightLen);

            for (int lIdx = 0; lIdx < leftLen; lIdx++)
            {
                bool matched = false;
                if (!HasAnyNullKey(leftKeys, lIdx))
                {
                    int hash = ComputeKeyHash(leftKeys, lIdx);
                    if (rightMap.TryGetValue(hash, out var candidates))
                    {
                        for (int c = 0; c < candidates.Count; c++)
                        {
                            int rIdx = candidates[c];
                            if (KeysEqual(leftKeys, lIdx, rightKeys, rIdx))
                            {
                                lResult.Add(lIdx);
                                rResult.Add(rIdx);
                                rightVisited[rIdx] = true;
                                matched = true;
                            }
                        }
                    }
                }

                if (!matched)
                {
                    lResult.Add(lIdx);
                    rResult.Add(-1);
                }
            }

            for (int rIdx = 0; rIdx < rightLen; rIdx++)
            {
                if (!rightVisited[rIdx])
                {
                    lResult.Add(-1);
                    rResult.Add(rIdx);
                }
            }

            var result = new JoinResult { LeftIndices = lResult.ToArray(), RightIndices = rResult.ToArray() };
            SortByLeftIndices(ref result);
            return result;
        }

        public static JoinResult SemiJoin(ISeries[] leftKeys, ISeries[] rightKeys)
        {
            ValidateJoinKeys(leftKeys, rightKeys);
            if (leftKeys.Length == 1 && leftKeys[0] is Int32Series li && rightKeys[0] is Int32Series ri)
                return SemiJoin(li, ri);

            int leftLen = leftKeys[0].Length;
            int rightLen = rightKeys[0].Length;
            if (leftLen == 0 || rightLen == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            var rightMap = BuildRightHashMap(rightKeys, rightLen);
            if (rightMap.Count == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            var lResult = new List<int>();
            var rResult = new List<int>();

            for (int lIdx = 0; lIdx < leftLen; lIdx++)
            {
                if (HasAnyNullKey(leftKeys, lIdx)) continue;
                int hash = ComputeKeyHash(leftKeys, lIdx);
                if (!rightMap.TryGetValue(hash, out var candidates)) continue;

                for (int c = 0; c < candidates.Count; c++)
                {
                    if (KeysEqual(leftKeys, lIdx, rightKeys, candidates[c]))
                    {
                        lResult.Add(lIdx);
                        rResult.Add(-1);
                        break;
                    }
                }
            }

            return new JoinResult { LeftIndices = lResult.ToArray(), RightIndices = rResult.ToArray() };
        }

        public static JoinResult AntiJoin(ISeries[] leftKeys, ISeries[] rightKeys)
        {
            ValidateJoinKeys(leftKeys, rightKeys);
            if (leftKeys.Length == 1 && leftKeys[0] is Int32Series li && rightKeys[0] is Int32Series ri)
                return AntiJoin(li, ri);

            int leftLen = leftKeys[0].Length;
            int rightLen = rightKeys[0].Length;
            if (leftLen == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            if (rightLen == 0)
            {
                var allLeft = new int[leftLen];
                var allMinusOne = new int[leftLen];
                for (int i = 0; i < leftLen; i++) { allLeft[i] = i; allMinusOne[i] = -1; }
                return new JoinResult { LeftIndices = allLeft, RightIndices = allMinusOne };
            }

            var rightMap = BuildRightHashMap(rightKeys, rightLen);
            var lResult = new List<int>();
            var rResult = new List<int>();

            for (int lIdx = 0; lIdx < leftLen; lIdx++)
            {
                bool matched = false;
                if (!HasAnyNullKey(leftKeys, lIdx))
                {
                    int hash = ComputeKeyHash(leftKeys, lIdx);
                    if (rightMap.TryGetValue(hash, out var candidates))
                    {
                        for (int c = 0; c < candidates.Count; c++)
                        {
                            if (KeysEqual(leftKeys, lIdx, rightKeys, candidates[c]))
                            {
                                matched = true;
                                break;
                            }
                        }
                    }
                }

                if (!matched)
                {
                    lResult.Add(lIdx);
                    rResult.Add(-1);
                }
            }

            return new JoinResult { LeftIndices = lResult.ToArray(), RightIndices = rResult.ToArray() };
        }

        public static JoinResult JoinAsof(ISeries left, ISeries right)
        {
            ArgumentNullException.ThrowIfNull(left);
            ArgumentNullException.ThrowIfNull(right);
            if (left.GetType() != right.GetType() || left.DataType != right.DataType)
                throw new ArgumentException($"AsOf join key type mismatch: '{left.Name}' ({left.DataType.Name}) vs '{right.Name}' ({right.DataType.Name}).");

            if (left is Int32Series li && right is Int32Series ri && !li.ValidityMask.HasNulls && !ri.ValidityMask.HasNulls)
                return JoinAsof(li, ri);

            int leftLen = left.Length;
            int rightLen = right.Length;
            int[] leftIndices = new int[leftLen];
            int[] rightIndices = new int[leftLen];

            int[]? validRightIndices = null;
            int searchLen = rightLen;
            if (right.ValidityMask.HasNulls && rightLen > 0)
            {
                var validList = new List<int>(rightLen);
                for (int r = 0; r < rightLen; r++)
                {
                    if (!right.ValidityMask.IsNull(r))
                        validList.Add(r);
                }
                validRightIndices = validList.ToArray();
                searchLen = validRightIndices.Length;
            }

            for (int i = 0; i < leftLen; i++)
            {
                leftIndices[i] = i;
                if (left.ValidityMask.IsNull(i) || searchLen == 0)
                {
                    rightIndices[i] = -1;
                    continue;
                }

                int low = 0, high = searchLen - 1, best = -1;
                while (low <= high)
                {
                    int mid = low + (high - low) / 2;
                    int rIdx = validRightIndices != null ? validRightIndices[mid] : mid;
                    int cmp = CompareNonNullSeriesElements(right, rIdx, left, i);
                    if (cmp <= 0)
                    {
                        best = rIdx;
                        low = mid + 1;
                    }
                    else
                    {
                        high = mid - 1;
                    }
                }
                rightIndices[i] = best;
            }

            return new JoinResult { LeftIndices = leftIndices, RightIndices = rightIndices };
        }

        private static void ValidateJoinKeys(ISeries[] leftKeys, ISeries[] rightKeys)
        {
            ArgumentNullException.ThrowIfNull(leftKeys);
            ArgumentNullException.ThrowIfNull(rightKeys);
            if (leftKeys.Length == 0)
                throw new ArgumentException("At least one join key column is required.", nameof(leftKeys));
            if (leftKeys.Length != rightKeys.Length)
                throw new ArgumentException($"Join key arity mismatch: left has {leftKeys.Length} keys, right has {rightKeys.Length} keys.");

            for (int k = 0; k < leftKeys.Length; k++)
            {
                if (leftKeys[k].GetType() != rightKeys[k].GetType() || leftKeys[k].DataType != rightKeys[k].DataType)
                {
                    throw new ArgumentException(
                        $"Join key type mismatch at index {k}: left '{leftKeys[k].Name}' is {leftKeys[k].DataType.Name} ({leftKeys[k].GetType().Name}), right '{rightKeys[k].Name}' is {rightKeys[k].DataType.Name} ({rightKeys[k].GetType().Name}).");
                }
            }
        }

        private static Dictionary<int, List<int>> BuildRightHashMap(ISeries[] rightKeys, int rightLen)
        {
            var map = new Dictionary<int, List<int>>(rightLen);
            for (int rIdx = 0; rIdx < rightLen; rIdx++)
            {
                if (HasAnyNullKey(rightKeys, rIdx)) continue;
                int hash = ComputeKeyHash(rightKeys, rIdx);
                if (!map.TryGetValue(hash, out var list))
                {
                    list = new List<int>(1);
                    map[hash] = list;
                }
                list.Add(rIdx);
            }
            return map;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool HasAnyNullKey(ISeries[] keys, int row)
        {
            for (int c = 0; c < keys.Length; c++)
            {
                if (keys[c].ValidityMask.IsNull(row)) return true;
            }
            return false;
        }

        private static int ComputeKeyHash(ISeries[] keys, int row)
        {
            var hc = new HashCode();
            for (int c = 0; c < keys.Length; c++)
            {
                ISeries col = keys[c];
                hc.Add(0x1F2E3D4C ^ c);
                switch (col)
                {
                    case Int32Series i32:
                        hc.Add(i32.Memory.Span[row]);
                        break;
                    case Int64Series i64:
                        hc.Add(i64.Memory.Span[row]);
                        break;
                    case Float64Series f64:
                        hc.Add(CanonicalizeDoubleBits(f64.Memory.Span[row]));
                        break;
                    case Float32Series f32:
                        hc.Add(CanonicalizeFloatBits(f32.Memory.Span[row]));
                        break;
                    case BooleanSeries b:
                        hc.Add(b.Memory.Span[row]);
                        break;
                    case Utf8StringSeries u8:
                        hc.AddBytes(u8.GetStringSpan(row));
                        break;
                    case BinarySeries bin:
                        hc.AddBytes(bin.GetSpan(row));
                        break;
                    case DateSeries dt:
                        hc.Add(dt.Memory.Span[row]);
                        break;
                    case DatetimeSeries dtm:
                        hc.Add(dtm.Memory.Span[row]);
                        break;
                    case DurationSeries dur:
                        hc.Add(dur.Memory.Span[row]);
                        break;
                    case TimeSeries tm:
                        hc.Add(tm.Memory.Span[row]);
                        break;
                    case DecimalSeries dec:
                        hc.Add(dec.Memory.Span[row]);
                        break;
                    case Int8Series i8:
                        hc.Add(i8.Memory.Span[row]);
                        break;
                    case Int16Series i16:
                        hc.Add(i16.Memory.Span[row]);
                        break;
                    case UInt8Series ui8:
                        hc.Add(ui8.Memory.Span[row]);
                        break;
                    case UInt16Series ui16:
                        hc.Add(ui16.Memory.Span[row]);
                        break;
                    case UInt32Series ui32:
                        hc.Add(ui32.Memory.Span[row]);
                        break;
                    case UInt64Series ui64:
                        hc.Add(ui64.Memory.Span[row]);
                        break;
                    case EnumSeries en:
                        hc.Add(en.Get(row) as string, StringComparer.Ordinal);
                        break;
                    case CategoricalSeries cat:
                        hc.Add(cat.Get(row) as string, StringComparer.Ordinal);
                        break;
                    default:
                        AddObjectHash(ref hc, col.Get(row));
                        break;
                }
            }
            return hc.ToHashCode();
        }

        private static bool KeysEqual(ISeries[] leftKeys, int leftRow, ISeries[] rightKeys, int rightRow)
        {
            for (int c = 0; c < leftKeys.Length; c++)
            {
                ISeries lCol = leftKeys[c];
                ISeries rCol = rightKeys[c];
                if (lCol.ValidityMask.IsNull(leftRow) || rCol.ValidityMask.IsNull(rightRow))
                    return false;

                bool eq = lCol switch
                {
                    Int32Series li32 => li32.Memory.Span[leftRow] == ((Int32Series)rCol).Memory.Span[rightRow],
                    Int64Series li64 => li64.Memory.Span[leftRow] == ((Int64Series)rCol).Memory.Span[rightRow],
                    Float64Series lf64 => CanonicalizeDoubleBits(lf64.Memory.Span[leftRow]) == CanonicalizeDoubleBits(((Float64Series)rCol).Memory.Span[rightRow]),
                    Float32Series lf32 => CanonicalizeFloatBits(lf32.Memory.Span[leftRow]) == CanonicalizeFloatBits(((Float32Series)rCol).Memory.Span[rightRow]),
                    BooleanSeries lb => lb.Memory.Span[leftRow] == ((BooleanSeries)rCol).Memory.Span[rightRow],
                    Utf8StringSeries lu8 => lu8.GetStringSpan(leftRow).SequenceEqual(((Utf8StringSeries)rCol).GetStringSpan(rightRow)),
                    BinarySeries lbin => lbin.GetSpan(leftRow).SequenceEqual(((BinarySeries)rCol).GetSpan(rightRow)),
                    DateSeries ldt => ldt.Memory.Span[leftRow] == ((DateSeries)rCol).Memory.Span[rightRow],
                    DatetimeSeries ldtm => ldtm.Memory.Span[leftRow] == ((DatetimeSeries)rCol).Memory.Span[rightRow],
                    DurationSeries ldur => ldur.Memory.Span[leftRow] == ((DurationSeries)rCol).Memory.Span[rightRow],
                    TimeSeries ltm => ltm.Memory.Span[leftRow] == ((TimeSeries)rCol).Memory.Span[rightRow],
                    DecimalSeries ldec => ldec.Memory.Span[leftRow] == ((DecimalSeries)rCol).Memory.Span[rightRow],
                    Int8Series li8 => li8.Memory.Span[leftRow] == ((Int8Series)rCol).Memory.Span[rightRow],
                    Int16Series li16 => li16.Memory.Span[leftRow] == ((Int16Series)rCol).Memory.Span[rightRow],
                    UInt8Series lui8 => lui8.Memory.Span[leftRow] == ((UInt8Series)rCol).Memory.Span[rightRow],
                    UInt16Series lui16 => lui16.Memory.Span[leftRow] == ((UInt16Series)rCol).Memory.Span[rightRow],
                    UInt32Series lui32 => lui32.Memory.Span[leftRow] == ((UInt32Series)rCol).Memory.Span[rightRow],
                    UInt64Series lui64 => lui64.Memory.Span[leftRow] == ((UInt64Series)rCol).Memory.Span[rightRow],
                    EnumSeries len => string.Equals(len.Get(leftRow) as string, ((EnumSeries)rCol).Get(rightRow) as string, StringComparison.Ordinal),
                    CategoricalSeries lcat => string.Equals(lcat.Get(leftRow) as string, ((CategoricalSeries)rCol).Get(rightRow) as string, StringComparison.Ordinal),
                    _ => ObjectsEqual(lCol.Get(leftRow), rCol.Get(rightRow))
                };
                if (!eq) return false;
            }
            return true;
        }

        private static int CompareNonNullSeriesElements(ISeries a, int rowA, ISeries b, int rowB) => a switch
        {
            Int32Series i32 => i32.Memory.Span[rowA].CompareTo(((Int32Series)b).Memory.Span[rowB]),
            Int64Series i64 => i64.Memory.Span[rowA].CompareTo(((Int64Series)b).Memory.Span[rowB]),
            Float64Series f64 => f64.Memory.Span[rowA].CompareTo(((Float64Series)b).Memory.Span[rowB]),
            Float32Series f32 => f32.Memory.Span[rowA].CompareTo(((Float32Series)b).Memory.Span[rowB]),
            DateSeries dt => dt.Memory.Span[rowA].CompareTo(((DateSeries)b).Memory.Span[rowB]),
            DatetimeSeries dtm => dtm.Memory.Span[rowA].CompareTo(((DatetimeSeries)b).Memory.Span[rowB]),
            DurationSeries dur => dur.Memory.Span[rowA].CompareTo(((DurationSeries)b).Memory.Span[rowB]),
            TimeSeries tm => tm.Memory.Span[rowA].CompareTo(((TimeSeries)b).Memory.Span[rowB]),
            DecimalSeries dec => dec.Memory.Span[rowA].CompareTo(((DecimalSeries)b).Memory.Span[rowB]),
            Int8Series i8 => i8.Memory.Span[rowA].CompareTo(((Int8Series)b).Memory.Span[rowB]),
            Int16Series i16 => i16.Memory.Span[rowA].CompareTo(((Int16Series)b).Memory.Span[rowB]),
            UInt8Series ui8 => ui8.Memory.Span[rowA].CompareTo(((UInt8Series)b).Memory.Span[rowB]),
            UInt16Series ui16 => ui16.Memory.Span[rowA].CompareTo(((UInt16Series)b).Memory.Span[rowB]),
            UInt32Series ui32 => ui32.Memory.Span[rowA].CompareTo(((UInt32Series)b).Memory.Span[rowB]),
            UInt64Series ui64 => ui64.Memory.Span[rowA].CompareTo(((UInt64Series)b).Memory.Span[rowB]),
            BooleanSeries bl => bl.Memory.Span[rowA].CompareTo(((BooleanSeries)b).Memory.Span[rowB]),
            Utf8StringSeries u8 => u8.GetStringSpan(rowA).SequenceCompareTo(((Utf8StringSeries)b).GetStringSpan(rowB)),
            BinarySeries bin => bin.GetSpan(rowA).SequenceCompareTo(((BinarySeries)b).GetSpan(rowB)),
            _ => Comparer<object?>.Default.Compare(a.Get(rowA), b.Get(rowB))
        };

        private static void AddObjectHash(ref HashCode hc, object? val)
        {
            if (val == null) { hc.Add(0x7F3A91B2); return; }
            if (val is object?[] arr)
            {
                hc.Add(arr.Length);
                for (int i = 0; i < arr.Length; i++) AddObjectHash(ref hc, arr[i]);
                return;
            }
            if (val is IDictionary<string, object?> dict)
            {
                hc.Add(dict.Count);
                foreach (var kvp in dict)
                {
                    hc.Add(kvp.Key);
                    AddObjectHash(ref hc, kvp.Value);
                }
                return;
            }
            if (val is byte[] bytes)
            {
                hc.AddBytes(bytes);
                return;
            }
            hc.Add(val);
        }

        private static bool ObjectsEqual(object? a, object? b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            if (a is object?[] arrA && b is object?[] arrB)
            {
                if (arrA.Length != arrB.Length) return false;
                for (int i = 0; i < arrA.Length; i++)
                    if (!ObjectsEqual(arrA[i], arrB[i])) return false;
                return true;
            }
            if (a is IDictionary<string, object?> dictA && b is IDictionary<string, object?> dictB)
            {
                if (dictA.Count != dictB.Count) return false;
                foreach (var kvp in dictA)
                    if (!dictB.TryGetValue(kvp.Key, out var bVal) || !ObjectsEqual(kvp.Value, bVal))
                        return false;
                return true;
            }
            if (a is byte[] bytesA && b is byte[] bytesB)
                return bytesA.AsSpan().SequenceEqual(bytesB);
            return Equals(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long CanonicalizeDoubleBits(double val)
        {
            if (val == 0.0) return 0L;
            return double.IsNaN(val) ? 0x7FF8000000000000L : BitConverter.DoubleToInt64Bits(val);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CanonicalizeFloatBits(float val)
        {
            if (val == 0.0f) return 0;
            return float.IsNaN(val) ? 0x7FC00000 : BitConverter.SingleToInt32Bits(val);
        }
    }
}
