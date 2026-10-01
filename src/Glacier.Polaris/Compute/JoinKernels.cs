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
            // Fast path for small right-side tables (< 10K rows): avoid partition overhead
            if (right.Length < 10000)
                return InnerJoinSmallRight(left, right);
            const int Partitions = 64;
            var leftBuckets = Bucketize(left.Memory.Span, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBuckets(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span);
                if (res.left.Length > 0) results.Add(res);
            });

            var flattened = FlattenPairs(results);
            SortByLeftIndices(ref flattened);
            return flattened;
        }
        public static JoinResult LeftJoin(Int32Series left, Int32Series right)
        {
            // Fast path for small right-side tables (< 10K rows): avoid partition overhead
            if (right.Length < 10000)
                return LeftJoinSmallRight(left, right);
            const int Partitions = 64;
            var leftBuckets = Bucketize(left.Memory.Span, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBucketsLeft(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span);
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
            var leftBuckets = Bucketize(left.Memory.Span, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBucketsOuter(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span);
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
            var leftBuckets = Bucketize(left.Memory.Span, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBucketsSemi(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span);
                if (res.left.Length > 0) results.Add(res);
            });

            var flattened = FlattenPairs(results);
            SortByLeftIndices(ref flattened);
            return flattened;
        }

        public static JoinResult AntiJoin(Int32Series left, Int32Series right)
        {
            const int Partitions = 64;
            var leftBuckets = Bucketize(left.Memory.Span, Partitions);
            var rightBuckets = Bucketize(right.Memory.Span, Partitions);

            var results = new System.Collections.Concurrent.ConcurrentBag<(int[] left, int[] right)>();

            System.Threading.Tasks.Parallel.For(0, Partitions, p =>
            {
                var res = JoinBucketsAnti(leftBuckets[p], rightBuckets[p], left.Memory.Span, right.Memory.Span);
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

        private static List<int>[] Bucketize(ReadOnlySpan<int> data, int count)
        {
            var buckets = new List<int>[count];
            for (int i = 0; i < count; i++) buckets[i] = new List<int>();

            for (int i = 0; i < data.Length; i++)
            {
                int bucket = (int)((uint)data[i].GetHashCode() % (uint)count);
                buckets[bucket].Add(i);
            }
            return buckets;
        }

        private static (int[] left, int[] right) JoinBuckets(List<int> leftIndices, List<int> rightIndices, ReadOnlySpan<int> leftData, ReadOnlySpan<int> rightData)
        {
            if (leftIndices.Count == 0 || rightIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            var map = new Dictionary<int, List<int>>(leftIndices.Count);
            foreach (var idx in leftIndices)
            {
                int val = leftData[idx];
                if (!map.TryGetValue(val, out var list)) map[val] = list = new List<int>();
                list.Add(idx);
            }

            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in rightIndices)
            {
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

        private static (int[] left, int[] right) JoinBucketsLeft(List<int> leftIndices, List<int> rightIndices, ReadOnlySpan<int> leftData, ReadOnlySpan<int> rightData)
        {
            if (leftIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            var map = new Dictionary<int, List<int>>(rightIndices.Count);
            foreach (var idx in rightIndices)
            {
                int val = rightData[idx];
                if (!map.TryGetValue(val, out var list)) map[val] = list = new List<int>();
                list.Add(idx);
            }

            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in leftIndices)
            {
                int val = leftData[idx];
                if (map.TryGetValue(val, out var matchingRight))
                {
                    foreach (var rIdx in matchingRight)
                    {
                        lResult.Add(idx);
                        rResult.Add(rIdx);
                    }
                }
                else
                {
                    lResult.Add(idx);
                    rResult.Add(-1);
                }
            }

            return (lResult.ToArray(), rResult.ToArray());
        }

        private static (int[] left, int[] right) JoinBucketsOuter(List<int> leftIndices, List<int> rightIndices, ReadOnlySpan<int> leftData, ReadOnlySpan<int> rightData)
        {
            if (leftIndices.Count == 0 && rightIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            var map = new Dictionary<int, List<int>>(rightIndices.Count);
            foreach (var idx in rightIndices)
            {
                int val = rightData[idx];
                if (!map.TryGetValue(val, out var list)) map[val] = list = new List<int>();
                list.Add(idx);
            }

            var rightVisited = new HashSet<int>();
            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in leftIndices)
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
                }
                else
                {
                    lResult.Add(idx);
                    rResult.Add(-1);
                }
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

        private static (int[] left, int[] right) JoinBucketsSemi(List<int> leftIndices, List<int> rightIndices, ReadOnlySpan<int> leftData, ReadOnlySpan<int> rightData)
        {
            if (leftIndices.Count == 0 || rightIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());

            var rightKeys = new HashSet<int>(rightIndices.Count);
            foreach (var idx in rightIndices) rightKeys.Add(rightData[idx]);

            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in leftIndices)
            {
                if (rightKeys.Contains(leftData[idx]))
                {
                    lResult.Add(idx);
                    rResult.Add(-1);
                }
            }

            return (lResult.ToArray(), rResult.ToArray());
        }

        private static (int[] left, int[] right) JoinBucketsAnti(List<int> leftIndices, List<int> rightIndices, ReadOnlySpan<int> leftData, ReadOnlySpan<int> rightData)
        {
            if (leftIndices.Count == 0) return (Array.Empty<int>(), Array.Empty<int>());
            if (rightIndices.Count == 0) return (leftIndices.ToArray(), Enumerable.Repeat(-1, leftIndices.Count).ToArray());

            var rightKeys = new HashSet<int>(rightIndices.Count);
            foreach (var idx in rightIndices) rightKeys.Add(rightData[idx]);

            var lResult = new List<int>();
            var rResult = new List<int>();

            foreach (var idx in leftIndices)
            {
                if (!rightKeys.Contains(leftData[idx]))
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
    }
}
