using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Data;

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
        private static JoinResult InnerJoinSmallRight(Int32Series left, Int32Series right)
        {
            var rightSpan = right.Memory.Span;
            var leftSpan = left.Memory.Span;

            int rightLen = rightSpan.Length;
            if (rightLen == 0 || leftSpan.Length == 0)
                return new JoinResult { LeftIndices = Array.Empty<int>(), RightIndices = Array.Empty<int>() };

            // 1. Build a flat, allocation-free hash map
            // Find next power of 2, then double it to keep the load factor < 0.5 for fewer collisions
            int bucketCount = 1;
            while (bucketCount <= rightLen) bucketCount <<= 1;
            bucketCount <<= 1;
            uint bucketMask = (uint)(bucketCount - 1);

            // 'buckets' acts as the Dictionary Keys, 'next' acts as the List<int>
            var buckets = new int[bucketCount];
            Array.Fill(buckets, -1);
            var next = new int[rightLen];

            // Populate the hash map
            for (int i = 0; i < rightLen; i++)
            {
                int val = rightSpan[i];

                // Fibonacci hashing for 32-bit integers (super fast, prevents clustering)
                uint bucket = ((uint)val * 2654435761u) & bucketMask;

                next[i] = buckets[bucket];
                buckets[bucket] = i;
            }

            // 2. First pass: count total matches
            int totalMatches = 0;
            for (int i = 0; i < leftSpan.Length; i++)
            {
                int val = leftSpan[i];
                uint bucket = ((uint)val * 2654435761u) & bucketMask;
                int rIdx = buckets[bucket];

                while (rIdx >= 0)
                {
                    if (rightSpan[rIdx] == val)
                        totalMatches++;
                    rIdx = next[rIdx];
                }
            }

            // Allocate exact-sized output arrays
            var leftResult = new int[totalMatches];
            var rightResult = new int[totalMatches];

            // 3. Second pass: fill results
            int pos = 0;
            for (int i = 0; i < leftSpan.Length; i++)
            {
                int val = leftSpan[i];
                uint bucket = ((uint)val * 2654435761u) & bucketMask;
                int rIdx = buckets[bucket];

                while (rIdx >= 0)
                {
                    if (rightSpan[rIdx] == val)
                    {
                        leftResult[pos] = i;
                        rightResult[pos] = rIdx;
                        pos++;
                    }
                    rIdx = next[rIdx];
                }
            }

            return new JoinResult { LeftIndices = leftResult, RightIndices = rightResult };
        }

        /// <summary>
        /// Fast-path left join optimized for small right-side tables.
        /// Automatically selected when right table is &lt; 10K rows.
        /// </summary>
        private static JoinResult LeftJoinSmallRight(Int32Series left, Int32Series right)
        {
            var rightSpan = right.Memory.Span;
            var leftSpan = left.Memory.Span;

            // Build hash map from right table
            var rightMap = new Dictionary<int, List<int>>(rightSpan.Length);
            for (int i = 0; i < rightSpan.Length; i++)
            {
                int val = rightSpan[i];
                if (!rightMap.TryGetValue(val, out var list))
                    rightMap[val] = new List<int> { i };
                else
                    list.Add(i);
            }

            // First pass: count total matches
            int totalMatches = 0;
            for (int i = 0; i < leftSpan.Length; i++)
            {
                if (rightMap.TryGetValue(leftSpan[i], out var list))
                    totalMatches += list.Count;
                else
                    totalMatches++; // unmatched left row
            }

            var leftResult = new int[totalMatches];
            var rightResult = new int[totalMatches];

            int pos = 0;
            for (int i = 0; i < leftSpan.Length; i++)
            {
                if (rightMap.TryGetValue(leftSpan[i], out var list))
                {
                    foreach (int rIdx in list)
                    {
                        leftResult[pos] = i;
                        rightResult[pos] = rIdx;
                        pos++;
                    }
                }
                else
                {
                    leftResult[pos] = i;
                    rightResult[pos] = -1;
                    pos++;
                }
            }

            return new JoinResult { LeftIndices = leftResult, RightIndices = rightResult };
        }
    }
}
