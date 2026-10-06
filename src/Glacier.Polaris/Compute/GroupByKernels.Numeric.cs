using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class GroupByKernels
    {
        public static DataFrame GroupBySumInt32(Int32Series keys, Int32Series values)
        {
            return GroupBySumInt32Fast(keys, values);
        }

        /// <summary>
        /// Single-pass GroupBy + Sum for Int32 keys and Float64 values.
        /// </summary>
        public static DataFrame GroupBySumF64(Int32Series keys, Float64Series values)
        {
            int rowCount = keys.Length;

            var indices = SortKernels.ArgSort(keys.Memory.Span);
            var uniqueKeys = new List<int>();
            uniqueKeys.Add(keys.Memory.Span[indices[0]]);
            for (int i = 1; i < indices.Length; i++)
            {
                if (keys.Memory.Span[indices[i]] != keys.Memory.Span[indices[i - 1]])
                    uniqueKeys.Add(keys.Memory.Span[indices[i]]);
            }

            int groupCount = uniqueKeys.Count;
            var keyCol = new Int32Series("key", groupCount);
            var sumCol = new Float64Series("a_sum", groupCount);

            var keyToIndex = new Dictionary<int, int>(groupCount);
            for (int i = 0; i < groupCount; i++)
            {
                keyToIndex[uniqueKeys[i]] = i;
                keyCol.Memory.Span[i] = uniqueKeys[i];
                sumCol.Memory.Span[i] = 0;
            }

            var keySpan = keys.Memory.Span;
            var valSpan = values.Memory.Span;
            for (int i = 0; i < rowCount; i++)
            {
                if (keyToIndex.TryGetValue(keySpan[i], out int idx))
                    sumCol.Memory.Span[idx] += valSpan[i];
            }

            return new DataFrame(new ISeries[] { keyCol, sumCol });
        }

        public static DataFrame GroupByMeanF64(Int32Series keys, Float64Series values)
        {
            return GroupByMeanF64Fast(keys, values);
        }

        internal struct SumInt32Entry
        {
            public int Key;
            public long Sum;
            public byte Occupied;
        }

        internal sealed class LocalSumInt32Map
        {
            public SumInt32Entry[] Table;
            public int[] Order;
            public int OrderCount;
            private int _capacity;
            private uint _mask;

            public LocalSumInt32Map(int initialCapacity = 2048)
            {
                _capacity = initialCapacity;
                _mask = (uint)(_capacity - 1);
                Table = new SumInt32Entry[_capacity];
                Order = new int[_capacity];
                OrderCount = 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Add(int k, int v)
            {
                uint slot = ((uint)k * 2654435761u) & _mask;
                while (Table[slot].Occupied != 0)
                {
                    if (Table[slot].Key == k)
                    {
                        Table[slot].Sum += v;
                        return;
                    }
                    slot = (slot + 1) & _mask;
                }

                Table[slot].Occupied = 1;
                Table[slot].Key = k;
                Table[slot].Sum = v;

                if (OrderCount >= Order.Length)
                    Array.Resize(ref Order, Order.Length * 2);
                Order[OrderCount++] = k;

                if (OrderCount * 2 > _capacity)
                    Grow();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Merge(int k, long sum)
            {
                uint slot = ((uint)k * 2654435761u) & _mask;
                while (Table[slot].Occupied != 0)
                {
                    if (Table[slot].Key == k)
                    {
                        Table[slot].Sum += sum;
                        return;
                    }
                    slot = (slot + 1) & _mask;
                }

                Table[slot].Occupied = 1;
                Table[slot].Key = k;
                Table[slot].Sum = sum;

                if (OrderCount >= Order.Length)
                    Array.Resize(ref Order, Order.Length * 2);
                Order[OrderCount++] = k;

                if (OrderCount * 2 > _capacity)
                    Grow();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ref SumInt32Entry GetRef(int k)
            {
                uint slot = ((uint)k * 2654435761u) & _mask;
                while (Table[slot].Occupied != 0)
                {
                    if (Table[slot].Key == k) return ref Table[slot];
                    slot = (slot + 1) & _mask;
                }
                throw new KeyNotFoundException();
            }

            private void Grow()
            {
                int oldCap = _capacity;
                _capacity *= 2;
                _mask = (uint)(_capacity - 1);
                var newTable = new SumInt32Entry[_capacity];
                for (int i = 0; i < oldCap; i++)
                {
                    if (Table[i].Occupied != 0)
                    {
                        int k = Table[i].Key;
                        uint slot = ((uint)k * 2654435761u) & _mask;
                        while (newTable[slot].Occupied != 0)
                            slot = (slot + 1) & _mask;
                        newTable[slot] = Table[i];
                    }
                }
                Table = newTable;
            }
        }

        internal struct MeanF64Entry
        {
            public int Key;
            public double Sum;
            public int Count;
            public byte Occupied;
        }

        internal sealed class LocalMeanF64Map
        {
            public MeanF64Entry[] Table;
            public int[] Order;
            public int OrderCount;
            private int _capacity;
            private uint _mask;

            public LocalMeanF64Map(int initialCapacity = 2048)
            {
                _capacity = initialCapacity;
                _mask = (uint)(_capacity - 1);
                Table = new MeanF64Entry[_capacity];
                Order = new int[_capacity];
                OrderCount = 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Add(int k, double v)
            {
                uint slot = ((uint)k * 2654435761u) & _mask;
                while (Table[slot].Occupied != 0)
                {
                    if (Table[slot].Key == k)
                    {
                        Table[slot].Sum += v;
                        Table[slot].Count++;
                        return;
                    }
                    slot = (slot + 1) & _mask;
                }

                Table[slot].Occupied = 1;
                Table[slot].Key = k;
                Table[slot].Sum = v;
                Table[slot].Count = 1;

                if (OrderCount >= Order.Length)
                    Array.Resize(ref Order, Order.Length * 2);
                Order[OrderCount++] = k;

                if (OrderCount * 2 > _capacity)
                    Grow();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Merge(int k, double sum, int count)
            {
                uint slot = ((uint)k * 2654435761u) & _mask;
                while (Table[slot].Occupied != 0)
                {
                    if (Table[slot].Key == k)
                    {
                        Table[slot].Sum += sum;
                        Table[slot].Count += count;
                        return;
                    }
                    slot = (slot + 1) & _mask;
                }

                Table[slot].Occupied = 1;
                Table[slot].Key = k;
                Table[slot].Sum = sum;
                Table[slot].Count = count;

                if (OrderCount >= Order.Length)
                    Array.Resize(ref Order, Order.Length * 2);
                Order[OrderCount++] = k;

                if (OrderCount * 2 > _capacity)
                    Grow();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ref MeanF64Entry GetRef(int k)
            {
                uint slot = ((uint)k * 2654435761u) & _mask;
                while (Table[slot].Occupied != 0)
                {
                    if (Table[slot].Key == k) return ref Table[slot];
                    slot = (slot + 1) & _mask;
                }
                throw new KeyNotFoundException();
            }

            private void Grow()
            {
                int oldCap = _capacity;
                _capacity *= 2;
                _mask = (uint)(_capacity - 1);
                var newTable = new MeanF64Entry[_capacity];
                for (int i = 0; i < oldCap; i++)
                {
                    if (Table[i].Occupied != 0)
                    {
                        int k = Table[i].Key;
                        uint slot = ((uint)k * 2654435761u) & _mask;
                        while (newTable[slot].Occupied != 0)
                            slot = (slot + 1) & _mask;
                        newTable[slot] = Table[i];
                    }
                }
                Table = newTable;
            }
        }

        public static DataFrame GroupBySumInt32Fast(Int32Series keys, Int32Series values)
        {
            int rowCount = keys.Length;
            if (rowCount == 0)
            {
                return new DataFrame(new ISeries[] { new Int32Series("key", 0), new Int32Series("a_sum", 0) });
            }

            if (rowCount >= 100_000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, rowCount / 32768));
                int chunkSize = (rowCount + numChunks - 1) / numChunks;
                var localMaps = new LocalSumInt32Map[numChunks];

                unsafe
                {
                    fixed (int* pKeys = keys.Memory.Span)
                    fixed (int* pVals = values.Memory.Span)
                    {
                        int* kPtr = pKeys;
                        int* vPtr = pVals;

                        System.Threading.Tasks.Parallel.For(0, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int length = Math.Min(chunkSize, rowCount - start);
                            if (length <= 0) return;

                            var local = new LocalSumInt32Map(2048);
                            int end = start + length;
                            for (int i = start; i < end; i++)
                            {
                                local.Add(kPtr[i], vPtr[i]);
                            }
                            localMaps[c] = local;
                        });
                    }
                }

                var globalMap = new LocalSumInt32Map(Math.Max(2048, localMaps[0]?.OrderCount * 2 ?? 2048));
                for (int c = 0; c < numChunks; c++)
                {
                    var local = localMaps[c];
                    if (local == null) continue;
                    for (int i = 0; i < local.OrderCount; i++)
                    {
                        int k = local.Order[i];
                        ref var entry = ref local.GetRef(k);
                        globalMap.Merge(k, entry.Sum);
                    }
                }

                int groupCount = globalMap.OrderCount;
                var keyCol = new Int32Series("key", groupCount);
                var sumCol = new Int32Series("a_sum", groupCount);
                for (int i = 0; i < groupCount; i++)
                {
                    int k = globalMap.Order[i];
                    keyCol.Memory.Span[i] = k;
                    sumCol.Memory.Span[i] = (int)globalMap.GetRef(k).Sum;
                }
                return new DataFrame(new ISeries[] { keyCol, sumCol });
            }

            var singleMap = new LocalSumInt32Map(2048);
            var keySpan = keys.Memory.Span;
            var valSpan = values.Memory.Span;
            for (int i = 0; i < rowCount; i++)
            {
                singleMap.Add(keySpan[i], valSpan[i]);
            }
            int singleCount = singleMap.OrderCount;
            var sKeyCol = new Int32Series("key", singleCount);
            var sSumCol = new Int32Series("a_sum", singleCount);
            for (int i = 0; i < singleCount; i++)
            {
                int k = singleMap.Order[i];
                sKeyCol.Memory.Span[i] = k;
                sSumCol.Memory.Span[i] = (int)singleMap.GetRef(k).Sum;
            }
            return new DataFrame(new ISeries[] { sKeyCol, sSumCol });
        }

        public static DataFrame GroupByMeanF64Fast(Int32Series keys, Float64Series values)
        {
            int rowCount = keys.Length;
            if (rowCount == 0)
            {
                return new DataFrame(new ISeries[] { new Int32Series("key", 0), new Float64Series("a_mean", 0) });
            }

            if (rowCount >= 100_000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, rowCount / 32768));
                int chunkSize = (rowCount + numChunks - 1) / numChunks;
                var localMaps = new LocalMeanF64Map[numChunks];

                unsafe
                {
                    fixed (int* pKeys = keys.Memory.Span)
                    fixed (double* pVals = values.Memory.Span)
                    {
                        int* kPtr = pKeys;
                        double* vPtr = pVals;

                        System.Threading.Tasks.Parallel.For(0, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int length = Math.Min(chunkSize, rowCount - start);
                            if (length <= 0) return;

                            var local = new LocalMeanF64Map(2048);
                            int end = start + length;
                            for (int i = start; i < end; i++)
                            {
                                local.Add(kPtr[i], vPtr[i]);
                            }
                            localMaps[c] = local;
                        });
                    }
                }

                var globalMap = new LocalMeanF64Map(Math.Max(2048, localMaps[0]?.OrderCount * 2 ?? 2048));
                for (int c = 0; c < numChunks; c++)
                {
                    var local = localMaps[c];
                    if (local == null) continue;
                    for (int i = 0; i < local.OrderCount; i++)
                    {
                        int k = local.Order[i];
                        ref var entry = ref local.GetRef(k);
                        globalMap.Merge(k, entry.Sum, entry.Count);
                    }
                }

                int groupCount = globalMap.OrderCount;
                var keyCol = new Int32Series("key", groupCount);
                var meanCol = new Float64Series("a_mean", groupCount);
                for (int i = 0; i < groupCount; i++)
                {
                    int k = globalMap.Order[i];
                    ref var entry = ref globalMap.GetRef(k);
                    keyCol.Memory.Span[i] = k;
                    meanCol.Memory.Span[i] = entry.Count > 0 ? entry.Sum / entry.Count : 0;
                }
                return new DataFrame(new ISeries[] { keyCol, meanCol });
            }

            var singleMap = new LocalMeanF64Map(2048);
            var keySpan = keys.Memory.Span;
            var valSpan = values.Memory.Span;
            for (int i = 0; i < rowCount; i++)
            {
                singleMap.Add(keySpan[i], valSpan[i]);
            }
            int singleCount = singleMap.OrderCount;
            var sKeyCol = new Int32Series("key", singleCount);
            var sMeanCol = new Float64Series("a_mean", singleCount);
            for (int i = 0; i < singleCount; i++)
            {
                int k = singleMap.Order[i];
                ref var entry = ref singleMap.GetRef(k);
                sKeyCol.Memory.Span[i] = k;
                sMeanCol.Memory.Span[i] = entry.Count > 0 ? entry.Sum / entry.Count : 0;
            }
            return new DataFrame(new ISeries[] { sKeyCol, sMeanCol });
        }
    }
}
