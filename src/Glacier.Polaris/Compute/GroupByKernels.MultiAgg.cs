using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class GroupByKernels
    {
        internal struct MultiAggEntry
        {
            public int Key;
            public double Sum;
            public double Min;
            public double Max;
            public int Count;
            public byte Occupied;
        }

        internal sealed class LocalMultiAggMap
        {
            public MultiAggEntry[] Table;
            public int[] Order;
            public int OrderCount;
            private int _capacity;
            private uint _mask;

            public LocalMultiAggMap(int initialCapacity = 2048)
            {
                _capacity = initialCapacity;
                _mask = (uint)(_capacity - 1);
                Table = new MultiAggEntry[_capacity];
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
                        if (v < Table[slot].Min) Table[slot].Min = v;
                        if (v > Table[slot].Max) Table[slot].Max = v;
                        return;
                    }
                    slot = (slot + 1) & _mask;
                }

                Table[slot].Occupied = 1;
                Table[slot].Key = k;
                Table[slot].Sum = v;
                Table[slot].Min = v;
                Table[slot].Max = v;
                Table[slot].Count = 1;

                if (OrderCount >= Order.Length)
                    Array.Resize(ref Order, Order.Length * 2);
                Order[OrderCount++] = k;

                if (OrderCount * 2 > _capacity)
                    Grow();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Merge(int k, double sum, double min, double max, int count)
            {
                uint slot = ((uint)k * 2654435761u) & _mask;
                while (Table[slot].Occupied != 0)
                {
                    if (Table[slot].Key == k)
                    {
                        Table[slot].Sum += sum;
                        Table[slot].Count += count;
                        if (min < Table[slot].Min) Table[slot].Min = min;
                        if (max > Table[slot].Max) Table[slot].Max = max;
                        return;
                    }
                    slot = (slot + 1) & _mask;
                }

                Table[slot].Occupied = 1;
                Table[slot].Key = k;
                Table[slot].Sum = sum;
                Table[slot].Min = min;
                Table[slot].Max = max;
                Table[slot].Count = count;

                if (OrderCount >= Order.Length)
                    Array.Resize(ref Order, Order.Length * 2);
                Order[OrderCount++] = k;

                if (OrderCount * 2 > _capacity)
                    Grow();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ref MultiAggEntry GetRef(int k)
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
                var newTable = new MultiAggEntry[_capacity];
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

        public static DataFrame GroupByMultiAggF64Fast(Int32Series keys, Float64Series values)
        {
            int rowCount = keys.Length;
            if (rowCount == 0)
            {
                return new DataFrame(new ISeries[] {
                    new Int32Series("key", 0),
                    new Float64Series("a_sum", 0),
                    new Float64Series("a_mean", 0),
                    new Float64Series("a_min", 0),
                    new Float64Series("a_max", 0),
                    new Int32Series("a_count", 0)
                });
            }

            if (rowCount >= 100_000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, rowCount / 32768));
                int chunkSize = (rowCount + numChunks - 1) / numChunks;
                var localMaps = new LocalMultiAggMap[numChunks];

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

                            var local = new LocalMultiAggMap(2048);
                            int end = start + length;
                            for (int i = start; i < end; i++)
                            {
                                local.Add(kPtr[i], vPtr[i]);
                            }
                            localMaps[c] = local;
                        });
                    }
                }

                var globalMap = new LocalMultiAggMap(Math.Max(2048, localMaps[0]?.OrderCount * 2 ?? 2048));
                for (int c = 0; c < numChunks; c++)
                {
                    var local = localMaps[c];
                    if (local == null) continue;
                    for (int i = 0; i < local.OrderCount; i++)
                    {
                        int k = local.Order[i];
                        ref var entry = ref local.GetRef(k);
                        globalMap.Merge(k, entry.Sum, entry.Min, entry.Max, entry.Count);
                    }
                }

                int groupCount = globalMap.OrderCount;
                var keyCol = new Int32Series("key", groupCount);
                var sumCol = new Float64Series("a_sum", groupCount);
                var meanCol = new Float64Series("a_mean", groupCount);
                var minCol = new Float64Series("a_min", groupCount);
                var maxCol = new Float64Series("a_max", groupCount);
                var countCol = new Int32Series("a_count", groupCount);

                for (int i = 0; i < groupCount; i++)
                {
                    int k = globalMap.Order[i];
                    ref var entry = ref globalMap.GetRef(k);
                    keyCol.Memory.Span[i] = k;
                    sumCol.Memory.Span[i] = entry.Sum;
                    meanCol.Memory.Span[i] = entry.Count > 0 ? entry.Sum / entry.Count : 0;
                    minCol.Memory.Span[i] = entry.Min;
                    maxCol.Memory.Span[i] = entry.Max;
                    countCol.Memory.Span[i] = entry.Count;
                }

                return new DataFrame(new ISeries[] { keyCol, sumCol, meanCol, minCol, maxCol, countCol });
            }

            var singleMap = new LocalMultiAggMap(2048);
            var keySpan = keys.Memory.Span;
            var valSpan = values.Memory.Span;
            for (int i = 0; i < rowCount; i++)
            {
                singleMap.Add(keySpan[i], valSpan[i]);
            }

            int singleCount = singleMap.OrderCount;
            var sKeyCol = new Int32Series("key", singleCount);
            var sSumCol = new Float64Series("a_sum", singleCount);
            var sMeanCol = new Float64Series("a_mean", singleCount);
            var sMinCol = new Float64Series("a_min", singleCount);
            var sMaxCol = new Float64Series("a_max", singleCount);
            var sCountCol = new Int32Series("a_count", singleCount);

            for (int i = 0; i < singleCount; i++)
            {
                int k = singleMap.Order[i];
                ref var entry = ref singleMap.GetRef(k);
                sKeyCol.Memory.Span[i] = k;
                sSumCol.Memory.Span[i] = entry.Sum;
                sMeanCol.Memory.Span[i] = entry.Count > 0 ? entry.Sum / entry.Count : 0;
                sMinCol.Memory.Span[i] = entry.Min;
                sMaxCol.Memory.Span[i] = entry.Max;
                sCountCol.Memory.Span[i] = entry.Count;
            }

            return new DataFrame(new ISeries[] { sKeyCol, sSumCol, sMeanCol, sMinCol, sMaxCol, sCountCol });
        }
    }
}
