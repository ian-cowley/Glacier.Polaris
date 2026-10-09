using System;
using System.Buffers;
using System.Linq;
using Glacier.Polaris.Compute;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class FilterKernelsTests
    {
        [Fact]
        public void FilterKernels_EmptyAndSingleElement()
        {
            var empty = ReadOnlySpan<long>.Empty;
            var (emptyIdx, emptyCnt) = FilterKernels.Filter<long>(empty, 10L, FilterOperation.GreaterThan);
            Assert.Equal(0, emptyCnt);
            ArrayPool<int>.Shared.Return(emptyIdx);

            long[] single = [42L];
            var (sIdx1, sCnt1) = FilterKernels.Filter<long>(single, 40L, FilterOperation.GreaterThan);
            Assert.Equal(1, sCnt1);
            Assert.Equal(0, sIdx1[0]);
            ArrayPool<int>.Shared.Return(sIdx1);

            var (sIdx2, sCnt2) = FilterKernels.Filter<long>(single, 50L, FilterOperation.GreaterThan);
            Assert.Equal(0, sCnt2);
            ArrayPool<int>.Shared.Return(sIdx2);
        }

        [Theory]
        [InlineData(FilterOperation.Equal)]
        [InlineData(FilterOperation.NotEqual)]
        [InlineData(FilterOperation.GreaterThan)]
        [InlineData(FilterOperation.GreaterThanOrEqual)]
        [InlineData(FilterOperation.LessThan)]
        [InlineData(FilterOperation.LessThanOrEqual)]
        public void FilterKernels_SmallData_AllOperations(FilterOperation op)
        {
            int[] data = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50];
            int threshold = 25;

            var (idx, cnt) = FilterKernels.Filter<int>(data, threshold, op);
            try
            {
                var expected = data
                    .Select((v, i) => (v, i))
                    .Where(pair => op switch
                    {
                        FilterOperation.Equal => pair.v == threshold,
                        FilterOperation.NotEqual => pair.v != threshold,
                        FilterOperation.GreaterThan => pair.v > threshold,
                        FilterOperation.GreaterThanOrEqual => pair.v >= threshold,
                        FilterOperation.LessThan => pair.v < threshold,
                        FilterOperation.LessThanOrEqual => pair.v <= threshold,
                        _ => false
                    })
                    .Select(pair => pair.i)
                    .ToArray();

                Assert.Equal(expected.Length, cnt);
                for (int i = 0; i < cnt; i++)
                {
                    Assert.Equal(expected[i], idx[i]);
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(idx);
            }
        }

        [Fact]
        public void FilterKernels_ParallelSIMD_LargeData_GreaterThan()
        {
            const int n = 300_000;
            long[] data = new long[n];
            for (int i = 0; i < n; i++)
            {
                data[i] = i;
            }

            long threshold = 150_000L;
            var (idx, cnt) = FilterKernels.Filter<long>(data, threshold, FilterOperation.GreaterThan);

            try
            {
                int expectedCount = n - 150_001; // 150,001 to 299,999 inclusive = 149,999 items
                Assert.Equal(expectedCount, cnt);

                for (int i = 0; i < cnt; i++)
                {
                    int expectedIndex = 150_001 + i;
                    Assert.Equal(expectedIndex, idx[i]);
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(idx);
            }
        }

        [Fact]
        public void FilterKernels_AllMatch_FastPath()
        {
            // Triggers Fast Path 2: word == ulong.MaxValue with four Vector512<int> stores
            const int n = 200_000;
            int[] data = new int[n];
            Array.Fill(data, 100);

            var (idx, cnt) = FilterKernels.Filter<int>(data, 50, FilterOperation.GreaterThan);

            try
            {
                Assert.Equal(n, cnt);
                for (int i = 0; i < n; i++)
                {
                    Assert.Equal(i, idx[i]);
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(idx);
            }
        }

        [Fact]
        public void FilterKernels_NoneMatch_FastPath()
        {
            // Triggers Fast Path 1: word == 0UL instant 64-row skip
            const int n = 200_000;
            int[] data = new int[n];
            Array.Fill(data, 10);

            var (idx, cnt) = FilterKernels.Filter<int>(data, 50, FilterOperation.GreaterThan);

            try
            {
                Assert.Equal(0, cnt);
            }
            finally
            {
                ArrayPool<int>.Shared.Return(idx);
            }
        }

        [Fact]
        public void FilterKernels_MixedBitmask_TrailingZeroCount()
        {
            // Triggers General Path: mixed bits in word
            const int n = 250_000;
            long[] data = new long[n];
            int expectedCount = 0;
            for (int i = 0; i < n; i++)
            {
                data[i] = (i % 3 == 0) ? 100L : 10L;
                if (i % 3 == 0) expectedCount++;
            }

            var (idx, cnt) = FilterKernels.Filter<long>(data, 50L, FilterOperation.GreaterThan);

            try
            {
                Assert.Equal(expectedCount, cnt);
                int expectedIdx = 0;
                for (int i = 0; i < cnt; i++)
                {
                    Assert.Equal(expectedIdx, idx[i]);
                    expectedIdx += 3;
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(idx);
            }
        }

        [Fact]
        public void FilterKernels_AllNumericTypes_Coverage()
        {
            const int n = 200_000;

            // Float64 / double
            double[] dData = new double[n];
            for (int i = 0; i < n; i++) dData[i] = i * 0.5;
            var (dIdx, dCnt) = FilterKernels.Filter<double>(dData, 50000.0, FilterOperation.GreaterThan);
            Assert.Equal(n - 100001, dCnt);
            ArrayPool<int>.Shared.Return(dIdx);

            // Float32 / float
            float[] fData = new float[n];
            for (int i = 0; i < n; i++) fData[i] = i * 0.5f;
            var (fIdx, fCnt) = FilterKernels.Filter<float>(fData, 50000.0f, FilterOperation.GreaterThan);
            Assert.Equal(n - 100001, fCnt);
            ArrayPool<int>.Shared.Return(fIdx);

            // UInt32
            uint[] u32Data = new uint[n];
            for (int i = 0; i < n; i++) u32Data[i] = (uint)i;
            var (u32Idx, u32Cnt) = FilterKernels.Filter<uint>(u32Data, 100000u, FilterOperation.GreaterThan);
            Assert.Equal(n - 100001, u32Cnt);
            ArrayPool<int>.Shared.Return(u32Idx);

            // UInt64
            ulong[] u64Data = new ulong[n];
            for (int i = 0; i < n; i++) u64Data[i] = (ulong)i;
            var (u64Idx, u64Cnt) = FilterKernels.Filter<ulong>(u64Data, 100000ul, FilterOperation.GreaterThan);
            Assert.Equal(n - 100001, u64Cnt);
            ArrayPool<int>.Shared.Return(u64Idx);

            // Int16 / short
            short[] s16Data = new short[n];
            for (int i = 0; i < n; i++) s16Data[i] = (short)(i % 1000);
            var (s16Idx, s16Cnt) = FilterKernels.Filter<short>(s16Data, (short)500, FilterOperation.GreaterThan);
            Assert.True(s16Cnt > 0);
            ArrayPool<int>.Shared.Return(s16Idx);

            // UInt16 / ushort
            ushort[] u16Data = new ushort[n];
            for (int i = 0; i < n; i++) u16Data[i] = (ushort)(i % 1000);
            var (u16Idx, u16Cnt) = FilterKernels.Filter<ushort>(u16Data, (ushort)500, FilterOperation.GreaterThan);
            Assert.True(u16Cnt > 0);
            ArrayPool<int>.Shared.Return(u16Idx);

            // Int8 / sbyte
            sbyte[] s8Data = new sbyte[n];
            for (int i = 0; i < n; i++) s8Data[i] = (sbyte)(i % 100);
            var (s8Idx, s8Cnt) = FilterKernels.Filter<sbyte>(s8Data, (sbyte)50, FilterOperation.GreaterThan);
            Assert.True(s8Cnt > 0);
            ArrayPool<int>.Shared.Return(s8Idx);

            // UInt8 / byte
            byte[] u8Data = new byte[n];
            for (int i = 0; i < n; i++) u8Data[i] = (byte)(i % 100);
            var (u8Idx, u8Cnt) = FilterKernels.Filter<byte>(u8Data, (byte)50, FilterOperation.GreaterThan);
            Assert.True(u8Cnt > 0);
            ArrayPool<int>.Shared.Return(u8Idx);
        }
    }
}
