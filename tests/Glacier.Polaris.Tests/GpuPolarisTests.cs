using System;
using System.Threading.Tasks;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests;

[CollectionDefinition("Gpu", DisableParallelization = true)]
public sealed class GpuCollectionDefinition
{
}

[Collection("Gpu")]
public class GpuPolarisTests
{
    [Fact]
    [Trait("Hardware", "Gpu")]
    public void GpuAvailability_CanBeQueriedWithoutThrowing()
    {
        bool nvidia = GpuPolarisAccelerator.IsNvidiaAvailable;
        bool amd = GpuPolarisAccelerator.IsAmdAvailable;
        bool any = GpuPolarisAccelerator.IsGpuAvailable;

        Assert.False(amd);
        Assert.Equal(nvidia, any);
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void VectorAdd_CpuAndGpu_MatchExpected()
    {
        int n = 100_000;
        float[] a = new float[n];
        float[] b = new float[n];
        float[] cpuRes = new float[n];
        float[] autoRes = new float[n];

        for (int i = 0; i < n; i++)
        {
            a[i] = i * 0.5f;
            b[i] = 10.0f;
        }

        GpuPolarisAccelerator.VectorAdd(a, b, cpuRes, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorAdd(a, b, autoRes, GpuTarget.Auto);

        for (int i = 0; i < n; i++)
        {
            Assert.Equal(a[i] + b[i], cpuRes[i], 1e-5f);
            Assert.Equal(cpuRes[i], autoRes[i], 1e-4f);
        }
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void VectorSub_CpuAndGpu_MatchExpected()
    {
        int n = 100_000;
        float[] a = new float[n];
        float[] b = new float[n];
        float[] cpuRes = new float[n];
        float[] autoRes = new float[n];

        for (int i = 0; i < n; i++)
        {
            a[i] = i * 1.5f;
            b[i] = i * 0.5f;
        }

        GpuPolarisAccelerator.VectorSub(a, b, cpuRes, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorSub(a, b, autoRes, GpuTarget.Auto);

        for (int i = 0; i < n; i++)
        {
            Assert.Equal(a[i] - b[i], cpuRes[i], 1e-5f);
            Assert.Equal(cpuRes[i], autoRes[i], 1e-4f);
        }
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void VectorMul_CpuAndGpu_MatchExpected()
    {
        int n = 100_000;
        float[] a = new float[n];
        float[] b = new float[n];
        float[] cpuRes = new float[n];
        float[] autoRes = new float[n];

        for (int i = 0; i < n; i++)
        {
            a[i] = (i % 100) * 0.1f;
            b[i] = 2.0f;
        }

        GpuPolarisAccelerator.VectorMul(a, b, cpuRes, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorMul(a, b, autoRes, GpuTarget.Auto);

        for (int i = 0; i < n; i++)
        {
            Assert.Equal(a[i] * b[i], cpuRes[i], 1e-5f);
            Assert.Equal(cpuRes[i], autoRes[i], 1e-4f);
        }
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void VectorDiv_CpuAndGpu_MatchExpected()
    {
        int n = 100_000;
        float[] a = new float[n];
        float[] b = new float[n];
        float[] cpuRes = new float[n];
        float[] autoRes = new float[n];

        for (int i = 0; i < n; i++)
        {
            a[i] = (i + 1) * 2.0f;
            b[i] = 2.0f;
        }

        GpuPolarisAccelerator.VectorDiv(a, b, cpuRes, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorDiv(a, b, autoRes, GpuTarget.Auto);

        for (int i = 0; i < n; i++)
        {
            Assert.Equal(a[i] / b[i], cpuRes[i], 1e-5f);
            Assert.Equal(cpuRes[i], autoRes[i], 1e-4f);
        }
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void VectorFma_CpuAndGpu_MatchExpected()
    {
        int n = 100_000;
        float[] a = new float[n];
        float[] b = new float[n];
        float[] c = new float[n];
        float[] cpuRes = new float[n];
        float[] autoRes = new float[n];

        for (int i = 0; i < n; i++)
        {
            a[i] = (i % 50) * 0.2f;
            b[i] = 3.0f;
            c[i] = 1.5f;
        }

        GpuPolarisAccelerator.VectorFma(a, b, c, cpuRes, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorFma(a, b, c, autoRes, GpuTarget.Auto);

        for (int i = 0; i < n; i++)
        {
            float expected = MathF.FusedMultiplyAdd(a[i], b[i], c[i]);
            Assert.Equal(expected, cpuRes[i], 1e-4f);
            Assert.Equal(cpuRes[i], autoRes[i], 1e-4f);
        }
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void VectorExp_Log_Sqrt_Sigmoid_MatchExpected()
    {
        int n = 100_000;
        float[] a = new float[n];
        float[] expRes = new float[n];
        float[] logRes = new float[n];
        float[] sqrtRes = new float[n];
        float[] sigRes = new float[n];

        for (int i = 0; i < n; i++)
        {
            a[i] = 1.0f + (i % 20) * 0.1f;
        }

        GpuPolarisAccelerator.VectorExp(a, expRes, GpuTarget.Auto);
        GpuPolarisAccelerator.VectorLog(a, logRes, GpuTarget.Auto);
        GpuPolarisAccelerator.VectorSqrt(a, sqrtRes, GpuTarget.Auto);
        GpuPolarisAccelerator.VectorSigmoid(a, sigRes, GpuTarget.Auto);

        for (int i = 0; i < n; i++)
        {
            Assert.Equal(MathF.Exp(a[i]), expRes[i], 1e-3f);
            Assert.Equal(MathF.Log(a[i]), logRes[i], 1e-3f);
            Assert.Equal(MathF.Sqrt(a[i]), sqrtRes[i], 1e-3f);
            Assert.Equal(1.0f / (1.0f + MathF.Exp(-a[i])), sigRes[i], 1e-3f);
        }
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void VectorSum_CpuAndGpu_MatchExpected()
    {
        int n = 150_000;
        float[] a = new float[n];
        for (int i = 0; i < n; i++)
        {
            a[i] = 1.0f;
        }

        float cpuSum = GpuPolarisAccelerator.VectorSum(a, GpuTarget.Cpu);
        float autoSum = GpuPolarisAccelerator.VectorSum(a, GpuTarget.Auto);

        Assert.Equal(150_000.0f, cpuSum, 1.0f);
        Assert.Equal(cpuSum, autoSum, 2.0f);
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void Float32Series_GpuMethods_WorkCorrectly()
    {
        int n = 100_000;
        float[] data1 = new float[n];
        float[] data2 = new float[n];
        for (int i = 0; i < n; i++)
        {
            data1[i] = 2.0f;
            data2[i] = 3.0f;
        }

        var s1 = new Float32Series("s1", data1);
        var s2 = new Float32Series("s2", data2);

        var sAdd = s1.Add(s2);
        Assert.Equal(n, sAdd.Length);
        Assert.Equal(5.0f, sAdd[0], 1e-4f);

        var sMul = s1.Multiply(s2);
        Assert.Equal(6.0f, sMul[0], 1e-4f);

        var sSub = s2.Subtract(s1);
        Assert.Equal(1.0f, sSub[0], 1e-4f);

        var sDiv = s2.Divide(s1);
        Assert.Equal(1.5f, sDiv[0], 1e-4f);

        var sExp = s1.Exp();
        Assert.Equal(MathF.Exp(2.0f), sExp[0], 1e-3f);

        float sum = s1.Sum();
        Assert.Equal(200_000.0f, sum, 1.0f);
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void GpuTarget_Nvidia_MatchesCpuParity_AcrossAllTenOpsAndFloat32Series()
    {
        Assert.True(GpuPolarisAccelerator.IsNvidiaAvailable, "Expected NVIDIA CUDA GPU to be available for Hardware=Gpu test.");

        const int n = 131_072;
        float[] a = new float[n];
        float[] b = new float[n];
        float[] c = new float[n];

        for (int i = 0; i < n; i++)
        {
            a[i] = 1.0f + (i % 64) * 0.05f;
            b[i] = 2.0f + (i % 32) * 0.1f;
            c[i] = -0.5f + (i % 16) * 0.05f;
        }

        float[] cpuOut = new float[n];
        float[] gpuOut = new float[n];

        GpuPolarisAccelerator.VectorAdd(a, b, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorAdd(a, b, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-4f);

        GpuPolarisAccelerator.VectorSub(a, b, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorSub(a, b, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-4f);

        GpuPolarisAccelerator.VectorMul(a, b, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorMul(a, b, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-4f);

        GpuPolarisAccelerator.VectorDiv(a, b, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorDiv(a, b, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-4f);

        GpuPolarisAccelerator.VectorFma(a, b, c, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorFma(a, b, c, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-4f);

        GpuPolarisAccelerator.VectorExp(a, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorExp(a, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-3f);

        GpuPolarisAccelerator.VectorLog(a, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorLog(a, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-3f);

        GpuPolarisAccelerator.VectorSqrt(a, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorSqrt(a, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-4f);

        GpuPolarisAccelerator.VectorSigmoid(a, cpuOut, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorSigmoid(a, gpuOut, GpuTarget.Nvidia);
        AssertSpanClose(cpuOut, gpuOut, 1e-4f);

        float cpuSum = GpuPolarisAccelerator.VectorSum(a, GpuTarget.Cpu);
        float gpuSum = GpuPolarisAccelerator.VectorSum(a, GpuTarget.Nvidia);
        Assert.Equal(cpuSum, gpuSum, MathF.Max(1.0f, MathF.Abs(cpuSum) * 1e-4f));

        using var sa = new Float32Series("a", a);
        using var sb = new Float32Series("b", b);
        using var sc = new Float32Series("c", c);

        using var sAdd = sa.Add(sb, GpuTarget.Nvidia);
        using var sSub = sa.Subtract(sb, GpuTarget.Nvidia);
        using var sMul = sa.Multiply(sb, GpuTarget.Nvidia);
        using var sDiv = sa.Divide(sb, GpuTarget.Nvidia);
        using var sFma = sa.Fma(sb, sc, GpuTarget.Nvidia);
        using var sExp = sa.Exp(GpuTarget.Nvidia);
        using var sLog = sa.Log(GpuTarget.Nvidia);
        using var sSqrt = sa.Sqrt(GpuTarget.Nvidia);
        using var sSig = sa.Sigmoid(GpuTarget.Nvidia);

        Assert.Equal(a[0] + b[0], sAdd[0], 1e-4f);
        Assert.Equal(a[0] - b[0], sSub[0], 1e-4f);
        Assert.Equal(a[0] * b[0], sMul[0], 1e-4f);
        Assert.Equal(a[0] / b[0], sDiv[0], 1e-4f);
        Assert.Equal(MathF.FusedMultiplyAdd(a[0], b[0], c[0]), sFma[0], 1e-4f);
        Assert.Equal(MathF.Exp(a[0]), sExp[0], 1e-3f);
        Assert.Equal(MathF.Log(a[0]), sLog[0], 1e-3f);
        Assert.Equal(MathF.Sqrt(a[0]), sSqrt[0], 1e-4f);
        Assert.Equal(1.0f / (1.0f + MathF.Exp(-a[0])), sSig[0], 1e-4f);
        Assert.Equal(sa.Sum(GpuTarget.Cpu), sa.Sum(GpuTarget.Nvidia), MathF.Max(1.0f, MathF.Abs(cpuSum) * 1e-4f));
    }

    [Fact]
    [Trait("Hardware", "Gpu")]
    public void StreamPool_BoundedEviction_And_ReleaseReinit_UnderConcurrency()
    {
        Assert.True(GpuPolarisAccelerator.IsNvidiaAvailable, "Expected NVIDIA CUDA GPU to be available for Hardware=Gpu test.");

        GpuPolarisAccelerator.ReleaseGpuResources();
        Assert.Equal(0, GpuPolarisAccelerator.PooledContextCount);

        const int n = 65_536;
        float[] a = new float[n];
        float[] b = new float[n];
        for (int i = 0; i < n; i++)
        {
            a[i] = 1.0f + (i % 17) * 0.1f;
            b[i] = 2.5f;
        }

        Parallel.For(0, 16, iter =>
        {
            float[] dst = new float[n];
            if ((iter & 1) == 0)
            {
                GpuPolarisAccelerator.VectorAdd(a, b, dst, GpuTarget.Nvidia);
                Assert.Equal(a[42] + b[42], dst[42], 1e-4f);
            }
            else
            {
                GpuPolarisAccelerator.VectorExp(a, dst, GpuTarget.Nvidia);
                Assert.Equal(MathF.Exp(a[42]), dst[42], 1e-3f);
            }
        });

        int pooled = GpuPolarisAccelerator.PooledContextCount;
        Assert.InRange(pooled, 1, 8);

        GpuPolarisAccelerator.ReleaseGpuResources();
        Assert.Equal(0, GpuPolarisAccelerator.PooledContextCount);

        float[] reinitDst = new float[n];
        GpuPolarisAccelerator.VectorAdd(a, b, reinitDst, GpuTarget.Nvidia);
        Assert.Equal(a[0] + b[0], reinitDst[0], 1e-4f);
        GpuPolarisAccelerator.VectorExp(a, reinitDst, GpuTarget.Nvidia);
        Assert.Equal(MathF.Exp(a[0]), reinitDst[0], 1e-3f);
        float reinitSum = GpuPolarisAccelerator.VectorSum(a, GpuTarget.Nvidia);
        Assert.Equal(GpuPolarisAccelerator.VectorSum(a, GpuTarget.Cpu), reinitSum, 2.0f);
    }

    private static void AssertSpanClose(ReadOnlySpan<float> expected, ReadOnlySpan<float> actual, float tolerance)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i], actual[i], tolerance);
        }
    }
}

public class GpuPolarisCpuUnitTests
{
    [Fact]
    public void GpuTarget_DualGpu_ThrowsNotSupportedException_AcrossAllApis()
    {
        float[] a = [1.0f, 2.0f, 3.0f, 4.0f];
        float[] b = [2.0f, 3.0f, 4.0f, 5.0f];
        float[] c = [0.5f, 0.5f, 0.5f, 0.5f];
        float[] dst = new float[4];

        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorAdd(a, b, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorSub(a, b, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorMul(a, b, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorDiv(a, b, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorFma(a, b, c, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorExp(a, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorLog(a, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorSqrt(a, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorSigmoid(a, dst, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => GpuPolarisAccelerator.VectorSum(a, GpuTarget.DualGpu));

        Assert.Throws<PlatformNotSupportedException>(() => GpuPolarisAccelerator.VectorAdd(a, b, dst, GpuTarget.Amd));
        Assert.Throws<PlatformNotSupportedException>(() => GpuPolarisAccelerator.VectorSum(a, GpuTarget.Amd));
        Assert.Throws<ArgumentOutOfRangeException>(() => GpuPolarisAccelerator.VectorAdd(a, b, dst, (GpuTarget)999));
        Assert.Throws<ArgumentOutOfRangeException>(() => GpuPolarisAccelerator.VectorSum(a, (GpuTarget)999));

        using var sa = new Float32Series("a", a);
        using var sb = new Float32Series("b", b);
        using var sc = new Float32Series("c", c);

        Assert.Throws<NotSupportedException>(() => sa.Add(sb, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Subtract(sb, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Multiply(sb, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Divide(sb, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Fma(sb, sc, GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Exp(GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Log(GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Sqrt(GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Sigmoid(GpuTarget.DualGpu));
        Assert.Throws<NotSupportedException>(() => sa.Sum(GpuTarget.DualGpu));
    }

    [Fact]
    public void ExecuteUnaryOp_TensorPrimitives_EmptyZeroOverlapAndParity_AcrossLengths()
    {
        int[] lengths = [0, 1, 7, 16, 63, 1024, 5000];
        foreach (int n in lengths)
        {
            float[] src = new float[n];
            for (int i = 0; i < n; i++)
            {
                src[i] = 0.5f + (i % 19) * 0.25f;
            }

            float[] dstExp = new float[n + 3];
            float[] dstLog = new float[n + 3];
            float[] dstSqrt = new float[n + 3];
            float[] dstSig = new float[n + 3];
            Array.Fill(dstExp, -999f);
            Array.Fill(dstLog, -999f);
            Array.Fill(dstSqrt, -999f);
            Array.Fill(dstSig, -999f);

            GpuPolarisAccelerator.VectorExp(src, dstExp, GpuTarget.Cpu);
            GpuPolarisAccelerator.VectorLog(src, dstLog, GpuTarget.Cpu);
            GpuPolarisAccelerator.VectorSqrt(src, dstSqrt, GpuTarget.Cpu);
            GpuPolarisAccelerator.VectorSigmoid(src, dstSig, GpuTarget.Cpu);

            for (int i = 0; i < n; i++)
            {
                Assert.Equal(MathF.Exp(src[i]), dstExp[i], 1e-4f);
                Assert.Equal(MathF.Log(src[i]), dstLog[i], 1e-4f);
                Assert.Equal(MathF.Sqrt(src[i]), dstSqrt[i], 1e-4f);
                Assert.Equal(1.0f / (1.0f + MathF.Exp(-src[i])), dstSig[i], 1e-4f);
            }
            for (int i = n; i < dstExp.Length; i++)
            {
                Assert.Equal(-999f, dstExp[i]);
                Assert.Equal(-999f, dstLog[i]);
                Assert.Equal(-999f, dstSqrt[i]);
                Assert.Equal(-999f, dstSig[i]);
            }

            if (n > 0)
            {
                // Exact same-span in-place
                float[] inPlace = (float[])src.Clone();
                GpuPolarisAccelerator.VectorSigmoid(inPlace, inPlace, GpuTarget.Cpu);
                for (int i = 0; i < n; i++)
                {
                    Assert.Equal(dstSig[i], inPlace[i], 1e-5f);
                }

                // Shifted positive overlap (dst starts after src)
                float[] overlapPos = new float[n + 5];
                Array.Copy(src, 0, overlapPos, 0, n);
                GpuPolarisAccelerator.VectorExp(overlapPos.AsSpan(0, n), overlapPos.AsSpan(2, n), GpuTarget.Cpu);
                for (int i = 0; i < n; i++)
                {
                    Assert.Equal(dstExp[i], overlapPos[i + 2], 1e-4f);
                }

                // Shifted negative overlap (dst starts before src)
                float[] overlapNeg = new float[n + 5];
                Array.Copy(src, 0, overlapNeg, 2, n);
                GpuPolarisAccelerator.VectorSqrt(overlapNeg.AsSpan(2, n), overlapNeg.AsSpan(0, n), GpuTarget.Cpu);
                for (int i = 0; i < n; i++)
                {
                    Assert.Equal(dstSqrt[i], overlapNeg[i], 1e-5f);
                }
            }
        }
    }

    [Fact]
    public void VectorOps_EmptyMismatchedAndLargeByteSizes_ValidateCleanly()
    {
        float[] empty = Array.Empty<float>();
        GpuPolarisAccelerator.VectorAdd(empty, empty, empty, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorSub(empty, empty, empty, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorMul(empty, empty, empty, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorDiv(empty, empty, empty, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorFma(empty, empty, empty, empty, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorExp(empty, empty, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorLog(empty, empty, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorSqrt(empty, empty, GpuTarget.Cpu);
        GpuPolarisAccelerator.VectorSigmoid(empty, empty, GpuTarget.Cpu);
        Assert.Equal(0f, GpuPolarisAccelerator.VectorSum(empty, GpuTarget.Cpu));

        float[] four = [1f, 2f, 3f, 4f];
        float[] two = [1f, 2f];
        Assert.Throws<ArgumentException>(() => GpuPolarisAccelerator.VectorAdd(four, two, four, GpuTarget.Cpu));
        Assert.Throws<ArgumentException>(() => GpuPolarisAccelerator.VectorAdd(four, four, two, GpuTarget.Cpu));
        Assert.Throws<ArgumentException>(() => GpuPolarisAccelerator.VectorFma(four, two, four, four, GpuTarget.Cpu));
        Assert.Throws<ArgumentException>(() => GpuPolarisAccelerator.VectorFma(four, four, two, four, GpuTarget.Cpu));
        Assert.Throws<ArgumentException>(() => GpuPolarisAccelerator.VectorFma(four, four, four, two, GpuTarget.Cpu));
        Assert.Throws<ArgumentException>(() => GpuPolarisAccelerator.VectorExp(four, two, GpuTarget.Cpu));

        // 600M float elements = 2.4 GB (> int.MaxValue / 4), must not wrap negative in 32-bit int
        Assert.Equal(2_400_000_000UL, (ulong)GpuPolarisAccelerator.ComputeFloatBufferBytes(600_000_000));
        Assert.Throws<ArgumentOutOfRangeException>(() => GpuPolarisAccelerator.ComputeFloatBufferBytes(-1));
    }
}
