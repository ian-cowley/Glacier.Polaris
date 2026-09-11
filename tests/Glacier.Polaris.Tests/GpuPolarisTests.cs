using System;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests;

public class GpuPolarisTests
{
    [Fact]
    public void GpuAvailability_CanBeQueriedWithoutThrowing()
    {
        bool nvidia = GpuPolarisAccelerator.IsNvidiaAvailable;
        bool amd = GpuPolarisAccelerator.IsAmdAvailable;
        bool any = GpuPolarisAccelerator.IsGpuAvailable;
        Assert.True(true);
    }

    [Fact]
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
}
