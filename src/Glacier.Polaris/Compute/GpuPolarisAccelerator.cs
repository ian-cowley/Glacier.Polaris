using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Gpu.Drivers;

namespace Glacier.Polaris.Compute;

/// <summary>
/// Bare-metal GPU hardware accelerator for Glacier.Polaris columnar operations.
/// Dispatches elementwise arithmetic, math transforms, and reductions directly
/// to NVIDIA RTX 4060 dGPU, AMD Radeon 890M APU, or multi-threaded AVX-512 CPU.
/// </summary>
public static unsafe class GpuPolarisAccelerator
{
    private static readonly Lock s_initLock = new();
    private static bool s_nvidiaInitialized;
    private static bool s_nvidiaAvailable;
    private static IntPtr s_cuContext;
    private static IntPtr s_cuModule;

    private static IntPtr s_fnAdd;
    private static IntPtr s_fnSub;
    private static IntPtr s_fnMul;
    private static IntPtr s_fnDiv;
    private static IntPtr s_fnFma;
    private static IntPtr s_fnExp;
    private static IntPtr s_fnLog;
    private static IntPtr s_fnSqrt;
    private static IntPtr s_fnSigmoid;
    private static IntPtr s_fnSum;
    private static IntPtr s_fnFilterGt;

    private sealed class PolarisGpuStreamContext : IDisposable
    {
        public IntPtr Stream;
        public IntPtr DIn1;
        public IntPtr DIn2;
        public IntPtr DIn3;
        public IntPtr DOut;
        public nuint CapIn1;
        public nuint CapIn2;
        public nuint CapIn3;
        public nuint CapOut;

        public PolarisGpuStreamContext(IntPtr stream)
        {
            Stream = stream;
        }

        public void EnsureBuffers(nuint cap1, nuint cap2, nuint cap3, nuint capOut)
        {
            if (cap1 > CapIn1)
            {
                if (DIn1 != IntPtr.Zero) CuDriver.MemFree(DIn1);
                CuDriver.MemAlloc(out DIn1, cap1);
                CapIn1 = cap1;
            }
            if (cap2 > CapIn2)
            {
                if (DIn2 != IntPtr.Zero) CuDriver.MemFree(DIn2);
                CuDriver.MemAlloc(out DIn2, cap2);
                CapIn2 = cap2;
            }
            if (cap3 > CapIn3)
            {
                if (DIn3 != IntPtr.Zero) CuDriver.MemFree(DIn3);
                CuDriver.MemAlloc(out DIn3, cap3);
                CapIn3 = cap3;
            }
            if (capOut > CapOut)
            {
                if (DOut != IntPtr.Zero) CuDriver.MemFree(DOut);
                CuDriver.MemAlloc(out DOut, capOut);
                CapOut = capOut;
            }
        }

        public void Dispose()
        {
            if (DIn1 != IntPtr.Zero) { CuDriver.MemFree(DIn1); DIn1 = IntPtr.Zero; }
            if (DIn2 != IntPtr.Zero) { CuDriver.MemFree(DIn2); DIn2 = IntPtr.Zero; }
            if (DIn3 != IntPtr.Zero) { CuDriver.MemFree(DIn3); DIn3 = IntPtr.Zero; }
            if (DOut != IntPtr.Zero) { CuDriver.MemFree(DOut); DOut = IntPtr.Zero; }
            if (Stream != IntPtr.Zero) { CuDriver.StreamDestroy(Stream); Stream = IntPtr.Zero; }
        }
    }

    private static readonly System.Collections.Concurrent.ConcurrentQueue<PolarisGpuStreamContext> s_streamPool = new();

    private static PolarisGpuStreamContext RentContext()
    {
        if (s_streamPool.TryDequeue(out var ctx))
            return ctx;

        CuDriver.StreamCreate(out IntPtr stream, 0);
        return new PolarisGpuStreamContext(stream);
    }

    private static void ReturnContext(PolarisGpuStreamContext ctx)
    {
        s_streamPool.Enqueue(ctx);
    }

    private static bool s_amdInitialized;
    private static bool s_amdAvailable;

    public static bool IsNvidiaAvailable => EnsureNvidiaInitialized();
    public static bool IsAmdAvailable => EnsureAmdInitialized();
    public static bool IsGpuAvailable => IsNvidiaAvailable || IsAmdAvailable;

    #region Driver Initialization

    private static bool EnsureNvidiaInitialized()
    {
        if (s_nvidiaInitialized) return s_nvidiaAvailable;
        lock (s_initLock)
        {
            if (s_nvidiaInitialized) return s_nvidiaAvailable;
            try
            {
                if (!CuDriver.IsAvailable())
                {
                    s_nvidiaAvailable = false;
                    s_nvidiaInitialized = true;
                    return false;
                }

                if (CuDriver.Init(0) != 0 || CuDriver.DeviceGet(out int dev, 0) != 0)
                {
                    s_nvidiaAvailable = false;
                    s_nvidiaInitialized = true;
                    return false;
                }

                CuDriver.DeviceGetAttribute(out int major, 75, dev);
                CuDriver.DeviceGetAttribute(out int minor, 76, dev);
                string targetArch = $"sm_{major}{minor}";

                if (CuDriver.CtxCreate(out s_cuContext, 0, dev) != 0)
                {
                    s_nvidiaAvailable = false;
                    s_nvidiaInitialized = true;
                    return false;
                }

                string ptx = GpuPolarisKernels.PtxSource;
                if (!ptx.Contains($".target {targetArch}"))
                {
                    ptx = System.Text.RegularExpressions.Regex.Replace(ptx, @"\.target\s+sm_\d+", $".target {targetArch}");
                }

                byte[] ptxBytes = Encoding.UTF8.GetBytes(ptx + "\0");
                if (CuDriver.ModuleLoadData(out s_cuModule, ptxBytes) != 0)
                {
                    s_nvidiaAvailable = false;
                    s_nvidiaInitialized = true;
                    return false;
                }

                CuDriver.ModuleGetFunction(out s_fnAdd, s_cuModule, "polaris_col_add_fp32");
                CuDriver.ModuleGetFunction(out s_fnSub, s_cuModule, "polaris_col_sub_fp32");
                CuDriver.ModuleGetFunction(out s_fnMul, s_cuModule, "polaris_col_mul_fp32");
                CuDriver.ModuleGetFunction(out s_fnDiv, s_cuModule, "polaris_col_div_fp32");
                CuDriver.ModuleGetFunction(out s_fnFma, s_cuModule, "polaris_col_fma_fp32");
                CuDriver.ModuleGetFunction(out s_fnExp, s_cuModule, "polaris_col_exp_fp32");
                CuDriver.ModuleGetFunction(out s_fnLog, s_cuModule, "polaris_col_log_fp32");
                CuDriver.ModuleGetFunction(out s_fnSqrt, s_cuModule, "polaris_col_sqrt_fp32");
                CuDriver.ModuleGetFunction(out s_fnSigmoid, s_cuModule, "polaris_col_sigmoid_fp32");
                CuDriver.ModuleGetFunction(out s_fnSum, s_cuModule, "polaris_col_sum_fp32");
                CuDriver.ModuleGetFunction(out s_fnFilterGt, s_cuModule, "polaris_col_filter_gt_fp32");

                s_nvidiaAvailable = s_fnAdd != IntPtr.Zero;
            }
            catch
            {
                s_nvidiaAvailable = false;
            }
            finally
            {
                s_nvidiaInitialized = true;
            }

            return s_nvidiaAvailable;
        }
    }

    private static bool EnsureAmdInitialized()
    {
        if (s_amdInitialized) return s_amdAvailable;
        lock (s_initLock)
        {
            if (s_amdInitialized) return s_amdAvailable;
            try
            {
                if (!HipDriver.IsAvailable() || HipDriver.Init(0) != 0 || HipDriver.GetDeviceCount(out int count) != 0 || count == 0)
                {
                    s_amdAvailable = false;
                    s_amdInitialized = true;
                    return false;
                }

                HipDriver.SetDevice(0);
                s_amdAvailable = true;
            }
            catch
            {
                s_amdAvailable = false;
            }
            finally
            {
                s_amdInitialized = true;
            }

            return s_amdAvailable;
        }
    }

    #endregion

    #region Binary Column Operations

    private enum BinaryOpType { Add, Sub, Mul, Div }

    public static void VectorAdd(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteBinaryOp(a, b, destination, s_fnAdd, BinaryOpType.Add, target);

    public static void VectorSub(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteBinaryOp(a, b, destination, s_fnSub, BinaryOpType.Sub, target);

    public static void VectorMul(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteBinaryOp(a, b, destination, s_fnMul, BinaryOpType.Mul, target);

    public static void VectorDiv(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteBinaryOp(a, b, destination, s_fnDiv, BinaryOpType.Div, target);

    public static void VectorFma(ReadOnlySpan<float> a, ReadOnlySpan<float> b, ReadOnlySpan<float> c, Span<float> destination, GpuTarget target = GpuTarget.Auto)
    {
        int n = a.Length;
        if (b.Length < n || c.Length < n || destination.Length < n)
            throw new ArgumentException("Buffer lengths mismatch");

        bool useGpu = target switch
        {
            GpuTarget.Cpu => false,
            GpuTarget.Nvidia => IsNvidiaAvailable,
            GpuTarget.Amd => IsAmdAvailable,
            _ => (IsNvidiaAvailable || IsAmdAvailable) && n >= 65536
        };

        if (useGpu && IsNvidiaAvailable && s_fnFma != IntPtr.Zero)
        {
            if (ExecuteTernaryKernel(a, b, c, destination, s_fnFma, n))
                return;
        }

        // SIMD AVX-512 / AVX2 CPU Fallback
        fixed (float* pA = a, pB = b, pC = c, pDest = destination)
        {
            float* pa = pA; float* pb = pB; float* pc = pC; float* pd = pDest;
            Parallel.For(0, (n + 1023) / 1024, chunk =>
            {
                int start = chunk * 1024;
                int end = Math.Min(start + 1024, n);
                int i = start;

                if (Vector512.IsHardwareAccelerated && end - i >= Vector512<float>.Count)
                {
                    int step = Vector512<float>.Count;
                    int limit = end - step;
                    while (i <= limit)
                    {
                        var va = Vector512.Load(pa + i);
                        var vb = Vector512.Load(pb + i);
                        var vc = Vector512.Load(pc + i);
                        var vres = Vector512.FusedMultiplyAdd(va, vb, vc);
                        vres.Store(pd + i);
                        i += step;
                    }
                }
                else if (Vector256.IsHardwareAccelerated && end - i >= Vector256<float>.Count)
                {
                    int step = Vector256<float>.Count;
                    int limit = end - step;
                    while (i <= limit)
                    {
                        var va = Vector256.Load(pa + i);
                        var vb = Vector256.Load(pb + i);
                        var vc = Vector256.Load(pc + i);
                        var vres = Vector256.FusedMultiplyAdd(va, vb, vc);
                        vres.Store(pd + i);
                        i += step;
                    }
                }

                for (; i < end; i++)
                {
                    pd[i] = MathF.FusedMultiplyAdd(pa[i], pb[i], pc[i]);
                }
            });
        }
    }

    private static void ExecuteBinaryOp(
        ReadOnlySpan<float> a,
        ReadOnlySpan<float> b,
        Span<float> destination,
        IntPtr kernelFn,
        BinaryOpType op,
        GpuTarget target)
    {
        int n = a.Length;
        if (b.Length < n || destination.Length < n)
            throw new ArgumentException("Buffer lengths mismatch");

        bool useGpu = target switch
        {
            GpuTarget.Cpu => false,
            GpuTarget.Nvidia => IsNvidiaAvailable,
            GpuTarget.Amd => IsAmdAvailable,
            _ => (IsNvidiaAvailable || IsAmdAvailable) && n >= 65536
        };

        if (useGpu && IsNvidiaAvailable && kernelFn != IntPtr.Zero)
        {
            if (ExecuteBinaryKernel(a, b, destination, kernelFn, n))
                return;
        }

        // SIMD AVX-512 / AVX2 CPU execution
        fixed (float* pA = a, pB = b, pDest = destination)
        {
            float* pa = pA; float* pb = pB; float* pd = pDest;
            Parallel.For(0, (n + 1023) / 1024, chunk =>
            {
                int start = chunk * 1024;
                int end = Math.Min(start + 1024, n);
                int i = start;

                if (Vector512.IsHardwareAccelerated && end - i >= Vector512<float>.Count)
                {
                    int step = Vector512<float>.Count;
                    int limit = end - step;
                    switch (op)
                    {
                        case BinaryOpType.Add:
                            while (i <= limit)
                            {
                                (Vector512.Load(pa + i) + Vector512.Load(pb + i)).Store(pd + i);
                                i += step;
                            }
                            break;
                        case BinaryOpType.Sub:
                            while (i <= limit)
                            {
                                (Vector512.Load(pa + i) - Vector512.Load(pb + i)).Store(pd + i);
                                i += step;
                            }
                            break;
                        case BinaryOpType.Mul:
                            while (i <= limit)
                            {
                                (Vector512.Load(pa + i) * Vector512.Load(pb + i)).Store(pd + i);
                                i += step;
                            }
                            break;
                        case BinaryOpType.Div:
                            while (i <= limit)
                            {
                                (Vector512.Load(pa + i) / Vector512.Load(pb + i)).Store(pd + i);
                                i += step;
                            }
                            break;
                    }
                }
                else if (Vector256.IsHardwareAccelerated && end - i >= Vector256<float>.Count)
                {
                    int step = Vector256<float>.Count;
                    int limit = end - step;
                    switch (op)
                    {
                        case BinaryOpType.Add:
                            while (i <= limit)
                            {
                                (Vector256.Load(pa + i) + Vector256.Load(pb + i)).Store(pd + i);
                                i += step;
                            }
                            break;
                        case BinaryOpType.Sub:
                            while (i <= limit)
                            {
                                (Vector256.Load(pa + i) - Vector256.Load(pb + i)).Store(pd + i);
                                i += step;
                            }
                            break;
                        case BinaryOpType.Mul:
                            while (i <= limit)
                            {
                                (Vector256.Load(pa + i) * Vector256.Load(pb + i)).Store(pd + i);
                                i += step;
                            }
                            break;
                        case BinaryOpType.Div:
                            while (i <= limit)
                            {
                                (Vector256.Load(pa + i) / Vector256.Load(pb + i)).Store(pd + i);
                                i += step;
                            }
                            break;
                    }
                }

                switch (op)
                {
                    case BinaryOpType.Add:
                        for (; i < end; i++) pd[i] = pa[i] + pb[i];
                        break;
                    case BinaryOpType.Sub:
                        for (; i < end; i++) pd[i] = pa[i] - pb[i];
                        break;
                    case BinaryOpType.Mul:
                        for (; i < end; i++) pd[i] = pa[i] * pb[i];
                        break;
                    case BinaryOpType.Div:
                        for (; i < end; i++) pd[i] = pa[i] / pb[i];
                        break;
                }
            });
        }
    }

    #endregion

    #region Math Transformations

    public static void VectorExp(ReadOnlySpan<float> input, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteUnaryOp(input, destination, s_fnExp, MathF.Exp, target);

    public static void VectorLog(ReadOnlySpan<float> input, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteUnaryOp(input, destination, s_fnLog, MathF.Log, target);

    public static void VectorSqrt(ReadOnlySpan<float> input, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteUnaryOp(input, destination, s_fnSqrt, MathF.Sqrt, target);

    public static void VectorSigmoid(ReadOnlySpan<float> input, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteUnaryOp(input, destination, s_fnSigmoid, x => 1.0f / (1.0f + MathF.Exp(-x)), target);

    private static void ExecuteUnaryOp(
        ReadOnlySpan<float> input,
        Span<float> destination,
        IntPtr kernelFn,
        Func<float, float> cpuOp,
        GpuTarget target)
    {
        int n = input.Length;
        if (destination.Length < n)
            throw new ArgumentException("Destination buffer too short");

        bool useGpu = target switch
        {
            GpuTarget.Cpu => false,
            GpuTarget.Nvidia => IsNvidiaAvailable,
            GpuTarget.Amd => IsAmdAvailable,
            _ => (IsNvidiaAvailable || IsAmdAvailable) && n >= 65536
        };

        if (useGpu && IsNvidiaAvailable && kernelFn != IntPtr.Zero)
        {
            if (ExecuteUnaryKernel(input, destination, kernelFn, n))
                return;
        }

        fixed (float* pIn = input, pDest = destination)
        {
            float* pi = pIn; float* pd = pDest;
            Parallel.For(0, (n + 1023) / 1024, chunk =>
            {
                int start = chunk * 1024;
                int end = Math.Min(start + 1024, n);
                for (int i = start; i < end; i++)
                {
                    pd[i] = cpuOp(pi[i]);
                }
            });
        }
    }

    #endregion

    #region Reductions

    public static float VectorSum(ReadOnlySpan<float> input, GpuTarget target = GpuTarget.Auto)
    {
        int n = input.Length;
        if (n == 0) return 0f;

        bool useGpu = target switch
        {
            GpuTarget.Cpu => false,
            GpuTarget.Nvidia => IsNvidiaAvailable,
            GpuTarget.Amd => IsAmdAvailable,
            _ => (IsNvidiaAvailable || IsAmdAvailable) && n >= 131072
        };

        if (useGpu && IsNvidiaAvailable && s_fnSum != IntPtr.Zero)
        {
            if (TryExecuteGpuVectorSum(input, n, out float gpuSum))
                return gpuSum;
        }

        // SIMD AVX-512 CPU Sum
        fixed (float* pIn = input)
        {
            float* pi = pIn;
            float sum = 0f;
            int i = 0;

            if (Vector512.IsHardwareAccelerated && n >= Vector512<float>.Count)
            {
                var acc512 = Vector512<float>.Zero;
                int step = Vector512<float>.Count;
                int limit = n - step;
                while (i <= limit)
                {
                    acc512 += Vector512.Load(pi + i);
                    i += step;
                }
                sum += Vector512.Sum(acc512);
            }
            else if (Vector256.IsHardwareAccelerated && n >= Vector256<float>.Count)
            {
                var acc256 = Vector256<float>.Zero;
                int step = Vector256<float>.Count;
                int limit = n - step;
                while (i <= limit)
                {
                    acc256 += Vector256.Load(pi + i);
                    i += step;
                }
                sum += Vector256.Sum(acc256);
            }

            for (; i < n; i++) sum += pi[i];
            return sum;
        }
    }

    #endregion

    #region Kernel Invocation Helpers

    private static bool ExecuteBinaryKernel(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> dest, IntPtr fn, int n)
    {
        nuint bytes = (nuint)(n * sizeof(float));
        CuDriver.CtxSetCurrent(s_cuContext);

        var ctx = RentContext();
        try
        {
            ctx.EnsureBuffers(bytes, bytes, 0, bytes);

            fixed (float* pA = a, pB = b, pD = dest)
            {
                CuDriver.MemcpyHtoDAsync(ctx.DIn1, (IntPtr)pA, bytes, ctx.Stream);
                CuDriver.MemcpyHtoDAsync(ctx.DIn2, (IntPtr)pB, bytes, ctx.Stream);

                IntPtr[] kernelParams = new IntPtr[4];
                GCHandle h0 = GCHandle.Alloc(ctx.DIn1, GCHandleType.Pinned);
                GCHandle h1 = GCHandle.Alloc(ctx.DIn2, GCHandleType.Pinned);
                GCHandle h2 = GCHandle.Alloc(ctx.DOut, GCHandleType.Pinned);
                GCHandle h3 = GCHandle.Alloc(n, GCHandleType.Pinned);

                kernelParams[0] = h0.AddrOfPinnedObject();
                kernelParams[1] = h1.AddrOfPinnedObject();
                kernelParams[2] = h2.AddrOfPinnedObject();
                kernelParams[3] = h3.AddrOfPinnedObject();

                GCHandle hArray = GCHandle.Alloc(kernelParams, GCHandleType.Pinned);
                try
                {
                    uint blockSize = 256;
                    uint itemsPerBlock = blockSize * 4;
                    uint gridSize = (uint)((n + itemsPerBlock - 1) / itemsPerBlock);

                    int launchRes = CuDriver.LaunchKernel(
                        fn,
                        gridSize, 1, 1,
                        blockSize, 1, 1,
                        0, ctx.Stream,
                        hArray.AddrOfPinnedObject(),
                        IntPtr.Zero);

                    if (launchRes != 0) return false;

                    CuDriver.MemcpyDtoHAsync((IntPtr)pD, ctx.DOut, bytes, ctx.Stream);
                    CuDriver.StreamSynchronize(ctx.Stream);
                    return true;
                }
                finally
                {
                    hArray.Free();
                    h0.Free();
                    h1.Free();
                    h2.Free();
                    h3.Free();
                }
            }
        }
        finally
        {
            ReturnContext(ctx);
        }
    }

    private static bool ExecuteTernaryKernel(ReadOnlySpan<float> a, ReadOnlySpan<float> b, ReadOnlySpan<float> c, Span<float> dest, IntPtr fn, int n)
    {
        nuint bytes = (nuint)(n * sizeof(float));
        CuDriver.CtxSetCurrent(s_cuContext);

        var ctx = RentContext();
        try
        {
            ctx.EnsureBuffers(bytes, bytes, bytes, bytes);

            fixed (float* pA = a, pB = b, pC = c, pD = dest)
            {
                CuDriver.MemcpyHtoDAsync(ctx.DIn1, (IntPtr)pA, bytes, ctx.Stream);
                CuDriver.MemcpyHtoDAsync(ctx.DIn2, (IntPtr)pB, bytes, ctx.Stream);
                CuDriver.MemcpyHtoDAsync(ctx.DIn3, (IntPtr)pC, bytes, ctx.Stream);

                IntPtr[] kernelParams = new IntPtr[5];
                GCHandle h0 = GCHandle.Alloc(ctx.DIn1, GCHandleType.Pinned);
                GCHandle h1 = GCHandle.Alloc(ctx.DIn2, GCHandleType.Pinned);
                GCHandle h2 = GCHandle.Alloc(ctx.DIn3, GCHandleType.Pinned);
                GCHandle h3 = GCHandle.Alloc(ctx.DOut, GCHandleType.Pinned);
                GCHandle h4 = GCHandle.Alloc(n, GCHandleType.Pinned);

                kernelParams[0] = h0.AddrOfPinnedObject();
                kernelParams[1] = h1.AddrOfPinnedObject();
                kernelParams[2] = h2.AddrOfPinnedObject();
                kernelParams[3] = h3.AddrOfPinnedObject();
                kernelParams[4] = h4.AddrOfPinnedObject();

                GCHandle hArray = GCHandle.Alloc(kernelParams, GCHandleType.Pinned);
                try
                {
                    uint blockSize = 256;
                    uint itemsPerBlock = blockSize * 4;
                    uint gridSize = (uint)((n + itemsPerBlock - 1) / itemsPerBlock);

                    int launchRes = CuDriver.LaunchKernel(
                        fn,
                        gridSize, 1, 1,
                        blockSize, 1, 1,
                        0, ctx.Stream,
                        hArray.AddrOfPinnedObject(),
                        IntPtr.Zero);

                    if (launchRes != 0) return false;

                    CuDriver.MemcpyDtoHAsync((IntPtr)pD, ctx.DOut, bytes, ctx.Stream);
                    CuDriver.StreamSynchronize(ctx.Stream);
                    return true;
                }
                finally
                {
                    hArray.Free();
                    h0.Free();
                    h1.Free();
                    h2.Free();
                    h3.Free();
                    h4.Free();
                }
            }
        }
        finally
        {
            ReturnContext(ctx);
        }
    }

    private static bool ExecuteUnaryKernel(ReadOnlySpan<float> input, Span<float> dest, IntPtr fn, int n)
    {
        nuint bytes = (nuint)(n * sizeof(float));
        CuDriver.CtxSetCurrent(s_cuContext);

        var ctx = RentContext();
        try
        {
            ctx.EnsureBuffers(bytes, 0, 0, bytes);

            fixed (float* pIn = input, pD = dest)
            {
                CuDriver.MemcpyHtoDAsync(ctx.DIn1, (IntPtr)pIn, bytes, ctx.Stream);

                IntPtr[] kernelParams = new IntPtr[3];
                GCHandle h0 = GCHandle.Alloc(ctx.DIn1, GCHandleType.Pinned);
                GCHandle h1 = GCHandle.Alloc(ctx.DOut, GCHandleType.Pinned);
                GCHandle h2 = GCHandle.Alloc(n, GCHandleType.Pinned);

                kernelParams[0] = h0.AddrOfPinnedObject();
                kernelParams[1] = h1.AddrOfPinnedObject();
                kernelParams[2] = h2.AddrOfPinnedObject();

                GCHandle hArray = GCHandle.Alloc(kernelParams, GCHandleType.Pinned);
                try
                {
                    uint blockSize = 256;
                    uint gridSize = (uint)((n + blockSize - 1) / blockSize);

                    int launchRes = CuDriver.LaunchKernel(
                        fn,
                        gridSize, 1, 1,
                        blockSize, 1, 1,
                        0, ctx.Stream,
                        hArray.AddrOfPinnedObject(),
                        IntPtr.Zero);

                    if (launchRes != 0) return false;

                    CuDriver.MemcpyDtoHAsync((IntPtr)pD, ctx.DOut, bytes, ctx.Stream);
                    CuDriver.StreamSynchronize(ctx.Stream);
                    return true;
                }
                finally
                {
                    hArray.Free();
                    h0.Free();
                    h1.Free();
                    h2.Free();
                }
            }
        }
        finally
        {
            ReturnContext(ctx);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool TryExecuteGpuVectorSum(ReadOnlySpan<float> input, int n, out float total)
    {
        total = 0f;
        try
        {
            uint blockSize = 256;
            uint gridSize = (uint)((n + (blockSize * 2) - 1) / (blockSize * 2));
            float[] blockSums = GC.AllocateArray<float>((int)gridSize, pinned: true);

            nuint bytesIn = (nuint)(n * sizeof(float));
            nuint bytesOut = (nuint)(gridSize * sizeof(float));

            CuDriver.CtxSetCurrent(s_cuContext);
            var ctx = RentContext();
            try
            {
                ctx.EnsureBuffers(bytesIn, 0, 0, bytesOut);

                fixed (float* pIn = input, pSums = blockSums)
                {
                    CuDriver.MemcpyHtoDAsync(ctx.DIn1, (IntPtr)pIn, bytesIn, ctx.Stream);

                    IntPtr[] kernelParams = new IntPtr[3];
                    GCHandle h0 = GCHandle.Alloc(ctx.DIn1, GCHandleType.Pinned);
                    GCHandle h1 = GCHandle.Alloc(ctx.DOut, GCHandleType.Pinned);
                    GCHandle h2 = GCHandle.Alloc(n, GCHandleType.Pinned);

                    kernelParams[0] = h0.AddrOfPinnedObject();
                    kernelParams[1] = h1.AddrOfPinnedObject();
                    kernelParams[2] = h2.AddrOfPinnedObject();

                    GCHandle hArray = GCHandle.Alloc(kernelParams, GCHandleType.Pinned);
                    try
                    {
                        uint sharedMemBytes = blockSize * sizeof(float);
                        int launchRes = CuDriver.LaunchKernel(
                            s_fnSum,
                            gridSize, 1, 1,
                            blockSize, 1, 1,
                            sharedMemBytes, ctx.Stream,
                            hArray.AddrOfPinnedObject(),
                            IntPtr.Zero);

                        if (launchRes == 0)
                        {
                            CuDriver.MemcpyDtoHAsync((IntPtr)pSums, ctx.DOut, bytesOut, ctx.Stream);
                            CuDriver.StreamSynchronize(ctx.Stream);

                            for (int i = 0; i < blockSums.Length; i++) total += blockSums[i];
                            return true;
                        }
                    }
                    finally
                    {
                        hArray.Free();
                        h0.Free();
                        h1.Free();
                        h2.Free();
                    }
                }
            }
            finally
            {
                ReturnContext(ctx);
            }
        }
        catch
        {
            return false;
        }

        return false;
    }

    #endregion
}
