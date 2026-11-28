using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Gpu.Compilation;
using Glacier.Gpu.Drivers;

namespace Glacier.Polaris.Compute;

/// <summary>
/// Hardware accelerator for Glacier.Polaris single-precision columnar operations.
/// Dispatches elementwise arithmetic, math transforms, and reductions to NVIDIA CUDA GPUs
/// (when available) or multi-threaded AVX-512 / AVX2 SIMD / TensorPrimitives CPU kernels.
/// </summary>
public static unsafe class GpuPolarisAccelerator
{
    private const int MaxPooledContexts = 8;
    private const nuint MaxIdleContextVramBytes = (nuint)(256UL * 1024UL * 1024UL);
    private const nuint MaxTotalIdlePoolVramBytes = (nuint)(512UL * 1024UL * 1024UL);

    private static readonly Lock s_initLock = new();
    private static readonly object s_poolLock = new();
    private static readonly ReaderWriterLockSlim s_lifecycleLock = new(LockRecursionPolicy.SupportsRecursion);

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

    private static readonly Stack<PolarisGpuStreamContext> s_freeContexts = new();
    private static int s_inUseContexts;
    private static nuint s_pooledIdleVramBytes;

    static GpuPolarisAccelerator()
    {
        AppDomain.CurrentDomain.ProcessExit += static (_, _) =>
        {
            try { ReleaseGpuResources(); }
            catch (Exception ex)
            {
                // Best-effort cleanup during CLR ProcessExit teardown
                _ = ex;
            }
        };
    }

    private static void TryCudaCleanup(Action op, string opName)
    {
        try
        {
            op();
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException or InvalidOperationException or System.Runtime.InteropServices.ExternalException)
        {
            System.Diagnostics.Trace.TraceWarning($"[GpuPolarisAccelerator] {opName} warning: {ex.Message}");
        }
    }

    internal static int PooledContextCount
    {
        get
        {
            lock (s_poolLock) return s_freeContexts.Count;
        }
    }

    internal static nuint ComputeFloatBufferBytes(int elementCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(elementCount);
        return checked((nuint)elementCount * (nuint)sizeof(float));
    }

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

        public nuint TotalAllocatedBytes => CapIn1 + CapIn2 + CapIn3 + CapOut;

        public PolarisGpuStreamContext(IntPtr stream)
        {
            Stream = stream;
        }

        public bool EnsureBuffers(nuint cap1, nuint cap2, nuint cap3, nuint capOut)
        {
            return EnsureSingleBuffer(ref DIn1, ref CapIn1, cap1)
                && EnsureSingleBuffer(ref DIn2, ref CapIn2, cap2)
                && EnsureSingleBuffer(ref DIn3, ref CapIn3, cap3)
                && EnsureSingleBuffer(ref DOut, ref CapOut, capOut);
        }

        private static bool EnsureSingleBuffer(ref IntPtr devicePtr, ref nuint capacity, nuint requiredBytes)
        {
            if (requiredBytes <= capacity)
                return true;

            if (devicePtr != IntPtr.Zero)
            {
                IntPtr oldPtr = devicePtr;
                devicePtr = IntPtr.Zero;
                capacity = 0;
                TryCudaCleanup(() => CuDriver.MemFree(oldPtr), "MemFree");
            }

            if (CuDriver.MemAlloc(out IntPtr ptr, requiredBytes) != 0 || ptr == IntPtr.Zero)
            {
                DrainIdleContexts();
                if (CuDriver.MemAlloc(out ptr, requiredBytes) != 0 || ptr == IntPtr.Zero)
                    return false;
            }

            devicePtr = ptr;
            capacity = requiredBytes;
            return true;
        }

        public void Dispose()
        {
            if (Stream != IntPtr.Zero)
            {
                IntPtr stream = Stream;
                Stream = IntPtr.Zero;
                TryCudaCleanup(() => CuDriver.StreamSynchronize(stream), "StreamSynchronize");
                TryCudaCleanup(() => CuDriver.StreamDestroy(stream), "StreamDestroy");
            }
            if (DIn1 != IntPtr.Zero) { IntPtr p = DIn1; DIn1 = IntPtr.Zero; CapIn1 = 0; TryCudaCleanup(() => CuDriver.MemFree(p), "MemFree DIn1"); }
            if (DIn2 != IntPtr.Zero) { IntPtr p = DIn2; DIn2 = IntPtr.Zero; CapIn2 = 0; TryCudaCleanup(() => CuDriver.MemFree(p), "MemFree DIn2"); }
            if (DIn3 != IntPtr.Zero) { IntPtr p = DIn3; DIn3 = IntPtr.Zero; CapIn3 = 0; TryCudaCleanup(() => CuDriver.MemFree(p), "MemFree DIn3"); }
            if (DOut != IntPtr.Zero) { IntPtr p = DOut; DOut = IntPtr.Zero; CapOut = 0; TryCudaCleanup(() => CuDriver.MemFree(p), "MemFree DOut"); }
        }
    }

    private static PolarisGpuStreamContext? RentContext()
    {
        PolarisGpuStreamContext? ctx = null;
        lock (s_poolLock)
        {
            if (s_freeContexts.Count > 0)
            {
                ctx = s_freeContexts.Pop();
                s_pooledIdleVramBytes -= ctx.TotalAllocatedBytes;
            }
            s_inUseContexts++;
        }

        if (ctx != null)
            return ctx;

        if (CuDriver.StreamCreate(out IntPtr stream, CuDriver.CU_STREAM_NON_BLOCKING) != 0 || stream == IntPtr.Zero)
        {
            lock (s_poolLock)
            {
                s_inUseContexts--;
                Monitor.PulseAll(s_poolLock);
            }
            return null;
        }

        return new PolarisGpuStreamContext(stream);
    }

    private static void ReturnContext(PolarisGpuStreamContext ctx, bool poison = false)
    {
        bool shouldDispose = poison;
        nuint ctxBytes = ctx.TotalAllocatedBytes;

        lock (s_poolLock)
        {
            if (!shouldDispose &&
                (s_freeContexts.Count >= MaxPooledContexts ||
                 ctxBytes > MaxIdleContextVramBytes ||
                 s_pooledIdleVramBytes + ctxBytes > MaxTotalIdlePoolVramBytes))
            {
                shouldDispose = true;
            }

            if (!shouldDispose)
            {
                s_freeContexts.Push(ctx);
                s_pooledIdleVramBytes += ctxBytes;
            }

            s_inUseContexts--;
            Monitor.PulseAll(s_poolLock);
        }

        if (shouldDispose)
        {
            ctx.Dispose();
        }
    }

    private static void DrainIdleContexts()
    {
        List<PolarisGpuStreamContext>? toDispose = null;
        lock (s_poolLock)
        {
            if (s_freeContexts.Count > 0)
            {
                toDispose = new List<PolarisGpuStreamContext>(s_freeContexts.Count);
                while (s_freeContexts.Count > 0)
                    toDispose.Add(s_freeContexts.Pop());
                s_pooledIdleVramBytes = 0;
            }
        }

        if (toDispose != null)
        {
            foreach (var idle in toDispose)
                idle.Dispose();
        }
    }

    public static bool IsNvidiaAvailable => EnsureNvidiaInitialized();

    /// <summary>
    /// AMD ROCm/HIP device kernels are not implemented in Glacier.Polaris; always returns <see langword="false"/>.
    /// </summary>
    public static bool IsAmdAvailable => false;

    public static bool IsGpuAvailable => IsNvidiaAvailable;

    private static void ValidateTarget(GpuTarget target)
    {
        switch (target)
        {
            case GpuTarget.Auto:
            case GpuTarget.Nvidia:
            case GpuTarget.Cpu:
                return;
            case GpuTarget.Amd:
                throw new PlatformNotSupportedException(
                    "AMD ROCm/HIP backend is not supported in Glacier.Polaris; use GpuTarget.Auto, GpuTarget.Nvidia, or GpuTarget.Cpu.");
            case GpuTarget.DualGpu:
                throw new NotSupportedException(
                    "Multi-GPU (GpuTarget.DualGpu) is not supported in Glacier.Polaris; use GpuTarget.Auto, GpuTarget.Nvidia, or GpuTarget.Cpu.");
            default:
                throw new ArgumentOutOfRangeException(nameof(target), target, "Unsupported GpuTarget value.");
        }
    }

    #region Driver Initialization & Lifecycle

    private static bool EnsureNvidiaInitialized()
    {
        if (Volatile.Read(ref s_nvidiaInitialized)) return Volatile.Read(ref s_nvidiaAvailable);
        lock (s_initLock)
        {
            if (s_nvidiaInitialized) return s_nvidiaAvailable;
            try
            {
                if (!CuDriver.IsAvailable())
                {
                    Volatile.Write(ref s_nvidiaAvailable, false);
                    return false;
                }

                if (CuDriver.Init(0) != 0 || CuDriver.DeviceGet(out int dev, 0) != 0)
                {
                    Volatile.Write(ref s_nvidiaAvailable, false);
                    return false;
                }

                if (CuDriver.DeviceGetAttribute(out int major, 75, dev) != 0 ||
                    CuDriver.DeviceGetAttribute(out int minor, 76, dev) != 0)
                {
                    Volatile.Write(ref s_nvidiaAvailable, false);
                    return false;
                }

                string targetArch = $"sm_{major}{minor}";

                if (CuDriver.CtxCreate(out s_cuContext, 0, dev) != 0 || s_cuContext == IntPtr.Zero)
                {
                    s_cuContext = IntPtr.Zero;
                    Volatile.Write(ref s_nvidiaAvailable, false);
                    return false;
                }

                string ptx = KernelCompiler.NormalizePtxDirectives(GpuPolarisKernels.PtxSource, targetArch);
                byte[] ptxBytes = Encoding.UTF8.GetBytes(ptx + "\0");
                if (CuDriver.ModuleLoadData(out s_cuModule, ptxBytes) != 0 || s_cuModule == IntPtr.Zero)
                {
                    s_cuModule = IntPtr.Zero;
                    RollbackPartialInit();
                    Volatile.Write(ref s_nvidiaAvailable, false);
                    return false;
                }

                if (CuDriver.ModuleGetFunction(out s_fnAdd, s_cuModule, "polaris_col_add_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnSub, s_cuModule, "polaris_col_sub_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnMul, s_cuModule, "polaris_col_mul_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnDiv, s_cuModule, "polaris_col_div_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnFma, s_cuModule, "polaris_col_fma_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnExp, s_cuModule, "polaris_col_exp_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnLog, s_cuModule, "polaris_col_log_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnSqrt, s_cuModule, "polaris_col_sqrt_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnSigmoid, s_cuModule, "polaris_col_sigmoid_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out s_fnSum, s_cuModule, "polaris_col_sum_fp32") != 0 ||
                    CuDriver.ModuleGetFunction(out _, s_cuModule, "polaris_col_filter_gt_fp32") != 0 ||
                    s_fnAdd == IntPtr.Zero ||
                    s_fnSub == IntPtr.Zero ||
                    s_fnMul == IntPtr.Zero ||
                    s_fnDiv == IntPtr.Zero ||
                    s_fnFma == IntPtr.Zero ||
                    s_fnExp == IntPtr.Zero ||
                    s_fnLog == IntPtr.Zero ||
                    s_fnSqrt == IntPtr.Zero ||
                    s_fnSigmoid == IntPtr.Zero ||
                    s_fnSum == IntPtr.Zero)
                {
                    RollbackPartialInit();
                    Volatile.Write(ref s_nvidiaAvailable, false);
                    return false;
                }

                Volatile.Write(ref s_nvidiaAvailable, true);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                Trace.TraceWarning($"Glacier.Polaris CUDA initialization failed: {ex.GetType().Name}: {ex.Message}");
                RollbackPartialInit();
                Volatile.Write(ref s_nvidiaAvailable, false);
            }
            finally
            {
                Volatile.Write(ref s_nvidiaInitialized, true);
            }

            return Volatile.Read(ref s_nvidiaAvailable);
        }
    }

    private static void RollbackPartialInit()
    {
        s_fnAdd = IntPtr.Zero;
        s_fnSub = IntPtr.Zero;
        s_fnMul = IntPtr.Zero;
        s_fnDiv = IntPtr.Zero;
        s_fnFma = IntPtr.Zero;
        s_fnExp = IntPtr.Zero;
        s_fnLog = IntPtr.Zero;
        s_fnSqrt = IntPtr.Zero;
        s_fnSigmoid = IntPtr.Zero;
        s_fnSum = IntPtr.Zero;

        if (s_cuModule != IntPtr.Zero)
        {
            IntPtr mod = s_cuModule;
            s_cuModule = IntPtr.Zero;
            TryCudaCleanup(() => CuDriver.ModuleUnload(mod), "ModuleUnload");
        }
        if (s_cuContext != IntPtr.Zero)
        {
            IntPtr ctx = s_cuContext;
            s_cuContext = IntPtr.Zero;
            TryCudaCleanup(() => CuDriver.CtxDestroy(ctx), "CtxDestroy");
        }
    }

    /// <summary>
    /// Releases all pooled CUDA streams, device buffers, loaded PTX modules, and the active CUDA context.
    /// Safe to call multiple times; subsequent GPU calls will re-initialize CUDA cleanly.
    /// </summary>
    public static void ReleaseGpuResources()
    {
        s_lifecycleLock.EnterWriteLock();
        try
        {
            lock (s_initLock)
            {
                List<PolarisGpuStreamContext> toDispose;
                lock (s_poolLock)
                {
                    while (s_inUseContexts > 0)
                        Monitor.Wait(s_poolLock, 50);

                    toDispose = new List<PolarisGpuStreamContext>(s_freeContexts.Count);
                    while (s_freeContexts.Count > 0)
                        toDispose.Add(s_freeContexts.Pop());
                    s_pooledIdleVramBytes = 0;
                }

                if (s_cuContext != IntPtr.Zero)
                {
                    IntPtr ctx = s_cuContext;
                    TryCudaCleanup(() => CuDriver.CtxSetCurrent(ctx), "CtxSetCurrent");
                }

                foreach (var ctx in toDispose)
                {
                    ctx.Dispose();
                }

                RollbackPartialInit();
                Volatile.Write(ref s_nvidiaAvailable, false);
                Volatile.Write(ref s_nvidiaInitialized, false);
            }
        }
        finally
        {
            s_lifecycleLock.ExitWriteLock();
        }
    }

    private static bool TryEnterNvidiaExecutionReadLock()
    {
        for (int attempt = 0; attempt < 2; attempt++)
        {
            if (!EnsureNvidiaInitialized()) return false;
            s_lifecycleLock.EnterReadLock();
            if (Volatile.Read(ref s_nvidiaAvailable) && s_cuContext != IntPtr.Zero)
            {
                return true;
            }
            s_lifecycleLock.ExitReadLock();
        }
        return false;
    }

    #endregion

    #region Binary Column Operations

    private enum BinaryOpType { Add, Sub, Mul, Div }

    public static void VectorAdd(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteBinaryOp(a, b, destination, BinaryOpType.Add, target);

    public static void VectorSub(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteBinaryOp(a, b, destination, BinaryOpType.Sub, target);

    public static void VectorMul(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteBinaryOp(a, b, destination, BinaryOpType.Mul, target);

    public static void VectorDiv(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteBinaryOp(a, b, destination, BinaryOpType.Div, target);

    public static void VectorFma(ReadOnlySpan<float> a, ReadOnlySpan<float> b, ReadOnlySpan<float> c, Span<float> destination, GpuTarget target = GpuTarget.Auto)
    {
        ValidateTarget(target);
        int n = a.Length;
        if (b.Length < n || c.Length < n || destination.Length < n)
            throw new ArgumentException("Buffer lengths mismatch");
        if (n == 0) return;

        bool useGpu = target switch
        {
            GpuTarget.Cpu => false,
            GpuTarget.Nvidia => IsNvidiaAvailable,
            GpuTarget.Auto => IsNvidiaAvailable && n >= 65536,
            _ => false
        };

        if (useGpu && TryExecuteTernaryKernel(a, b, c, destination, n))
            return;

        // SIMD AVX-512 / AVX2 CPU Fallback
        int chunkCount = (int)(((long)n + 1023) / 1024);
        fixed (float* pA = a, pB = b, pC = c, pDest = destination)
        {
            float* pa = pA; float* pb = pB; float* pc = pC; float* pd = pDest;
            Parallel.For(0, chunkCount, chunk =>
            {
                long startLong = (long)chunk * 1024;
                int start = (int)startLong;
                int end = (int)Math.Min(startLong + 1024, n);
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
        BinaryOpType op,
        GpuTarget target)
    {
        ValidateTarget(target);
        int n = a.Length;
        if (b.Length < n || destination.Length < n)
            throw new ArgumentException("Buffer lengths mismatch");
        if (n == 0) return;

        bool useGpu = target switch
        {
            GpuTarget.Cpu => false,
            GpuTarget.Nvidia => IsNvidiaAvailable,
            GpuTarget.Auto => IsNvidiaAvailable && n >= 65536,
            _ => false
        };

        if (useGpu && TryExecuteBinaryKernel(a, b, destination, op, n))
            return;

        // SIMD AVX-512 / AVX2 CPU execution
        int chunkCount = (int)(((long)n + 1023) / 1024);
        fixed (float* pA = a, pB = b, pDest = destination)
        {
            float* pa = pA; float* pb = pB; float* pd = pDest;
            Parallel.For(0, chunkCount, chunk =>
            {
                long startLong = (long)chunk * 1024;
                int start = (int)startLong;
                int end = (int)Math.Min(startLong + 1024, n);
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

    private enum UnaryOpType { Exp, Log, Sqrt, Sigmoid }

    public static void VectorExp(ReadOnlySpan<float> input, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteUnaryOp(input, destination, UnaryOpType.Exp, target);

    public static void VectorLog(ReadOnlySpan<float> input, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteUnaryOp(input, destination, UnaryOpType.Log, target);

    public static void VectorSqrt(ReadOnlySpan<float> input, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteUnaryOp(input, destination, UnaryOpType.Sqrt, target);

    public static void VectorSigmoid(ReadOnlySpan<float> input, Span<float> destination, GpuTarget target = GpuTarget.Auto)
        => ExecuteUnaryOp(input, destination, UnaryOpType.Sigmoid, target);

    private static void ExecuteUnaryOp(
        ReadOnlySpan<float> input,
        Span<float> destination,
        UnaryOpType op,
        GpuTarget target)
    {
        ValidateTarget(target);
        int n = input.Length;
        if (destination.Length < n)
            throw new ArgumentException("Destination buffer too short");
        if (n == 0) return;

        bool useGpu = target switch
        {
            GpuTarget.Cpu => false,
            GpuTarget.Nvidia => IsNvidiaAvailable,
            GpuTarget.Auto => IsNvidiaAvailable && n >= 65536,
            _ => false
        };

        if (useGpu && TryExecuteUnaryKernel(input, destination, op, n))
            return;

        var src = input;
        var dst = destination[..n];

        if (src.Overlaps(dst, out int offset) && offset != 0)
        {
            if (offset > 0)
            {
                for (int i = n - 1; i >= 0; i--)
                    dst[i] = ApplyScalarUnary(op, src[i]);
            }
            else
            {
                for (int i = 0; i < n; i++)
                    dst[i] = ApplyScalarUnary(op, src[i]);
            }
            return;
        }

        if (n < 4096)
        {
            ApplyTensorUnary(op, src, dst);
            return;
        }

        int chunkCount = (int)(((long)n + 1023) / 1024);
        fixed (float* pIn = src, pDest = dst)
        {
            float* pi = pIn; float* pd = pDest;
            Parallel.For(0, chunkCount, chunk =>
            {
                long startLong = (long)chunk * 1024;
                int start = (int)startLong;
                int len = (int)Math.Min(1024L, n - startLong);
                ApplyTensorUnary(op, new ReadOnlySpan<float>(pi + start, len), new Span<float>(pd + start, len));
            });
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyTensorUnary(UnaryOpType op, ReadOnlySpan<float> src, Span<float> dst)
    {
        switch (op)
        {
            case UnaryOpType.Exp:
                TensorPrimitives.Exp(src, dst);
                break;
            case UnaryOpType.Log:
                TensorPrimitives.Log(src, dst);
                break;
            case UnaryOpType.Sqrt:
                TensorPrimitives.Sqrt<float>(src, dst);
                break;
            case UnaryOpType.Sigmoid:
                TensorPrimitives.Sigmoid(src, dst);
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float ApplyScalarUnary(UnaryOpType op, float x) => op switch
    {
        UnaryOpType.Exp => MathF.Exp(x),
        UnaryOpType.Log => MathF.Log(x),
        UnaryOpType.Sqrt => MathF.Sqrt(x),
        UnaryOpType.Sigmoid => 1.0f / (1.0f + MathF.Exp(-x)),
        _ => x
    };

    #endregion

    #region Reductions

    public static float VectorSum(ReadOnlySpan<float> input, GpuTarget target = GpuTarget.Auto)
    {
        ValidateTarget(target);
        int n = input.Length;
        if (n == 0) return 0f;

        bool useGpu = target switch
        {
            GpuTarget.Cpu => false,
            GpuTarget.Nvidia => IsNvidiaAvailable,
            GpuTarget.Auto => IsNvidiaAvailable && n >= 131072,
            _ => false
        };

        if (useGpu && TryExecuteGpuVectorSum(input, n, out float gpuSum))
            return gpuSum;

        // SIMD AVX-512 / AVX2 CPU Sum
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

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool TryExecuteBinaryKernel(ReadOnlySpan<float> a, ReadOnlySpan<float> b, Span<float> dest, BinaryOpType op, int n)
    {
        if (!TryEnterNvidiaExecutionReadLock()) return false;
        try
        {
            IntPtr fn = op switch
            {
                BinaryOpType.Add => s_fnAdd,
                BinaryOpType.Sub => s_fnSub,
                BinaryOpType.Mul => s_fnMul,
                BinaryOpType.Div => s_fnDiv,
                _ => IntPtr.Zero
            };
            if (fn == IntPtr.Zero) return false;

            nuint bytes = ComputeFloatBufferBytes(n);
            if (CuDriver.CtxSetCurrent(s_cuContext) != 0) return false;

            var ctx = RentContext();
            if (ctx == null) return false;

            bool poison = true;
            try
            {
                if (!ctx.EnsureBuffers(bytes, bytes, 0, bytes))
                    return false;

                fixed (float* pA = a, pB = b, pD = dest)
                {
                    if (CuDriver.MemcpyHtoDAsync(ctx.DIn1, (IntPtr)pA, bytes, ctx.Stream) != 0)
                        return false;
                    if (CuDriver.MemcpyHtoDAsync(ctx.DIn2, (IntPtr)pB, bytes, ctx.Stream) != 0)
                        return false;

                    IntPtr dIn1 = ctx.DIn1;
                    IntPtr dIn2 = ctx.DIn2;
                    IntPtr dOut = ctx.DOut;
                    int count = n;

                    void** kernelParams = stackalloc void*[4];
                    kernelParams[0] = &dIn1;
                    kernelParams[1] = &dIn2;
                    kernelParams[2] = &dOut;
                    kernelParams[3] = &count;

                    uint blockSize = 256;
                    uint itemsPerBlock = blockSize * 4;
                    uint gridSize = (uint)(((long)n + itemsPerBlock - 1) / itemsPerBlock);

                    int launchRes = CuDriver.LaunchKernel(
                        fn,
                        gridSize, 1, 1,
                        blockSize, 1, 1,
                        0, ctx.Stream,
                        (IntPtr)kernelParams,
                        IntPtr.Zero);

                    if (launchRes != 0) return false;

                    if (CuDriver.MemcpyDtoHAsync((IntPtr)pD, ctx.DOut, bytes, ctx.Stream) != 0)
                        return false;
                    if (CuDriver.StreamSynchronize(ctx.Stream) != 0)
                        return false;

                    poison = false;
                    return true;
                }
            }
            finally
            {
                ReturnContext(ctx, poison);
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Trace.TraceWarning($"Glacier.Polaris CUDA binary kernel failed, falling back to CPU: {ex.GetType().Name}: {ex.Message}");
            return false;
        }
        finally
        {
            s_lifecycleLock.ExitReadLock();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool TryExecuteTernaryKernel(ReadOnlySpan<float> a, ReadOnlySpan<float> b, ReadOnlySpan<float> c, Span<float> dest, int n)
    {
        if (!TryEnterNvidiaExecutionReadLock()) return false;
        try
        {
            IntPtr fn = s_fnFma;
            if (fn == IntPtr.Zero) return false;

            nuint bytes = ComputeFloatBufferBytes(n);
            if (CuDriver.CtxSetCurrent(s_cuContext) != 0) return false;

            var ctx = RentContext();
            if (ctx == null) return false;

            bool poison = true;
            try
            {
                if (!ctx.EnsureBuffers(bytes, bytes, bytes, bytes))
                    return false;

                fixed (float* pA = a, pB = b, pC = c, pD = dest)
                {
                    if (CuDriver.MemcpyHtoDAsync(ctx.DIn1, (IntPtr)pA, bytes, ctx.Stream) != 0)
                        return false;
                    if (CuDriver.MemcpyHtoDAsync(ctx.DIn2, (IntPtr)pB, bytes, ctx.Stream) != 0)
                        return false;
                    if (CuDriver.MemcpyHtoDAsync(ctx.DIn3, (IntPtr)pC, bytes, ctx.Stream) != 0)
                        return false;

                    IntPtr dIn1 = ctx.DIn1;
                    IntPtr dIn2 = ctx.DIn2;
                    IntPtr dIn3 = ctx.DIn3;
                    IntPtr dOut = ctx.DOut;
                    int count = n;

                    void** kernelParams = stackalloc void*[5];
                    kernelParams[0] = &dIn1;
                    kernelParams[1] = &dIn2;
                    kernelParams[2] = &dIn3;
                    kernelParams[3] = &dOut;
                    kernelParams[4] = &count;

                    uint blockSize = 256;
                    uint itemsPerBlock = blockSize * 4;
                    uint gridSize = (uint)(((long)n + itemsPerBlock - 1) / itemsPerBlock);

                    int launchRes = CuDriver.LaunchKernel(
                        fn,
                        gridSize, 1, 1,
                        blockSize, 1, 1,
                        0, ctx.Stream,
                        (IntPtr)kernelParams,
                        IntPtr.Zero);

                    if (launchRes != 0) return false;

                    if (CuDriver.MemcpyDtoHAsync((IntPtr)pD, ctx.DOut, bytes, ctx.Stream) != 0)
                        return false;
                    if (CuDriver.StreamSynchronize(ctx.Stream) != 0)
                        return false;

                    poison = false;
                    return true;
                }
            }
            finally
            {
                ReturnContext(ctx, poison);
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Trace.TraceWarning($"Glacier.Polaris CUDA ternary kernel failed, falling back to CPU: {ex.GetType().Name}: {ex.Message}");
            return false;
        }
        finally
        {
            s_lifecycleLock.ExitReadLock();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool TryExecuteUnaryKernel(ReadOnlySpan<float> input, Span<float> dest, UnaryOpType op, int n)
    {
        if (!TryEnterNvidiaExecutionReadLock()) return false;
        try
        {
            IntPtr fn = op switch
            {
                UnaryOpType.Exp => s_fnExp,
                UnaryOpType.Log => s_fnLog,
                UnaryOpType.Sqrt => s_fnSqrt,
                UnaryOpType.Sigmoid => s_fnSigmoid,
                _ => IntPtr.Zero
            };
            if (fn == IntPtr.Zero) return false;

            nuint bytes = ComputeFloatBufferBytes(n);
            if (CuDriver.CtxSetCurrent(s_cuContext) != 0) return false;

            var ctx = RentContext();
            if (ctx == null) return false;

            bool poison = true;
            try
            {
                if (!ctx.EnsureBuffers(bytes, 0, 0, bytes))
                    return false;

                fixed (float* pIn = input, pD = dest)
                {
                    if (CuDriver.MemcpyHtoDAsync(ctx.DIn1, (IntPtr)pIn, bytes, ctx.Stream) != 0)
                        return false;

                    IntPtr dIn1 = ctx.DIn1;
                    IntPtr dOut = ctx.DOut;
                    int count = n;

                    void** kernelParams = stackalloc void*[3];
                    kernelParams[0] = &dIn1;
                    kernelParams[1] = &dOut;
                    kernelParams[2] = &count;

                    uint blockSize = 256;
                    uint gridSize = (uint)(((long)n + blockSize - 1) / blockSize);

                    int launchRes = CuDriver.LaunchKernel(
                        fn,
                        gridSize, 1, 1,
                        blockSize, 1, 1,
                        0, ctx.Stream,
                        (IntPtr)kernelParams,
                        IntPtr.Zero);

                    if (launchRes != 0) return false;

                    if (CuDriver.MemcpyDtoHAsync((IntPtr)pD, ctx.DOut, bytes, ctx.Stream) != 0)
                        return false;
                    if (CuDriver.StreamSynchronize(ctx.Stream) != 0)
                        return false;

                    poison = false;
                    return true;
                }
            }
            finally
            {
                ReturnContext(ctx, poison);
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Trace.TraceWarning($"Glacier.Polaris CUDA unary kernel failed, falling back to CPU: {ex.GetType().Name}: {ex.Message}");
            return false;
        }
        finally
        {
            s_lifecycleLock.ExitReadLock();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool TryExecuteGpuVectorSum(ReadOnlySpan<float> input, int n, out float total)
    {
        total = 0f;
        if (!TryEnterNvidiaExecutionReadLock()) return false;
        float[]? blockSums = null;
        try
        {
            if (s_fnSum == IntPtr.Zero) return false;

            uint blockSize = 256;
            uint gridSize = (uint)(((long)n + (blockSize * 2L) - 1L) / (blockSize * 2L));
            int gridCount = checked((int)gridSize);
            blockSums = ArrayPool<float>.Shared.Rent(gridCount);

            nuint bytesIn = ComputeFloatBufferBytes(n);
            nuint bytesOut = checked((nuint)gridSize * (nuint)sizeof(float));

            if (CuDriver.CtxSetCurrent(s_cuContext) != 0) return false;
            var ctx = RentContext();
            if (ctx == null) return false;

            bool poison = true;
            try
            {
                if (!ctx.EnsureBuffers(bytesIn, 0, 0, bytesOut))
                    return false;

                fixed (float* pIn = input, pSums = blockSums)
                {
                    if (CuDriver.MemcpyHtoDAsync(ctx.DIn1, (IntPtr)pIn, bytesIn, ctx.Stream) != 0)
                        return false;

                    IntPtr dIn1 = ctx.DIn1;
                    IntPtr dOut = ctx.DOut;
                    int count = n;

                    void** kernelParams = stackalloc void*[3];
                    kernelParams[0] = &dIn1;
                    kernelParams[1] = &dOut;
                    kernelParams[2] = &count;

                    uint sharedMemBytes = blockSize * (uint)sizeof(float);
                    int launchRes = CuDriver.LaunchKernel(
                        s_fnSum,
                        gridSize, 1, 1,
                        blockSize, 1, 1,
                        sharedMemBytes, ctx.Stream,
                        (IntPtr)kernelParams,
                        IntPtr.Zero);

                    if (launchRes != 0) return false;

                    if (CuDriver.MemcpyDtoHAsync((IntPtr)pSums, ctx.DOut, bytesOut, ctx.Stream) != 0)
                        return false;
                    if (CuDriver.StreamSynchronize(ctx.Stream) != 0)
                        return false;

                    for (int i = 0; i < gridCount; i++) total += blockSums[i];
                    poison = false;
                    return true;
                }
            }
            finally
            {
                ReturnContext(ctx, poison);
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Trace.TraceWarning($"Glacier.Polaris CUDA sum kernel failed, falling back to CPU: {ex.GetType().Name}: {ex.Message}");
            total = 0f;
            return false;
        }
        finally
        {
            if (blockSums != null)
                ArrayPool<float>.Shared.Return(blockSums);
            s_lifecycleLock.ExitReadLock();
        }
    }

    #endregion
}
