namespace Glacier.Polaris.Compute;

/// <summary>
/// Specifies the hardware execution target for Glacier.Polaris accelerated columnar operations.
/// </summary>
public enum GpuTarget
{
    /// <summary>
    /// Automatically selects optimal execution between multi-threaded AVX-512 / AVX2 CPU and NVIDIA CUDA GPU
    /// based on series length, memory footprint, and hardware availability.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Bare-metal NVIDIA CUDA acceleration.
    /// </summary>
    Nvidia = 1,

    /// <summary>
    /// Unsupported AMD ROCm/HIP target. Explicitly selecting <see cref="Amd"/> throws <see cref="System.PlatformNotSupportedException"/>.
    /// </summary>
    Amd = 2,

    /// <summary>
    /// Distributes workload across available supported GPU accelerators (falls back to NVIDIA CUDA or CPU).
    /// </summary>
    DualGpu = 3,

    /// <summary>
    /// Forces CPU-only execution with AVX-512 / AVX2 SIMD vectorization.
    /// </summary>
    Cpu = 4
}
