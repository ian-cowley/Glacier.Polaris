namespace Glacier.Polaris.Compute;

/// <summary>
/// Specifies the hardware execution target for Glacier.Polaris accelerated columnar operations.
/// </summary>
public enum GpuTarget
{
    /// <summary>
    /// Automatically selects optimal execution between multi-threaded AVX-512 CPU, NVIDIA dGPU, and AMD APU
    /// based on series length, memory footprint, and hardware availability.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Bare-metal NVIDIA CUDA acceleration (RTX 4060 / Ada Lovelace).
    /// </summary>
    Nvidia = 1,

    /// <summary>
    /// Bare-metal AMD ROCm/HIP acceleration (Radeon 890M / RDNA 3.5).
    /// </summary>
    Amd = 2,

    /// <summary>
    /// Distributes workload across both GPUs when available.
    /// </summary>
    DualGpu = 3,

    /// <summary>
    /// Forces CPU-only execution with AVX-512 / AVX2 SIMD vectorization.
    /// </summary>
    Cpu = 4
}
