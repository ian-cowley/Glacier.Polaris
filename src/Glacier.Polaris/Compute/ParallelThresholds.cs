using System;
using System.Runtime.CompilerServices;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Provides dynamic, cache-aware and core-count-aware estimations for parallel threshold limits.
    /// Helps eliminate scheduling overhead on low-core/mobile devices and avoids cache thrashing on high-core servers.
    /// </summary>
    public static class ParallelThresholds
    {
        /// <summary>
        /// Calculates the minimum element count required to justify multi-threaded SIMD parallelization for filtering operations.
        /// Scales with the byte-size of T and logical processor count.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetFilterParallelThreshold<T>() where T : unmanaged
        {
            int elementSize = Unsafe.SizeOf<T>();
            int cores = Environment.ProcessorCount;

            // Single or dual core machines suffer from task scheduling and thread context-switching overheads.
            if (cores <= 2)
            {
                return int.MaxValue;
            }

            // Target working set per-thread to align with L2/L3 cache architectures.
            // We aim for at least ~1MB of input data, or 131,072 elements, whichever is larger.
            const int TargetBytes = 1024 * 1024;
            int elementsByByteCount = TargetBytes / elementSize;
            int baseThreshold = Math.Max(131072, elementsByByteCount);

            // Scale threshold up with logical core counts so each worker thread gets substantial work.
            int multiplier = Math.Clamp(cores / 4, 1, 8);
            return baseThreshold * multiplier;
        }

        /// <summary>
        /// Calculates the optimal number of parallel threads to spawn for memory-intensive radix sorting passes.
        /// Avoids memory bus saturation and hyperthreading cache pollution.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetSortThreadCount(int length, int elementSize)
        {
            int cores = Environment.ProcessorCount;
            if (cores <= 2) return 1;

            long totalBytes = (long)length * elementSize;

            // Below 1M elements, keep it sequential to avoid thread barrier synchronization cost.
            if (length <= 1_000_000) return 1;

            if (totalBytes < 8 * 1024 * 1024) // < 8MB (fits in typical L3 cache)
            {
                return Math.Min(2, cores);
            }
            if (totalBytes < 32 * 1024 * 1024) // < 32MB
            {
                return Math.Min(4, cores);
            }

            // Large datasets: cap logical threads at half of ProcessorCount to preserve memory bandwidth
            // and prevent resource-contention bottlenecks on SMT/HyperThreading.
            return Math.Clamp(cores / 2, 2, cores);
        }
    }
}
