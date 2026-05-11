using System;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Highly optimized SIMD arithmetic kernels using C# 11 Generic Math.
    /// Supports Int32, Float64, and any unmanaged type that implements INumber.
    /// </summary>
    public static class ArithmeticKernels
    {
        public static void Add<T>(ReadOnlySpan<T> left, ReadOnlySpan<T> right, Span<T> result) where T : unmanaged, INumber<T>
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && left.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                for (; i <= left.Length - step; i += step)
                {
                    var vLeft = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(left.Slice(i)));
                    var vRight = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(right.Slice(i)));
                    (vLeft + vRight).StoreUnsafe(ref MemoryMarshal.GetReference(result.Slice(i)));
                }
            }
            for (; i < left.Length; i++) result[i] = left[i] + right[i];
        }

        public static void Subtract<T>(ReadOnlySpan<T> left, ReadOnlySpan<T> right, Span<T> result) where T : unmanaged, INumber<T>
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && left.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                for (; i <= left.Length - step; i += step)
                {
                    var vLeft = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(left.Slice(i)));
                    var vRight = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(right.Slice(i)));
                    (vLeft - vRight).StoreUnsafe(ref MemoryMarshal.GetReference(result.Slice(i)));
                }
            }
            for (; i < left.Length; i++) result[i] = left[i] - right[i];
        }

        public static void Multiply<T>(ReadOnlySpan<T> left, ReadOnlySpan<T> right, Span<T> result) where T : unmanaged, INumber<T>
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && left.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                for (; i <= left.Length - step; i += step)
                {
                    var vLeft = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(left.Slice(i)));
                    var vRight = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(right.Slice(i)));
                    (vLeft * vRight).StoreUnsafe(ref MemoryMarshal.GetReference(result.Slice(i)));
                }
            }
            for (; i < left.Length; i++) result[i] = left[i] * right[i];
        }

        public static void Divide<T>(ReadOnlySpan<T> left, ReadOnlySpan<T> right, Span<T> result) where T : unmanaged, INumber<T>
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && left.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                for (; i <= left.Length - step; i += step)
                {
                    var vLeft = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(left.Slice(i)));
                    var vRight = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(right.Slice(i)));
                    (vLeft / vRight).StoreUnsafe(ref MemoryMarshal.GetReference(result.Slice(i)));
                }
            }
            for (; i < left.Length; i++) result[i] = left[i] / right[i];
        }

        // Vector-Scalar variants
        public static void AddScalar<T>(ReadOnlySpan<T> left, T right, Span<T> result) where T : unmanaged, INumber<T>
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && left.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                var vRight = Vector256.Create(right);
                for (; i <= left.Length - step; i += step)
                {
                    var vLeft = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(left.Slice(i)));
                    (vLeft + vRight).StoreUnsafe(ref MemoryMarshal.GetReference(result.Slice(i)));
                }
            }
            for (; i < left.Length; i++) result[i] = left[i] + right;
        }

        public static void SubtractScalar<T>(ReadOnlySpan<T> left, T right, Span<T> result) where T : unmanaged, INumber<T>
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && left.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                var vRight = Vector256.Create(right);
                for (; i <= left.Length - step; i += step)
                {
                    var vLeft = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(left.Slice(i)));
                    (vLeft - vRight).StoreUnsafe(ref MemoryMarshal.GetReference(result.Slice(i)));
                }
            }
            for (; i < left.Length; i++) result[i] = left[i] - right;
        }

        public static void MultiplyScalar<T>(ReadOnlySpan<T> left, T right, Span<T> result) where T : unmanaged, INumber<T>
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && left.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                var vRight = Vector256.Create(right);
                for (; i <= left.Length - step; i += step)
                {
                    var vLeft = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(left.Slice(i)));
                    (vLeft * vRight).StoreUnsafe(ref MemoryMarshal.GetReference(result.Slice(i)));
                }
            }
            for (; i < left.Length; i++) result[i] = left[i] * right;
        }

        public static void DivideScalar<T>(ReadOnlySpan<T> left, T right, Span<T> result) where T : unmanaged, INumber<T>
        {
            int i = 0;
            if (Vector256.IsHardwareAccelerated && left.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                var vRight = Vector256.Create(right);
                for (; i <= left.Length - step; i += step)
                {
                    var vLeft = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(left.Slice(i)));
                    (vLeft / vRight).StoreUnsafe(ref MemoryMarshal.GetReference(result.Slice(i)));
                }
            }
            for (; i < left.Length; i++) result[i] = left[i] / right;
        }
    }
}
