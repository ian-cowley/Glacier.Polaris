using System;
using System.Runtime.InteropServices;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class HashKernels
    {
        public static UInt64Series Hash(ISeries series)
        {
            if (series == null) throw new ArgumentNullException(nameof(series));
            int len = series.Length;
            var result = new UInt64Series(series.Name, len);
            var resultSpan = result.Memory.Span;

            const ulong fnvOffset = 14695981039346656037UL;
            const ulong fnvPrime = 1099511628211UL;

            if (series is Utf8StringSeries u8)
            {
                for (int i = 0; i < len; i++)
                {
                    if (u8.ValidityMask.IsNull(i))
                    {
                        resultSpan[i] = fnvOffset;
                    }
                    else
                    {
                        var strSpan = u8.GetStringSpan(i);
                        ulong hash = fnvOffset;
                        for (int j = 0; j < strSpan.Length; j++)
                        {
                            hash ^= strSpan[j];
                            hash *= fnvPrime;
                        }
                        resultSpan[i] = hash;
                    }
                    result.ValidityMask.SetValid(i);
                }
                return result;
            }

            if (series is Series<int> sInt) HashUnmanaged(sInt, resultSpan);
            else if (series is Series<uint> sUint) HashUnmanaged(sUint, resultSpan);
            else if (series is Series<long> sLong) HashUnmanaged(sLong, resultSpan);
            else if (series is Series<ulong> sUlong) HashUnmanaged(sUlong, resultSpan);
            else if (series is Series<double> sDouble) HashUnmanaged(sDouble, resultSpan);
            else if (series is Series<float> sFloat) HashUnmanaged(sFloat, resultSpan);
            else if (series is Series<short> sShort) HashUnmanaged(sShort, resultSpan);
            else if (series is Series<ushort> sUshort) HashUnmanaged(sUshort, resultSpan);
            else if (series is Series<sbyte> sSbyte) HashUnmanaged(sSbyte, resultSpan);
            else if (series is Series<byte> sByte) HashUnmanaged(sByte, resultSpan);
            else if (series is Series<bool> sBool) HashUnmanaged(sBool, resultSpan);
            else if (series is DecimalSeries sDec)
            {
                var decSpan = sDec.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (sDec.ValidityMask.IsNull(i))
                    {
                        resultSpan[i] = fnvOffset;
                    }
                    else
                    {
                        var val = decSpan[i];
                        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref val, 1));
                        ulong hash = fnvOffset;
                        for (int j = 0; j < bytes.Length; j++)
                        {
                            hash ^= bytes[j];
                            hash *= fnvPrime;
                        }
                        resultSpan[i] = hash;
                    }
                    result.ValidityMask.SetValid(i);
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    if (series.ValidityMask.IsNull(i))
                    {
                        resultSpan[i] = fnvOffset;
                    }
                    else
                    {
                        string valStr = series.Get(i)?.ToString() ?? string.Empty;
                        ulong hash = fnvOffset;
                        foreach (char c in valStr)
                        {
                            hash ^= (byte)(c & 0xFF);
                            hash *= fnvPrime;
                            hash ^= (byte)((c >> 8) & 0xFF);
                            hash *= fnvPrime;
                        }
                        resultSpan[i] = hash;
                    }
                    result.ValidityMask.SetValid(i);
                }
            }

            return result;
        }

        private static void HashUnmanaged<T>(Series<T> source, Span<ulong> destination) where T : unmanaged
        {
            const ulong fnvOffset = 14695981039346656037UL;
            const ulong fnvPrime = 1099511628211UL;

            var span = source.Memory.Span;
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i))
                {
                    destination[i] = fnvOffset;
                }
                else
                {
                    T val = span[i];
                    ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref val, 1));
                    ulong hash = fnvOffset;
                    for (int j = 0; j < bytes.Length; j++)
                    {
                        hash ^= bytes[j];
                        hash *= fnvPrime;
                    }
                    destination[i] = hash;
                }
            }
        }
    }
}
