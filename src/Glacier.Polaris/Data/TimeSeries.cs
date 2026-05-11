using System;
using Glacier.Polaris.Memory;
using Apache.Arrow;

namespace Glacier.Polaris.Data
{
    public sealed class TimeSeries : Series<long>
    {
        public TimeSeries(string name, int length) : base(name, length) { }

        public TimeSeries(string name, TimeSpan[] data) : base(name, data.Length)
        {
            var span = Memory.Span;
            for (int i = 0; i < data.Length; i++)
            {
                span[i] = data[i].Ticks * 100L; // Convert ticks (100ns) to nanoseconds
                ValidityMask.SetValid(i);
            }
        }

        public int GetHour(int i)
        {
            if (ValidityMask.IsNull(i)) return 0;
            return (int)((Memory.Span[i] / 3_600_000_000_000L) % 24);
        }

        public int GetMinute(int i)
        {
            if (ValidityMask.IsNull(i)) return 0;
            return (int)((Memory.Span[i] / 60_000_000_000L) % 60);
        }

        public int GetSecond(int i)
        {
            if (ValidityMask.IsNull(i)) return 0;
            return (int)((Memory.Span[i] / 1_000_000_000L) % 60);
        }

        public override IArrowArray ToArrowArray()
        {
            var nullBitmapBuilder = new ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) nullBitmapBuilder.Append(ValidityMask.IsValid(i));

            return new Time64Array(
                new Apache.Arrow.Types.Time64Type(Apache.Arrow.Types.TimeUnit.Nanosecond),
                new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(Memory.Span).ToArray()),
                nullBitmapBuilder.Build(),
                Length,
                ValidityMask.NullCount,
                0);
        }
    }
}
