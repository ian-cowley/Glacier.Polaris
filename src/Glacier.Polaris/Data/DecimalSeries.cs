using Glacier.Polaris.Memory;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Glacier.Polaris.Data
{
    public sealed class DecimalSeries : Series<decimal>
    {
        public int Precision { get; }
        public int Scale { get; }

        public DecimalSeries(string name, int length, int precision = 38, int scale = 9) : base(name, length)
        {
            Precision = precision;
            Scale = scale;
        }

        public DecimalSeries(string name, decimal?[] data, int precision = 38, int scale = 9) : base(name, data.Length)
        {
            Precision = precision;
            Scale = scale;
            for (int i = 0; i < data.Length; i++)
            {
                if (data[i].HasValue)
                {
                    Memory.Span[i] = data[i]!.Value;
                    ValidityMask.SetValid(i);
                }
                else
                {
                    ValidityMask.SetNull(i);
                }
            }
        }

        public decimal? GetValue(int i) => ValidityMask.IsValid(i) ? Memory.Span[i] : (decimal?)null;

        public override IArrowArray ToArrowArray()
        {
            var type = new Decimal128Type(Precision, Scale);
            var builder = new Decimal128Array.Builder(type);
            var span = Memory.Span;
            for (int i = 0; i < Length; i++)
            {
                if (ValidityMask.IsValid(i))
                    builder.Append(span[i]);
                else
                    builder.AppendNull();
            }
            return builder.Build();
        }

        public override ISeries CloneEmpty(int length)
        {
            return new DecimalSeries(Name, length, Precision, Scale);
        }
    }
}
