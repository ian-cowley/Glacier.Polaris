using Glacier.Polaris.Memory;
using Glacier.Storage.Arrow;

namespace Glacier.Polaris.Data
{
    public sealed class DecimalSeries : Series<decimal>
    {
        public int Precision { get; }
        public int Scale { get; }

        public DecimalSeries(string name, int length, System.Buffers.IMemoryOwner<decimal> data, ValidityMask validityMask, int precision = 38, int scale = 9) : base(name, length, data, validityMask)
        {
            Precision = precision;
            Scale = scale;
        }

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

        public override ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Decimal128, isNullable: ValidityMask.HasNulls);
            return new ArrowColumn(field, Length, ValidityMask.NullCount, ValidityMask.GetNullBitmapMemory(), ReadOnlyMemory<byte>.Empty, AsBytesMemory());
        }

        public override ISeries CloneEmpty(int length)
        {
            return new DecimalSeries(Name, length, Precision, Scale);
        }
    }
}
