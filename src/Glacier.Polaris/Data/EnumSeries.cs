using System;
using System.Collections.Generic;
using System.Linq;

namespace Glacier.Polaris.Data
{
    public sealed class EnumSeries : Series<uint>
    {
        public string[] Categories { get; }
        private readonly Dictionary<string, uint> _categoryMap;

        public EnumSeries(string name, int length, string[] categories) : base(name, length)
        {
            Categories = categories;
            _categoryMap = categories.Select((s, i) => new { s, i }).ToDictionary(x => x.s, x => (uint)x.i);
        }

        public EnumSeries(string name, uint[] codes, string[] categories) : base(name, codes.Length)
        {
            codes.CopyTo(Memory);
            Categories = categories;
            _categoryMap = categories.Select((s, i) => new { s, i }).ToDictionary(x => x.s, x => (uint)x.i);
        }

        public static EnumSeries FromStrings(string name, ReadOnlySpan<string> strings, string[] categories)
        {
            var series = new EnumSeries(name, strings.Length, categories);
            var span = series.Memory.Span;
            var categoryMap = series._categoryMap;

            for (int i = 0; i < strings.Length; i++)
            {
                string s = strings[i];
                if (s == null)
                {
                    series.ValidityMask.SetNull(i);
                    continue;
                }

                if (categoryMap.TryGetValue(s, out uint code))
                {
                    span[i] = code;
                    series.ValidityMask.SetValid(i);
                }
                else
                {
                    series.ValidityMask.SetNull(i);
                }
            }
            return series;
        }

        public override object? Get(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            uint code = Memory.Span[i];
            if (code < Categories.Length) return Categories[code];
            return null;
        }

        public override void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is EnumSeries t)
            {
                if (!Categories.SequenceEqual(t.Categories))
                    throw new ArgumentException("Enum categories mismatch.");

                base.Take(t, indices);
            }
            else throw new ArgumentException("Target series type mismatch.");
        }

        public override ISeries CloneEmpty(int length)
        {
            return new EnumSeries(Name ?? "", length, Categories);
        }
    }
}
