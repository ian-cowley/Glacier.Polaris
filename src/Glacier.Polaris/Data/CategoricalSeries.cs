using System;
using System.Collections.Generic;
using System.Linq;

namespace Glacier.Polaris.Data
{
    public sealed class CategoricalSeries : Series<uint>
    {
        public string[] RevMap { get; }

        public CategoricalSeries(string name, int length, string[] revMap) : base(name, length)
        {
            RevMap = revMap;
        }

        public CategoricalSeries(string name, uint[] codes, string[] revMap) : base(name, codes.Length)
        {
            codes.CopyTo(Memory);
            RevMap = revMap;
        }

        public static CategoricalSeries FromStrings(string name, ReadOnlySpan<string> strings)
        {
            if (StringCache.IsEnabled)
            {
                var codes = new uint[strings.Length];
                for (int i = 0; i < strings.Length; i++)
                {
                    codes[i] = StringCache.GetOrCreate(strings[i]);
                }
                var series = new CategoricalSeries(name, codes, StringCache.GetRevMap());
                for (int i = 0; i < strings.Length; i++)
                {
                    if (strings[i] == null) series.ValidityMask.SetNull(i);
                }
                return series;
            }

            var uniqueStrings = new Dictionary<string, uint>();
            var codesLocal = new uint[strings.Length];
            var revMap = new List<string>();

            for (int i = 0; i < strings.Length; i++)
            {
                string s = strings[i];
                if (s == null)
                {
                    codesLocal[i] = uint.MaxValue;
                    continue;
                }

                if (!uniqueStrings.TryGetValue(s, out uint code))
                {
                    code = (uint)revMap.Count;
                    uniqueStrings[s] = code;
                    revMap.Add(s);
                }
                codesLocal[i] = code;
            }

            var resultSeries = new CategoricalSeries(name, codesLocal, revMap.ToArray());
            for (int i = 0; i < strings.Length; i++)
            {
                if (strings[i] == null) resultSeries.ValidityMask.SetNull(i);
            }
            return resultSeries;
        }
        public override object? Get(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            uint code = Memory.Span[i];
            if (code < (uint)RevMap.Length)
                return RevMap[code];
            return null;
        }
        public override void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is CategoricalSeries t)
            {
                // Verify revmap matches or implement global string cache
                // For now, assume simple take within same series context
                base.Take(t, indices);
            }
            else throw new ArgumentException("Target series type mismatch.");
        }
    }
}
