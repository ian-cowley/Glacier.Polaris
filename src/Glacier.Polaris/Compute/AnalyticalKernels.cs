using System;
using System.Collections.Generic;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class AnalyticalKernels
    {
        public static DataFrame Histogram(ISeries series, int bins)
        {
            if (bins <= 0) throw new ArgumentException("Number of bins must be positive");
            int n = series.Length;

            if (n == 0)
            {
                return new DataFrame(new ISeries[]
                {
                    new Float64Series("bin_start", 0),
                    new Float64Series("bin_end", 0),
                    new Int32Series("count", 0)
                });
            }

            // Find min and max
            double min = double.MaxValue;
            double max = double.MinValue;
            int validCount = 0;

            for (int i = 0; i < n; i++)
            {
                if (series.ValidityMask.IsValid(i))
                {
                    double val = GetValueAsDouble(series, i);
                    if (val < min) min = val;
                    if (val > max) max = val;
                    validCount++;
                }
            }

            if (validCount == 0)
            {
                return new DataFrame(new ISeries[]
                {
                    new Float64Series("bin_start", bins),
                    new Float64Series("bin_end", bins),
                    new Int32Series("count", bins)
                });
            }

            // If min == max, put everything in a single bin
            if (Math.Abs(max - min) < 1e-9)
            {
                var bs = new Float64Series("bin_start", 1);
                var be = new Float64Series("bin_end", 1);
                var bc = new Int32Series("count", 1);
                bs.Memory.Span[0] = min - 0.5;
                be.Memory.Span[0] = min + 0.5;
                bc.Memory.Span[0] = validCount;
                return new DataFrame(new ISeries[] { bs, be, bc });
            }

            double binSize = (max - min) / bins;
            var binStarts = new Float64Series("bin_start", bins);
            var binEnds = new Float64Series("bin_end", bins);
            var counts = new Int32Series("count", bins);

            for (int j = 0; j < bins; j++)
            {
                binStarts.Memory.Span[j] = min + j * binSize;
                binEnds.Memory.Span[j] = min + (j + 1) * binSize;
            }

            // Count occurrences
            for (int i = 0; i < n; i++)
            {
                if (series.ValidityMask.IsValid(i))
                {
                    double val = GetValueAsDouble(series, i);
                    int binIdx = (int)((val - min) / binSize);
                    if (binIdx >= bins) binIdx = bins - 1;
                    if (binIdx < 0) binIdx = 0;
                    counts.Memory.Span[binIdx]++;
                }
            }

            return new DataFrame(new ISeries[] { binStarts, binEnds, counts });
        }

        public static DataFrame Kde(ISeries series, double bandwidth, int gridPoints = 100)
        {
            if (bandwidth <= 0) throw new ArgumentException("Bandwidth must be positive");
            if (gridPoints <= 0) throw new ArgumentException("Grid points must be positive");

            int n = series.Length;
            if (n == 0)
            {
                return new DataFrame(new ISeries[]
                {
                    new Float64Series("grid", 0),
                    new Float64Series("density", 0)
                });
            }

            // Gather valid double values
            var valsList = new List<double>();
            for (int i = 0; i < n; i++)
            {
                if (series.ValidityMask.IsValid(i))
                {
                    valsList.Add(GetValueAsDouble(series, i));
                }
            }

            int validN = valsList.Count;
            if (validN == 0)
            {
                return new DataFrame(new ISeries[]
                {
                    new Float64Series("grid", gridPoints),
                    new Float64Series("density", gridPoints)
                });
            }

            double[] vals = valsList.ToArray();
            Array.Sort(vals);

            double min = vals[0];
            double max = vals[validN - 1];

            // Define grid coordinates spanning [min - 3h, max + 3h]
            double gridMin = min - 3.0 * bandwidth;
            double gridMax = max + 3.0 * bandwidth;
            double step = (gridMax - gridMin) / (gridPoints - 1);

            var grids = new Float64Series("grid", gridPoints);
            var densities = new Float64Series("density", gridPoints);

            double factor = 1.0 / (validN * bandwidth * Math.Sqrt(2.0 * Math.PI));

            // Compute Gaussian KDE density for each grid coordinate
            System.Threading.Tasks.Parallel.For(0, gridPoints, g =>
            {
                double xg = gridMin + g * step;
                grids.Memory.Span[g] = xg;

                double sum = 0;
                for (int i = 0; i < validN; i++)
                {
                    double diff = (xg - vals[i]) / bandwidth;
                    sum += Math.Exp(-0.5 * diff * diff);
                }

                densities.Memory.Span[g] = sum * factor;
            });

            return new DataFrame(new ISeries[] { grids, densities });
        }

        private static double GetValueAsDouble(ISeries s, int idx)
        {
            if (s is Int32Series i32) return i32.Memory.Span[idx];
            if (s is Float64Series f64) return f64.Memory.Span[idx];
            if (s is Int64Series i64) return i64.Memory.Span[idx];
            if (s is Float32Series f32) return f32.Memory.Span[idx];
            if (s is Int16Series i16) return i16.Memory.Span[idx];
            if (s is Int8Series i8) return i8.Memory.Span[idx];
            if (s is UInt32Series u32) return u32.Memory.Span[idx];
            if (s is UInt64Series u64) return u64.Memory.Span[idx];
            if (s is UInt16Series u16) return u16.Memory.Span[idx];
            if (s is UInt8Series u8) return u8.Memory.Span[idx];
            var val = s.Get(idx);
            return val != null ? Convert.ToDouble(val) : 0;
        }
    }
}
