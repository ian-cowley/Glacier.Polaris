using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests;

[Collection("RegexCache")]
public class StringKernelsRegexCacheTests
{
    [Fact]
    public void RegexCache_RespectsCapacityBoundAndLruEviction()
    {
        StringKernels.ClearRegexCacheForTests();
        const RegexOptions opts = RegexOptions.Compiled | RegexOptions.CultureInvariant;
        TimeSpan timeout = TimeSpan.FromSeconds(5);

        int cap = StringKernels.RegexCacheCapacity;
        Regex? firstPinned = null;
        Regex? secondUnpinned = null;

        for (int i = 0; i < cap; i++)
        {
            string pattern = $@"^item_{i}_[a-z]+\d+$";
            var r = StringKernels.GetOrAddRegex(pattern, opts, timeout);
            if (i == 0) firstPinned = r;
            if (i == 1) secondUnpinned = r;
        }

        Assert.Equal(cap, StringKernels.RegexCacheCount);

        // Promote item 0 so item 1 becomes the LRU victim
        var promoted = StringKernels.GetOrAddRegex("^item_0_[a-z]+\\d+$", opts, timeout);
        Assert.Same(firstPinned, promoted);

        // Insert 50 more distinct patterns -> count stays strictly capped at RegexCacheCapacity
        for (int i = cap; i < cap + 50; i++)
        {
            StringKernels.GetOrAddRegex($@"^item_{i}_[a-z]+\d+$", opts, timeout);
        }

        Assert.Equal(cap, StringKernels.RegexCacheCount);

        // Item 1 was evicted, so re-requesting it creates a new instance; item 0 may also have been evicted after 50 inserts,
        // so verify explicitly with a single eviction step below.
        Assert.NotSame(secondUnpinned, StringKernels.GetOrAddRegex("^item_1_[a-z]+\\d+$", opts, timeout));

        // Deterministic single-eviction LRU verification
        StringKernels.ClearRegexCacheForTests();
        var keep = StringKernels.GetOrAddRegex("^keep_[0-9]+$", opts, timeout);
        var victim = StringKernels.GetOrAddRegex("^victim_[0-9]+$", opts, timeout);
        for (int i = 2; i < cap; i++)
        {
            StringKernels.GetOrAddRegex($@"^fill_{i}_[0-9]+$", opts, timeout);
        }
        // Touch 'keep' right before triggering 1 eviction
        Assert.Same(keep, StringKernels.GetOrAddRegex("^keep_[0-9]+$", opts, timeout));
        StringKernels.GetOrAddRegex("^overflow_trigger_[0-9]+$", opts, timeout);

        Assert.Equal(cap, StringKernels.RegexCacheCount);
        Assert.Same(keep, StringKernels.GetOrAddRegex("^keep_[0-9]+$", opts, timeout));
        Assert.NotSame(victim, StringKernels.GetOrAddRegex("^victim_[0-9]+$", opts, timeout));
    }

    [Fact]
    public void RegexCache_KeysByPatternOptionsAndTimeout()
    {
        StringKernels.ClearRegexCacheForTests();
        const string pattern = @"^[a-z]+\d{2}$";
        TimeSpan t5 = TimeSpan.FromSeconds(5);
        TimeSpan t1 = TimeSpan.FromSeconds(1);

        var r1 = StringKernels.GetOrAddRegex(pattern, RegexOptions.CultureInvariant, t5);
        var r1Hit = StringKernels.GetOrAddRegex(pattern, RegexOptions.CultureInvariant, t5);
        var rDiffOpts = StringKernels.GetOrAddRegex(pattern, RegexOptions.CultureInvariant | RegexOptions.IgnoreCase, t5);
        var rDiffTimeout = StringKernels.GetOrAddRegex(pattern, RegexOptions.CultureInvariant, t1);

        Assert.Same(r1, r1Hit);
        Assert.NotSame(r1, rDiffOpts);
        Assert.NotSame(r1, rDiffTimeout);
        Assert.NotSame(rDiffOpts, rDiffTimeout);
        Assert.Equal(3, StringKernels.RegexCacheCount);
    }

    [Fact]
    public void RegexCache_InvalidPattern_ThrowsWithoutPoisoningCache()
    {
        StringKernels.ClearRegexCacheForTests();
        StringKernels.GetOrAddRegex(@"^\d+$", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5));
        Assert.Equal(1, StringKernels.RegexCacheCount);

        Assert.ThrowsAny<ArgumentException>(() =>
            StringKernels.GetOrAddRegex("[unclosed(", RegexOptions.Compiled, TimeSpan.FromSeconds(5)));

        Assert.Equal(1, StringKernels.RegexCacheCount);
    }

    [Fact]
    public void StringKernels_ConcurrentRegexMatchExtractAndExtractAll_RespectsCacheBound()
    {
        StringKernels.ClearRegexCacheForTests();
        using var series = new Utf8StringSeries("s", ["alpha-12", "beta-34-56", "gamma"]);
        int[] offsets = [0, 8, 18, 23];
        byte[] dataBytes = Encoding.UTF8.GetBytes("alpha-12beta-34-56gamma");

        Parallel.For(0, 16, threadIdx =>
        {
            Span<int> matchMask = stackalloc int[3];
            for (int iter = 0; iter < 60; iter++)
            {
                string pat = (iter % 3 == 0)
                    ? @"\d+"
                    : $@"^[a-z]+-({(threadIdx * 60 + iter) % 300}|\d+).*$";

                StringKernels.RegexMatch(dataBytes, offsets, pat, matchMask);
                using var extracted = StringKernels.Extract(series, @"\d+");
                Assert.Equal("12", extracted.GetString(0));
                Assert.Equal("34", extracted.GetString(1));

                using var extractedAll = StringKernels.ExtractAll(series, @"\d+");
                var row1 = (object?[]?)extractedAll.Get(1);
                Assert.NotNull(row1);
                Assert.Equal(2, row1!.Length);
            }
        });

        Assert.InRange(StringKernels.RegexCacheCount, 1, StringKernels.RegexCacheCapacity);
    }
}
