using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Glacier.Polaris;
using Glacier.Polaris.Data;

var builder = WebApplication.CreateBuilder(args);

// Enable CORS and Response Compression
builder.Services.AddCors();
builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

app.UseCors(policy => policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader());
app.UseStaticFiles(); // Serves index.html, CSS, JS from wwwroot

// Generate realistic dataset (5,000 rows)
var dataset = GenerateRealisticData(5000);

app.MapGet("/api/dataset/overview", () =>
{
    try
    {
        var schema = dataset.Schema.ToDictionary(k => k.Key, k => k.Value.Name);
        var sample = dataset.Sample(10).ToDictionary();
        var describe = dataset.Describe().ToDictionary();

        return Results.Json(new
        {
            rowCount = dataset.RowCount,
            colCount = dataset.Columns.Count,
            estimatedMemory = dataset.EstimatedSize(),
            schema = schema,
            sample = sample,
            describe = describe
        });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapGet("/api/analytics/distribution", (string? column, int? bins, double? bandwidth) =>
{
    try
    {
        string col = column ?? "revenue";
        int b = bins ?? 10;
        double bw = bandwidth ?? 50.0;

        var hist = dataset.Hist(col, b).ToDictionary();
        var kde = dataset.Kde(col, bw, 50).ToDictionary();

        return Results.Json(new
        {
            histogram = hist,
            kde = kde
        });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapGet("/api/analytics/aggregations", (string? groupBy) =>
{
    try
    {
        string gb = groupBy ?? "category";

        var agg = dataset.GroupBy(gb).Agg(
            ("revenue", "sum"),
            ("revenue", "mean"),
            ("units_sold", "sum"),
            ("units_sold", "mean"),
            ("satisfaction", "mean"),
            ("revenue", "max"),
            ("revenue", "min")
        ).ToDictionary();

        return Results.Json(new { aggregations = agg });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapGet("/api/analytics/temporal", (int? window) =>
{
    try
    {
        int w = window ?? 7;

        // Group by day, sum revenue, then compute rolling mean
        var daily = dataset.GroupBy("day").Agg(
            ("revenue", "sum")
        ).Sort("day");

        // Compute rolling mean using functions
        var rolling = daily.Select(
            Expr.Col("day"),
            Expr.Col("revenue_sum"),
            Glacier.Polaris.Functions.RollingMean("revenue_sum", w).Alias("rolling_mean")
        ).ToDictionary();

        return Results.Json(new { temporal = rolling });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapGet("/api/analytics/wrangling", (string? categoryFilter) =>
{
    try
    {
        string cat = categoryFilter ?? "Electronics";

        // Filter exact category match
        var filtered = dataset.Filter(
            Expr.Col("category") == Expr.Lit(cat)
        );

        // Value counts on region
        var vCounts = filtered.GetColumn("region").ValueCounts(sort: true).ToDictionary();

        return Results.Json(new
        {
            filteredCount = filtered.RowCount,
            sample = filtered.Sample(Math.Min(10, filtered.RowCount)).ToDictionary(),
            regionCounts = vCounts
        });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

// Fallback to serve index.html for root
app.MapFallbackToFile("index.html");

app.Run();

DataFrame GenerateRealisticData(int rows)
{
    var rng = new Random(42);
    var categories = new[] { "Electronics", "Apparel", "Home & Kitchen", "Books", "Toys" };
    var regions = new[] { "North America", "Europe", "Asia", "Latin America" };

    var ids = new int[rows];
    var days = new int[rows];
    var cats = new string?[rows];
    var regs = new string?[rows];
    var revs = new double[rows];
    var units = new int[rows];
    var sats = new double[rows];
    var repeat = new bool[rows];

    for (int i = 0; i < rows; i++)
    {
        ids[i] = 1000 + i;
        days[i] = rng.Next(1, 91); // 90 days
        cats[i] = categories[rng.Next(categories.Length)];
        regs[i] = regions[rng.Next(regions.Length)];
        revs[i] = Math.Round(rng.NextDouble() * 1490 + 10, 2);
        units[i] = rng.Next(1, 21);
        sats[i] = Math.Round(rng.NextDouble() * 4 + 1, 1);
        repeat[i] = rng.NextDouble() > 0.4;
    }

    var idCol = new Int32Series("id", ids);
    var dayCol = new Int32Series("day", days);
    var catCol = Utf8StringSeries.FromStrings("category", cats);
    var regCol = Utf8StringSeries.FromStrings("region", regs);
    var revCol = new Float64Series("revenue", revs);
    var unitCol = new Int32Series("units_sold", units);
    var satCol = new Float64Series("satisfaction", sats);
    var repCol = new BooleanSeries("is_repeat", repeat);

    return new DataFrame(new ISeries[] { idCol, dayCol, catCol, regCol, revCol, unitCol, satCol, repCol });
}
