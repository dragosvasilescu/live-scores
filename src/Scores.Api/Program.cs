using Microsoft.AspNetCore.Http.Json;
using Scores.Domain;
using Scores.Domain.Events;
using Scores.Domain.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddScoresStorage(builder.Configuration);
builder.Services.Configure<JsonOptions>(o =>
{
    foreach (var c in ScoresJson.Options.Converters)
        o.SerializerOptions.Converters.Add(c);
});

var app = builder.Build();

await app.Services.GetRequiredService<StorageBootstrap>().EnsureCreatedAsync(CancellationToken.None);

app.MapGet("/", () => Results.Redirect("/matches"));

app.MapGet("/matches", async (IMatchStatusStore statuses, CancellationToken ct) =>
    (await statuses.ListAsync(ct))
        .OrderByDescending(m => m.LastEventAt)
        .Select(m => new { m.MatchId, m.Scoreline, m.Score, m.Minute, m.LastEventAt }));

app.MapGet("/matches/{matchId}", async (string matchId, IMatchStatusStore statuses, IMatchEventStore history, CancellationToken ct) =>
{
    var status = await statuses.GetAsync(matchId, ct);
    if (status is null)
        return Results.NotFound(new { error = $"match '{matchId}' not found" });

    var events = await history.GetHistoryAsync(matchId, ct);
    return Results.Ok(new
    {
        status.MatchId,
        status.Scoreline,
        status.Score,
        status.Minute,
        status.LastEventAt,
        Goals = events.Where(e => e.Type == MatchEventType.Goal).Select(e => new { e.Team, e.Player, e.Minute }),
        Cards = events.Where(e => e.Type == MatchEventType.Card).Select(e => new { e.Team, e.Player, e.Card, e.Minute }),
        Subs = events.Where(e => e.Type == MatchEventType.Sub).Select(e => new { e.Team, On = e.Player, Off = e.PlayerOff, e.Minute }),
    });
});

app.MapGet("/matches/{matchId}/events", async (string matchId, IMatchEventStore history, CancellationToken ct) =>
    await history.GetHistoryAsync(matchId, ct));

app.MapGet("/ingestion/health", async (IIngestionStatsStore stats, CancellationToken ct) =>
{
    var partitions = (await stats.ListAsync(ct)).OrderBy(p => p.PartitionId.PadLeft(4, '0')).ToList();
    return new
    {
        Processed = partitions.Sum(p => p.Processed),
        Duplicates = partitions.Sum(p => p.Duplicates),
        Rejected = partitions.Sum(p => p.Rejected),
        Partitions = partitions,
    };
});

app.MapGet("/healthz", () => Results.Ok("ok"));

app.Run();

public partial class Program;
