using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Scores.Domain.Events;
using Scores.Domain.State;

namespace Scores.Domain.Storage;

public sealed class TableMatchEventStore(TableClient table) : IMatchEventStore
{
    public const string TableName = "matchevents";

    public async Task<bool> TryAddAsync(MatchEvent e, long sequenceNumber, CancellationToken ct)
    {
        var entity = new TableEntity(e.MatchId, e.EventId)
        {
            ["Sequence"] = sequenceNumber,
            ["Type"] = e.Type.ToString(),
            ["Team"] = e.Team,
            ["Player"] = e.Player,
            ["Minute"] = e.Minute,
            ["OccurredAt"] = e.OccurredAt,
            ["Card"] = e.Card?.ToString(),
            ["PlayerOff"] = e.PlayerOff,
        };
        try
        {
            await table.AddEntityAsync(entity, ct);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<MatchEvent>> GetHistoryAsync(string matchId, CancellationToken ct)
    {
        var rows = new List<TableEntity>();
        await foreach (var row in table.QueryAsync<TableEntity>(r => r.PartitionKey == matchId, cancellationToken: ct))
            rows.Add(row);

        return rows
            .OrderBy(r => r.GetInt64("Sequence"))
            .Select(r => new MatchEvent(
                EventId: r.RowKey,
                MatchId: r.PartitionKey,
                Type: Enum.Parse<MatchEventType>(r.GetString("Type")),
                Team: r.GetString("Team"),
                Player: r.GetString("Player"),
                Minute: r.GetInt32("Minute") ?? 0,
                OccurredAt: r.GetDateTimeOffset("OccurredAt") ?? default,
                Card: r.GetString("Card") is { } card ? Enum.Parse<CardColour>(card) : null,
                PlayerOff: r.GetString("PlayerOff")))
            .ToList();
    }
}

public sealed class TableMatchStatusStore(TableClient table) : IMatchStatusStore
{
    public const string TableName = "matchstatus";
    private const string RowKey = "status";

    public Task SaveAsync(MatchStatus s, CancellationToken ct) =>
        table.UpsertEntityAsync(new TableEntity(s.MatchId, RowKey)
        {
            ["Scoreline"] = s.Scoreline,
            ["Score"] = JsonSerializer.Serialize(s.Score),
            ["Minute"] = s.Minute,
            ["EventCount"] = s.EventCount,
            ["LastEventAt"] = s.LastEventAt,
        }, TableUpdateMode.Replace, ct);

    public async Task<MatchStatus?> GetAsync(string matchId, CancellationToken ct)
    {
        var response = await table.GetEntityIfExistsAsync<TableEntity>(matchId, RowKey, cancellationToken: ct);
        return response.HasValue ? ToStatus(response.Value!) : null;
    }

    public async Task<IReadOnlyList<MatchStatus>> ListAsync(CancellationToken ct)
    {
        var result = new List<MatchStatus>();
        await foreach (var row in table.QueryAsync<TableEntity>(r => r.RowKey == RowKey, cancellationToken: ct))
            result.Add(ToStatus(row));
        return result;
    }

    private static MatchStatus ToStatus(TableEntity r) => new(
        r.PartitionKey,
        JsonSerializer.Deserialize<Dictionary<string, int>>(r.GetString("Score"))!,
        r.GetInt32("Minute") ?? 0,
        r.GetInt32("EventCount") ?? 0,
        r.GetDateTimeOffset("LastEventAt"));
}

public sealed class TableIngestionStatsStore(TableClient table) : IIngestionStatsStore
{
    public const string TableName = "ingestionstats";
    private const string PartitionKey = "partition";

    public async Task<PartitionStats?> GetAsync(string partitionId, CancellationToken ct)
    {
        var response = await table.GetEntityIfExistsAsync<TableEntity>(PartitionKey, partitionId, cancellationToken: ct);
        return response.HasValue ? ToStats(response.Value!) : null;
    }

    public Task SaveAsync(PartitionStats s, CancellationToken ct) =>
        table.UpsertEntityAsync(new TableEntity(PartitionKey, s.PartitionId)
        {
            ["Processed"] = s.Processed,
            ["Duplicates"] = s.Duplicates,
            ["Rejected"] = s.Rejected,
        }, TableUpdateMode.Replace, ct);

    public async Task<IReadOnlyList<PartitionStats>> ListAsync(CancellationToken ct)
    {
        var result = new List<PartitionStats>();
        await foreach (var row in table.QueryAsync<TableEntity>(r => r.PartitionKey == PartitionKey, cancellationToken: ct))
            result.Add(ToStats(row));
        return result;
    }

    private static PartitionStats ToStats(TableEntity r) => new()
    {
        PartitionId = r.RowKey,
        Processed = r.GetInt64("Processed") ?? 0,
        Duplicates = r.GetInt64("Duplicates") ?? 0,
        Rejected = r.GetInt64("Rejected") ?? 0,
    };
}

public sealed class BlobRejectedEventSink(BlobContainerClient container) : IRejectedEventSink
{
    public const string ContainerName = "rejected-events";

    public async Task WriteAsync(RejectedEvent rejected, CancellationToken ct)
    {
        var name = $"p{rejected.PartitionId}/{rejected.SequenceNumber:D12}.json";
        var json = JsonSerializer.SerializeToUtf8Bytes(rejected, ScoresJson.Options);
        await container.GetBlobClient(name).UploadAsync(
            new BinaryData(json),
            new BlobUploadOptions
            {
                HttpHeaders = new BlobHttpHeaders { ContentType = "application/json" },
                Metadata = new Dictionary<string, string> { ["reason"] = rejected.Reason },
            },
            ct);
    }
}
