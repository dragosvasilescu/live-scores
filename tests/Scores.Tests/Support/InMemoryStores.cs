using System.Collections.Concurrent;
using Scores.Domain.Events;
using Scores.Domain.State;
using Scores.Domain.Storage;

namespace Scores.Tests.Support;

public sealed class InMemoryEventStore : IMatchEventStore
{
    private readonly ConcurrentDictionary<(string MatchId, string EventId), (MatchEvent Event, long Seq)> _rows = new();

    public Task<bool> TryAddAsync(MatchEvent e, long sequenceNumber, CancellationToken ct) =>
        Task.FromResult(_rows.TryAdd((e.MatchId, e.EventId), (e, sequenceNumber)));

    public Task<IReadOnlyList<MatchEvent>> GetHistoryAsync(string matchId, CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<MatchEvent>>(_rows
            .Where(r => r.Key.MatchId == matchId)
            .OrderBy(r => r.Value.Seq)
            .Select(r => r.Value.Event)
            .ToList());
}

public sealed class InMemoryStatusStore : IMatchStatusStore
{
    public ConcurrentDictionary<string, MatchStatus> Rows { get; } = new();

    public Task SaveAsync(MatchStatus status, CancellationToken ct)
    {
        Rows[status.MatchId] = status;
        return Task.CompletedTask;
    }

    public Task<MatchStatus?> GetAsync(string matchId, CancellationToken ct) =>
        Task.FromResult(Rows.GetValueOrDefault(matchId));

    public Task<IReadOnlyList<MatchStatus>> ListAsync(CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<MatchStatus>>(Rows.Values.ToList());
}

public sealed class InMemoryRejectedSink : IRejectedEventSink
{
    public ConcurrentDictionary<string, RejectedEvent> Items { get; } = new();

    public Task WriteAsync(RejectedEvent rejected, CancellationToken ct)
    {
        Items[$"{rejected.PartitionId}/{rejected.SequenceNumber}"] = rejected;
        return Task.CompletedTask;
    }
}

public sealed class InMemoryStatsStore : IIngestionStatsStore
{
    private readonly ConcurrentDictionary<string, PartitionStats> _rows = new();

    public Task<PartitionStats?> GetAsync(string partitionId, CancellationToken ct) =>
        Task.FromResult(_rows.TryGetValue(partitionId, out var s) ? Copy(s) : null);

    public Task SaveAsync(PartitionStats stats, CancellationToken ct)
    {
        _rows[stats.PartitionId] = Copy(stats);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<PartitionStats>> ListAsync(CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<PartitionStats>>(_rows.Values.Select(Copy).ToList());

    private static PartitionStats Copy(PartitionStats s) => new()
    {
        PartitionId = s.PartitionId, Processed = s.Processed, Duplicates = s.Duplicates, Rejected = s.Rejected,
    };
}
