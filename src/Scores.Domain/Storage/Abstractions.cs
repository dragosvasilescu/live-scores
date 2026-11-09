using Scores.Domain.Events;
using Scores.Domain.State;

namespace Scores.Domain.Storage;

public interface IMatchEventStore
{
    Task<bool> TryAddAsync(MatchEvent e, long sequenceNumber, CancellationToken ct);

    Task<IReadOnlyList<MatchEvent>> GetHistoryAsync(string matchId, CancellationToken ct);
}

public interface IMatchStatusStore
{
    Task SaveAsync(MatchStatus status, CancellationToken ct);

    Task<MatchStatus?> GetAsync(string matchId, CancellationToken ct);

    Task<IReadOnlyList<MatchStatus>> ListAsync(CancellationToken ct);
}

public interface IRejectedEventSink
{
    Task WriteAsync(RejectedEvent rejected, CancellationToken ct);
}

public sealed record RejectedEvent(
    string Reason,
    string Detail,
    string PartitionId,
    long SequenceNumber,
    DateTimeOffset EnqueuedAt,
    string RawBody);

public interface IIngestionStatsStore
{
    Task<PartitionStats?> GetAsync(string partitionId, CancellationToken ct);

    Task SaveAsync(PartitionStats stats, CancellationToken ct);

    Task<IReadOnlyList<PartitionStats>> ListAsync(CancellationToken ct);
}

public sealed class PartitionStats
{
    public required string PartitionId { get; init; }

    public long Processed { get; set; }

    public long Duplicates { get; set; }

    public long Rejected { get; set; }
}
