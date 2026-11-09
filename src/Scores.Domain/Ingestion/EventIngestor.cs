using System.Text;
using Microsoft.Extensions.Logging;
using Scores.Domain.Events;
using Scores.Domain.State;
using Scores.Domain.Storage;

namespace Scores.Domain.Ingestion;

public sealed record IncomingEvent(
    ReadOnlyMemory<byte> Body,
    string PartitionId,
    long SequenceNumber,
    DateTimeOffset EnqueuedTime);

public enum IngestOutcome
{
    Processed,
    Duplicate,
    Rejected,
}

public sealed class EventIngestor(
    IMatchEventStore history,
    IMatchStatusStore statuses,
    IRejectedEventSink rejects,
    IIngestionStatsStore stats,
    ILogger<EventIngestor> logger)
{
    public async Task<IngestOutcome> ProcessAsync(IncomingEvent raw, CancellationToken ct)
    {
        var outcome = await ApplyAsync(raw, ct);
        await CountAsync(raw.PartitionId, outcome, ct);
        return outcome;
    }

    private async Task<IngestOutcome> ApplyAsync(IncomingEvent raw, CancellationToken ct)
    {
        var parsed = SafeParse(raw);
        if (!parsed.IsValid)
        {
            logger.LogWarning("Rejected p{Partition}#{Seq}: {Reason} ({Detail})",
                raw.PartitionId, raw.SequenceNumber, parsed.Reason, parsed.Detail);
            await rejects.WriteAsync(new RejectedEvent(
                parsed.Reason!.Value.ToString(), parsed.Detail!,
                raw.PartitionId, raw.SequenceNumber, raw.EnqueuedTime, DecodeBody(raw.Body)), ct);
            return IngestOutcome.Rejected;
        }

        var e = parsed.Event!;
        var isNew = await history.TryAddAsync(e, raw.SequenceNumber, ct);

        var events = await history.GetHistoryAsync(e.MatchId, ct);
        await statuses.SaveAsync(MatchStatus.FromHistory(e.MatchId, events), ct);

        return isNew ? IngestOutcome.Processed : IngestOutcome.Duplicate;
    }

    private async Task CountAsync(string partitionId, IngestOutcome outcome, CancellationToken ct)
    {
        var s = await stats.GetAsync(partitionId, ct) ?? new PartitionStats { PartitionId = partitionId };
        switch (outcome)
        {
            case IngestOutcome.Processed: s.Processed++; break;
            case IngestOutcome.Duplicate: s.Duplicates++; break;
            case IngestOutcome.Rejected: s.Rejected++; break;
        }
        await stats.SaveAsync(s, ct);
    }

    private ParseResult SafeParse(IncomingEvent raw)
    {
        try
        {
            return EventParser.Parse(raw.Body.Span);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Parser failed on p{Partition}#{Seq}", raw.PartitionId, raw.SequenceNumber);
            return ParseResult.Reject(RejectReason.UnparseablePayload, $"parser error: {ex.GetType().Name}");
        }
    }

    private static string DecodeBody(ReadOnlyMemory<byte> body)
    {
        try { return Encoding.UTF8.GetString(body.Span); }
        catch { return Convert.ToBase64String(body.Span); }
    }
}
