using Microsoft.Extensions.Logging.Abstractions;
using Scores.Domain.Events;
using Scores.Domain.Ingestion;
using Scores.Tests.Support;

namespace Scores.Tests;

public class EventIngestorTests
{
    private readonly InMemoryEventStore _history = new();
    private readonly InMemoryStatusStore _statuses = new();
    private readonly InMemoryRejectedSink _rejects = new();
    private readonly InMemoryStatsStore _stats = new();

    private EventIngestor NewIngestor() =>
        new(_history, _statuses, _rejects, _stats, NullLogger<EventIngestor>.Instance);

    [Fact]
    public async Task Duplicate_goal_does_not_turn_2_0_into_3_0()
    {
        var ingestor = NewIngestor();
        var second = Events.Goal(2, "ARS", "ARS-11").ToJsonString();
        await ingestor.ProcessAsync(Events.Incoming(Events.Goal(1, "ARS").ToJsonString(), 0), default);
        await ingestor.ProcessAsync(Events.Incoming(second, 1), default);

        var outcome = await ingestor.ProcessAsync(Events.Incoming(second, 2), default);

        Assert.Equal(IngestOutcome.Duplicate, outcome);
        Assert.Equal(2, _statuses.Rows[Events.MatchId].Score["ARS"]);
    }

    [Fact]
    public async Task Malformed_events_are_routed_aside_and_do_not_block_good_ones()
    {
        var ingestor = NewIngestor();

        await ingestor.ProcessAsync(Events.Incoming(Events.Goal(1, "ARS").ToJsonString(), 0), default);
        var bad1 = await ingestor.ProcessAsync(Events.Incoming("}{ not valid json", 1), default);
        var bad2 = await ingestor.ProcessAsync(Events.Incoming(Events.Without(Events.Goal(2, "CHE"), "player").ToJsonString(), 2), default);
        await ingestor.ProcessAsync(Events.Incoming(Events.Goal(3, "CHE", "CHE-10").ToJsonString(), 3), default);

        Assert.Equal(IngestOutcome.Rejected, bad1);
        Assert.Equal(IngestOutcome.Rejected, bad2);
        Assert.Equal("ARS 1 - CHE 1", _statuses.Rows[Events.MatchId].Scoreline);
        Assert.Equal(nameof(RejectReason.UnparseablePayload), _rejects.Items["0/1"].Reason);
        Assert.Equal("}{ not valid json", _rejects.Items["0/1"].RawBody);
        Assert.Equal(nameof(RejectReason.GoalWithoutScorer), _rejects.Items["0/2"].Reason);
    }
}
