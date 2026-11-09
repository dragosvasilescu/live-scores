using Scores.Domain.Events;

namespace Scores.Domain.State;

public sealed record MatchStatus(
    string MatchId,
    Dictionary<string, int> Score,
    int Minute,
    int EventCount,
    DateTimeOffset? LastEventAt)
{
    public string Scoreline => string.Join(" - ", Score.Select(kv => $"{kv.Key} {kv.Value}"));

    public static MatchStatus FromHistory(string matchId, IReadOnlyList<MatchEvent> history)
    {
        var score = new Dictionary<string, int>();
        foreach (var e in history)
        {
            score.TryAdd(e.Team, 0);
            if (e.Type == MatchEventType.Goal)
                score[e.Team]++;
        }

        return new MatchStatus(
            matchId,
            score,
            Minute: history.Count == 0 ? 0 : history.Max(e => e.Minute),
            EventCount: history.Count,
            LastEventAt: history.Count == 0 ? null : history.Max(e => e.OccurredAt));
    }
}
