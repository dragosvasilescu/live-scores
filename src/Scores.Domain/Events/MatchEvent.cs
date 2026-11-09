namespace Scores.Domain.Events;

public enum MatchEventType
{
    Goal,
    Card,
    Sub,
}

public enum CardColour
{
    Yellow,
    Red,
}

public sealed record MatchEvent(
    string EventId,
    string MatchId,
    MatchEventType Type,
    string Team,
    string Player,
    int Minute,
    DateTimeOffset OccurredAt,
    CardColour? Card = null,
    string? PlayerOff = null);
