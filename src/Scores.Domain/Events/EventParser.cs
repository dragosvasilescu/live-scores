using System.Globalization;
using System.Text.Json;

namespace Scores.Domain.Events;

public static class EventParser
{
    public const int MaxMinute = 130;

    public static ParseResult Parse(ReadOnlySpan<byte> utf8Json)
    {
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(utf8Json.ToArray());
        }
        catch (JsonException)
        {
            return ParseResult.Reject(RejectReason.UnparseablePayload, "payload is not valid JSON");
        }

        using (doc)
        {
            return Validate(doc.RootElement);
        }
    }

    public static ParseResult Parse(string json) => Parse(System.Text.Encoding.UTF8.GetBytes(json));

    private static ParseResult Validate(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object)
            return ParseResult.Reject(RejectReason.UnparseablePayload, "payload is not a JSON object");

        if (!TryString(root, "eventId", out var eventId))
            return Missing("eventId");
        if (!TryString(root, "matchId", out var matchId))
            return Missing("matchId");
        if (!IsValidKey(eventId))
            return ParseResult.Reject(RejectReason.InvalidField, "eventId has characters not allowed in a key");
        if (!IsValidKey(matchId))
            return ParseResult.Reject(RejectReason.InvalidField, "matchId has characters not allowed in a key");
        if (!TryString(root, "type", out var typeText))
            return Missing("type");
        if (!TryParseType(typeText, out var type))
            return ParseResult.Reject(RejectReason.UnknownType, $"unknown event type '{typeText}'");

        if (!TryString(root, "team", out var team))
            return Missing("team");

        if (!TryString(root, "player", out var player))
            return type == MatchEventType.Goal
                ? ParseResult.Reject(RejectReason.GoalWithoutScorer, "GOAL has no scorer (player)")
                : Missing("player");

        if (!root.TryGetProperty("minute", out var minuteEl))
            return Missing("minute");
        if (minuteEl.ValueKind != JsonValueKind.Number || !minuteEl.TryGetInt32(out var minute))
            return ParseResult.Reject(RejectReason.InvalidField, "minute must be an integer");
        if (minute is < 0 or > MaxMinute)
            return ParseResult.Reject(RejectReason.InvalidField, $"minute {minute} is out of range 0..{MaxMinute}");

        if (!TryString(root, "occurredAt", out var occurredText))
            return Missing("occurredAt");
        if (!DateTimeOffset.TryParse(occurredText, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var occurredAt))
            return ParseResult.Reject(RejectReason.InvalidField, "occurredAt is not a valid timestamp");

        CardColour? card = null;
        string? playerOff = null;
        switch (type)
        {
            case MatchEventType.Card:
                if (!TryString(root, "card", out var cardText))
                    return Missing("card");
                card = cardText.ToUpperInvariant() switch
                {
                    "YELLOW" => CardColour.Yellow,
                    "RED" => CardColour.Red,
                    _ => null,
                };
                if (card is null)
                    return ParseResult.Reject(RejectReason.InvalidField, $"unknown card colour '{cardText}'");
                break;
            case MatchEventType.Sub:
                if (!TryString(root, "playerOff", out playerOff))
                    return Missing("playerOff");
                break;
        }

        return ParseResult.Ok(new MatchEvent(eventId, matchId, type, team, player, minute, occurredAt, card, playerOff));
    }

    private static ParseResult Missing(string field) =>
        ParseResult.Reject(RejectReason.MissingField, $"required field '{field}' is missing or empty");

    private static bool TryString(JsonElement root, string name, out string value)
    {
        value = "";
        if (!root.TryGetProperty(name, out var el) || el.ValueKind != JsonValueKind.String)
            return false;
        value = el.GetString()!.Trim();
        return value.Length > 0;
    }

    private static bool IsValidKey(string s) =>
        s.Length <= 256 && !s.Any(c => c is '/' or '\\' or '#' or '?' || char.IsControl(c));

    private static bool TryParseType(string text, out MatchEventType type)
    {
        switch (text.ToUpperInvariant())
        {
            case "GOAL": type = MatchEventType.Goal; return true;
            case "CARD": type = MatchEventType.Card; return true;
            case "SUB": type = MatchEventType.Sub; return true;
            default: type = default; return false;
        }
    }
}

public enum RejectReason
{
    UnparseablePayload,
    MissingField,
    InvalidField,
    UnknownType,
    GoalWithoutScorer,
}

public sealed record ParseResult(MatchEvent? Event, RejectReason? Reason, string? Detail)
{
    public bool IsValid => Event is not null;

    public static ParseResult Ok(MatchEvent e) => new(e, null, null);

    public static ParseResult Reject(RejectReason reason, string detail) => new(null, reason, detail);
}
