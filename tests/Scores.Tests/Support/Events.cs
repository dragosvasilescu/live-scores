using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Scores.Domain.Events;
using Scores.Domain.Ingestion;

namespace Scores.Tests.Support;

public static class Events
{
    public const string MatchId = "m-1";

    public static JsonObject Goal(long seq, string team = "ARS", string player = "ARS-09", string? eventId = null, string matchId = MatchId) =>
        Base("GOAL", seq, team, player, eventId, matchId);

    public static JsonObject Card(long seq, string colour = "YELLOW", string team = "CHE", string player = "CHE-04") =>
        With(Base("CARD", seq, team, player, null, MatchId), "card", colour);

    public static JsonObject Sub(long seq, string team = "CHE", string on = "CHE-12", string off = "CHE-07") =>
        With(Base("SUB", seq, team, on, null, MatchId), "playerOff", off);

    public static MatchEvent Parsed(JsonObject json) =>
        EventParser.Parse(json.ToJsonString()).Event ?? throw new InvalidOperationException("test event is invalid");

    public static IncomingEvent Incoming(string body, long sequenceNumber, string partition = "0") =>
        new(Encoding.UTF8.GetBytes(body), partition, sequenceNumber, new DateTimeOffset(2026, 11, 7, 15, 0, 0, TimeSpan.Zero));

    public static JsonObject With(JsonObject o, string key, JsonNode? value)
    {
        o[key] = value;
        return o;
    }

    public static JsonObject Without(JsonObject o, string key)
    {
        o.Remove(key);
        return o;
    }

    private static JsonObject Base(string type, long seq, string team, string player, string? eventId, string matchId) => new()
    {
        ["eventId"] = eventId ?? Guid.NewGuid().ToString(),
        ["type"] = type,
        ["matchId"] = matchId,
        ["team"] = team,
        ["player"] = player,
        ["minute"] = (int)Math.Min(90, seq),
        ["occurredAt"] = "2026-11-07T15:00:00Z",
        ["seq"] = seq,
    };
}
