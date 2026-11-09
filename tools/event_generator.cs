// event_generator.cs — Live Football Scores challenge (C# single-file)
//
// Emits a deliberately hostile stream of football match events as NDJSON
// (one JSON object per line): mostly valid, in-order events, plus duplicates
// (same eventId re-sent) and malformed events (missing fields, bad types,
// a goal with no scorer, and the occasional unparseable line).
//
// It knows nothing about transport on purpose — getting this data into the
// ingestion mechanism you chose is part of the exercise.
//
// The RNG is seeded, so the event mix and ordering are reproducible run to run.
// (eventIds are fresh each run, so duplicates within a run still match, but two
// separate runs won't share ids — same behaviour as the Python generator.)
//
// Requires .NET 10+ (file-based app — no .csproj needed). Run:
//   dotnet run event_generator.cs -- --count 1500 --seed 42            // to stdout
//   dotnet run event_generator.cs -- --count 1500 --out events.ndjson
// Or paste the body into a console project's Program.cs and `dotnet run`.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

#nullable enable

// ---- args ----
int count = 1500;
int seed = 42;
string outPath = "-";
for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--count": count = int.Parse(args[++i]); break;
        case "--seed":  seed  = int.Parse(args[++i]); break;
        case "--out":   outPath = args[++i]; break;
    }
}

var rng = new Random(seed);
string[] teams = { "ARS", "CHE", "LIV", "MCI", "TOT", "MUN", "NEW", "AVL" };
const double MixMalformed = 0.10;   // missing fields / bad types / no scorer / unparseable
const double MixDuplicate = 0.10;   // same eventId re-sent; must not double-count

// ---- state ----
var matches = Enumerable.Range(0, 6).Select(_ => NewMatch()).ToList();  // a Saturday 3pm slate
var seqMap = new Dictionary<string, int>();
var recent = new List<Dictionary<string, object?>>();

// ---- build the stream ----
var stream = new List<object>();
var now = DateTime.UtcNow;
for (int i = 0; i < count; i++)
{
    now = now.AddMilliseconds(rng.Next(5, 61));
    double r = rng.NextDouble();
    if (r < MixMalformed)
    {
        stream.Add(Corrupt(MakeValid(now)));
    }
    else if (r < MixMalformed + MixDuplicate && recent.Count > 0)
    {
        stream.Add(new Dictionary<string, object?>(recent[rng.Next(recent.Count)])); // exact duplicate
    }
    else
    {
        var ev = MakeValid(now);
        stream.Add(ev);
        recent.Add(ev);
        if (recent.Count > 150) recent.RemoveRange(0, recent.Count - 150);
    }
}

// ---- write NDJSON ----
TextWriter writer = outPath == "-" ? Console.Out : new StreamWriter(outPath);
try
{
    foreach (var item in stream)
    {
        string line = item is string s ? s : ToJson((Dictionary<string, object?>)item);
        writer.WriteLine(line);
    }
    writer.Flush();
}
catch (IOException)
{
    // downstream closed the pipe (e.g. `| head`) — fine
}
finally
{
    if (outPath != "-")
    {
        writer.Dispose();
        Console.Error.WriteLine($"Wrote {stream.Count} events to {outPath}");
    }
}

// ---- helpers ----
string Iso(DateTime dt) => dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ");

Dictionary<string, object?> NewMatch()
{
    int a = rng.Next(teams.Length);
    int b;
    do { b = rng.Next(teams.Length); } while (b == a);
    return new Dictionary<string, object?>
    {
        ["matchId"] = Guid.NewGuid().ToString(),
        ["home"] = teams[a],
        ["away"] = teams[b],
    };
}

string WeightedKind()
{
    double r = rng.NextDouble();
    if (r < 0.45) return "GOAL";
    if (r < 0.75) return "CARD";
    return "SUB";
}

// A well-formed, in-order match event. matchId is the natural key per match;
// if you pick a partitioned ingestion mechanism it's the obvious partition key,
// but that's your call.
Dictionary<string, object?> MakeValid(DateTime ts)
{
    var match = matches[rng.Next(matches.Count)];
    string mid = (string)match["matchId"]!;
    seqMap[mid] = seqMap.GetValueOrDefault(mid) + 1;
    string kind = WeightedKind();
    string team = (string)(rng.Next(2) == 0 ? match["home"]! : match["away"]!);

    var ev = new Dictionary<string, object?>
    {
        ["eventId"] = Guid.NewGuid().ToString(),
        ["type"] = kind,
        ["matchId"] = mid,
        ["team"] = team,
        ["player"] = $"{team}-{rng.Next(1, 24):D2}",
        ["minute"] = Math.Min(90, seqMap[mid]),
        ["occurredAt"] = Iso(ts),
        ["seq"] = seqMap[mid],
    };
    if (kind == "CARD") ev["card"] = rng.Next(2) == 0 ? "YELLOW" : "RED";
    else if (kind == "SUB") ev["playerOff"] = $"{team}-{rng.Next(1, 24):D2}";
    return ev;
}

// Turn a good event into a malformed one — to be rejected.
object Corrupt(Dictionary<string, object?> ev)
{
    string[] modes = { "drop_field", "bad_type", "garbage", "goal_no_scorer" };
    string mode = modes[rng.Next(modes.Length)];
    if (mode == "garbage") return "}{ not valid json";   // an unparseable line

    var bad = new Dictionary<string, object?>(ev);
    if (mode == "drop_field")
    {
        string[] fields = { "matchId", "type", "eventId" };
        bad.Remove(fields[rng.Next(fields.Length)]);
    }
    else if (mode == "bad_type")
    {
        bad["minute"] = "ninety";
    }
    else // goal_no_scorer
    {
        bad["type"] = "GOAL";
        bad.Remove("player");
        bad.Remove("card");
        bad.Remove("playerOff");
    }
    return bad;
}

// Serialize a flat event dict to compact JSON without reflection — works under
// trimming / native-AOT / file-based apps where reflection serialization is off.
static string ToJson(Dictionary<string, object?> dict)
{
    using var ms = new MemoryStream();
    using (var w = new Utf8JsonWriter(ms))
    {
        w.WriteStartObject();
        foreach (var kv in dict)
        {
            w.WritePropertyName(kv.Key);
            switch (kv.Value)
            {
                case null: w.WriteNullValue(); break;
                case string s: w.WriteStringValue(s); break;
                case int n: w.WriteNumberValue(n); break;
                case long l: w.WriteNumberValue(l); break;
                case double d: w.WriteNumberValue(d); break;
                case bool b: w.WriteBooleanValue(b); break;
                default: w.WriteStringValue(kv.Value.ToString()); break;
            }
        }
        w.WriteEndObject();
    }
    return Encoding.UTF8.GetString(ms.ToArray());
}
