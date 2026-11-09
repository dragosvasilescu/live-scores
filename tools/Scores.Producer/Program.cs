using System.Text;
using System.Text.Json;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Scores.Domain.Events;

const string EmulatorConnection =
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
var connection = Environment.GetEnvironmentVariable("EVENTHUBS_CONNECTION") ?? EmulatorConnection;
var hubName = Environment.GetEnvironmentVariable("EVENTHUBS_NAME") ?? "match-events";

if (args.Length == 0)
    return Usage();

switch (args[0])
{
    case "send" when args.Length >= 2:
        await SendAsync(args[1], int.TryParse(Option("--delay-ms"), out var d) ? d : 0);
        return 0;
    case "replay" when Option("--match") is { } matchId:
        await ReplayAsync(matchId);
        return 0;
    default:
        return Usage();
}

string? Option(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

static int Usage()
{
    Console.Error.WriteLine("usage: send <file.ndjson> [--delay-ms N] | replay --match <matchId>");
    return 1;
}

async Task SendAsync(string path, int delayMs)
{
    await using var producer = new EventHubProducerClient(connection, hubName);
    int sent = 0, unkeyed = 0;

    var batches = new Dictionary<string, EventDataBatch>();

    async Task FlushAsync(string key)
    {
        if (batches.Remove(key, out var b))
        {
            using (b) await producer.SendAsync(b);
        }
    }

    foreach (var line in File.ReadLines(path))
    {
        if (string.IsNullOrWhiteSpace(line))
            continue;

        var key = TryGetMatchId(line);
        if (key is null)
            unkeyed++;
        var batchKey = key ?? "";

        if (delayMs > 0)
        {
            await producer.SendAsync([new EventData(Encoding.UTF8.GetBytes(line))],
                new SendEventOptions { PartitionKey = key });
            sent++;
            await Task.Delay(delayMs);
            continue;
        }

        if (!batches.TryGetValue(batchKey, out var batch))
            batches[batchKey] = batch = await producer.CreateBatchAsync(new CreateBatchOptions { PartitionKey = key });

        var data = new EventData(Encoding.UTF8.GetBytes(line));
        if (!batch.TryAdd(data))
        {
            await FlushAsync(batchKey);
            batches[batchKey] = batch = await producer.CreateBatchAsync(new CreateBatchOptions { PartitionKey = key });
            if (!batch.TryAdd(data))
                throw new InvalidOperationException("single event larger than a batch");
        }
        sent++;
    }

    foreach (var key in batches.Keys.ToList())
        await FlushAsync(key);

    Console.WriteLine($"Sent {sent} events to '{hubName}' ({unkeyed} without a matchId, sent unkeyed).");
}

async Task ReplayAsync(string matchId)
{
    await using var consumer = new EventHubConsumerClient("highlights", connection, hubName);
    var options = new ReadEventOptions { MaximumWaitTime = TimeSpan.FromSeconds(3) };
    var events = new List<(string EventId, string Json)>();
    int read = 0, invalid = 0;

    await foreach (var pe in consumer.ReadEventsAsync(startReadingAtEarliestEvent: true, options))
    {
        if (pe.Data is null)
            break;
        var body = pe.Data.EventBody.ToString();
        if (TryGetMatchId(body) != matchId)
            continue;
        read++;
        var parsed = EventParser.Parse(body);
        if (parsed.IsValid) events.Add((parsed.Event!.EventId, body));
        else invalid++;
    }

    var ordered = events.GroupBy(e => e.EventId).Select(g => g.First().Json).ToList();

    foreach (var json in ordered)
        Console.WriteLine(json);
    Console.Error.WriteLine(
        $"Replayed {ordered.Count} events for match {matchId} ({read} read, {invalid} malformed, {events.Count - ordered.Count} duplicates).");
}

static string? TryGetMatchId(string line)
{
    try
    {
        using var doc = JsonDocument.Parse(line);
        return doc.RootElement.ValueKind == JsonValueKind.Object
               && doc.RootElement.TryGetProperty("matchId", out var m)
               && m.ValueKind == JsonValueKind.String
            ? m.GetString()
            : null;
    }
    catch (JsonException)
    {
        return null;
    }
}
