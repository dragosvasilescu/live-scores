using System.Collections.Concurrent;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Options;
using Scores.Domain.Ingestion;
using Scores.Domain.Storage;

namespace Scores.Consumer;

public sealed class EventHubsOptions
{
    public string? ConnectionString { get; set; }

    public string? FullyQualifiedNamespace { get; set; }

    public string EventHubName { get; set; } = "match-events";

    public string ConsumerGroup { get; set; } = "scoreboard";

    public string CheckpointContainer { get; set; } = "checkpoints";

    public int CheckpointEveryEvents { get; set; } = 50;
}

public sealed class ScoreboardProcessor(
    IOptions<EventHubsOptions> options,
    BlobServiceClient blobs,
    StorageBootstrap storage,
    EventIngestor ingestor,
    ILogger<ScoreboardProcessor> logger) : BackgroundService
{
    private readonly EventHubsOptions _options = options.Value;

    private readonly ConcurrentDictionary<string, (int Pending, ProcessEventArgs Last)> _progress = new();
    private EventProcessorClient? _processor;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await storage.EnsureCreatedAsync(stoppingToken);
        var checkpoints = blobs.GetBlobContainerClient(_options.CheckpointContainer);
        await checkpoints.CreateIfNotExistsAsync(cancellationToken: stoppingToken);

        _processor = string.IsNullOrWhiteSpace(_options.ConnectionString)
            ? new EventProcessorClient(checkpoints, _options.ConsumerGroup,
                _options.FullyQualifiedNamespace, _options.EventHubName, new DefaultAzureCredential())
            : new EventProcessorClient(checkpoints, _options.ConsumerGroup,
                _options.ConnectionString, _options.EventHubName);

        _processor.ProcessEventAsync += OnEventAsync;
        _processor.ProcessErrorAsync += OnErrorAsync;
        _processor.PartitionClosingAsync += OnPartitionClosingAsync;

        logger.LogInformation("Starting processor on {Hub}/{Group}", _options.EventHubName, _options.ConsumerGroup);
        await _processor.StartProcessingAsync(stoppingToken);

        try { await Task.Delay(Timeout.Infinite, stoppingToken); }
        catch (OperationCanceledException) { }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_processor is not null)
            await _processor.StopProcessingAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    private async Task OnEventAsync(ProcessEventArgs args)
    {
        if (!args.HasEvent)
            return;

        var partitionId = args.Partition.PartitionId;
        var data = args.Data;
        var incoming = new IncomingEvent(data.EventBody.ToMemory(), partitionId, data.SequenceNumber, data.EnqueuedTime);

        if (!await WithRetryAsync(() => ingestor.ProcessAsync(incoming, args.CancellationToken), args.CancellationToken))
            return;

        var pending = _progress.TryGetValue(partitionId, out var p) ? p.Pending + 1 : 1;
        _progress[partitionId] = (pending, args);

        if (pending >= _options.CheckpointEveryEvents)
            await CheckpointAsync(partitionId, args.CancellationToken);
    }

    private async Task CheckpointAsync(string partitionId, CancellationToken ct)
    {
        if (!_progress.TryRemove(partitionId, out var p))
            return;
        await p.Last.UpdateCheckpointAsync(ct);
    }

    private async Task<bool> WithRetryAsync(Func<Task> work, CancellationToken ct)
    {
        var delay = TimeSpan.FromMilliseconds(200);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await work();
                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Processing failed; retrying in {Delay}", delay);
                try { await Task.Delay(delay, ct); }
                catch (OperationCanceledException) { return false; }
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 10_000));
            }
        }
        return false;
    }

    private async Task OnPartitionClosingAsync(PartitionClosingEventArgs args)
    {
        if (args.Reason == ProcessingStoppedReason.Shutdown)
        {
            try { await CheckpointAsync(args.PartitionId, CancellationToken.None); }
            catch (Exception ex) { logger.LogWarning(ex, "Final checkpoint failed for partition {Partition}", args.PartitionId); }
        }
        _progress.TryRemove(args.PartitionId, out _);
        logger.LogInformation("Partition {Partition} closed ({Reason})", args.PartitionId, args.Reason);
    }

    private Task OnErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(args.Exception, "Event Hubs error in {Operation} (partition {Partition})",
            args.Operation, args.PartitionId ?? "-");
        return Task.CompletedTask;
    }
}
