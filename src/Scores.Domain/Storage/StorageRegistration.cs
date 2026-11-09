using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Scores.Domain.Storage;

public static class StorageRegistration
{
    public static IServiceCollection AddScoresStorage(this IServiceCollection services, IConfiguration config)
    {
        var connectionString = config["Storage:ConnectionString"];
        var accountName = config["Storage:AccountName"];

        TableServiceClient tables;
        BlobServiceClient blobs;
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            tables = new TableServiceClient(connectionString);
            blobs = new BlobServiceClient(connectionString);
        }
        else if (!string.IsNullOrWhiteSpace(accountName))
        {
            var credential = new DefaultAzureCredential();
            tables = new TableServiceClient(new Uri($"https://{accountName}.table.core.windows.net"), credential);
            blobs = new BlobServiceClient(new Uri($"https://{accountName}.blob.core.windows.net"), credential);
        }
        else
        {
            throw new InvalidOperationException("Set Storage:ConnectionString or Storage:AccountName");
        }

        services.AddSingleton(blobs);
        services.AddSingleton<IMatchEventStore>(new TableMatchEventStore(tables.GetTableClient(TableMatchEventStore.TableName)));
        services.AddSingleton<IMatchStatusStore>(new TableMatchStatusStore(tables.GetTableClient(TableMatchStatusStore.TableName)));
        services.AddSingleton<IIngestionStatsStore>(new TableIngestionStatsStore(tables.GetTableClient(TableIngestionStatsStore.TableName)));
        services.AddSingleton<IRejectedEventSink>(new BlobRejectedEventSink(blobs.GetBlobContainerClient(BlobRejectedEventSink.ContainerName)));
        services.AddSingleton(new StorageBootstrap(tables, blobs));
        return services;
    }
}

public sealed class StorageBootstrap(TableServiceClient tables, BlobServiceClient blobs)
{
    public async Task EnsureCreatedAsync(CancellationToken ct)
    {
        await tables.CreateTableIfNotExistsAsync(TableMatchEventStore.TableName, ct);
        await tables.CreateTableIfNotExistsAsync(TableMatchStatusStore.TableName, ct);
        await tables.CreateTableIfNotExistsAsync(TableIngestionStatsStore.TableName, ct);
        await blobs.GetBlobContainerClient(BlobRejectedEventSink.ContainerName).CreateIfNotExistsAsync(cancellationToken: ct);
    }
}
