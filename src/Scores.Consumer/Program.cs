using Scores.Consumer;
using Scores.Domain.Ingestion;
using Scores.Domain.Storage;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<EventHubsOptions>(builder.Configuration.GetSection("EventHubs"));
builder.Services.AddScoresStorage(builder.Configuration);
builder.Services.AddSingleton<EventIngestor>();
builder.Services.AddHostedService<ScoreboardProcessor>();

builder.Build().Run();
