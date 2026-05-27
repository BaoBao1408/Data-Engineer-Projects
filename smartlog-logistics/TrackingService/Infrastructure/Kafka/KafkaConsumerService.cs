using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Shared.Contracts;
using Shared.Events;
using System.Text.Json;
using TrackingService.Data;
using TrackingService.Models;

namespace TrackingService.Infrastructure.Kafka;

public class KafkaConsumerService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _config;
    private readonly ILogger<KafkaConsumerService> _logger;

    // Fix: thêm PropertyNameCaseInsensitive phòng trường hợp JSON case mismatch
    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public KafkaConsumerService(IServiceScopeFactory scopeFactory, IConfiguration config,
        ILogger<KafkaConsumerService> logger)
    {
        _scopeFactory = scopeFactory;
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var bootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:29092";
        _logger.LogInformation("🔧 Kafka BootstrapServers = {Servers}", bootstrapServers);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "tracking-service-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(new[] { KafkaTopics.OrderCreated, KafkaTopics.OrderStatusUpdated });

        _logger.LogInformation("🚀 Kafka consumer started. Listening to topics: {Topics}",
            string.Join(", ", KafkaTopics.OrderCreated, KafkaTopics.OrderStatusUpdated));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result == null) continue;

                _logger.LogInformation("📨 Received [{Topic}] partition={P} offset={O} key={Key}",
                    result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key);
                _logger.LogDebug("📦 Raw JSON: {Json}", result.Message.Value);

                using var scope = _scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<TrackingDbContext>();

                if (result.Topic == KafkaTopics.OrderCreated)
                    await HandleOrderCreated(db, result.Message.Value);
                else if (result.Topic == KafkaTopics.OrderStatusUpdated)
                    await HandleOrderStatusUpdated(db, result.Message.Value);

                consumer.Commit(result);
                _logger.LogInformation("✔️ Committed offset {O} partition {P}", result.Offset.Value, result.Partition.Value);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "❌ Consume error: {Reason}", ex.Error.Reason);
                await Task.Delay(1000, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Unexpected error: {Msg}", ex.Message);
                await Task.Delay(2000, stoppingToken);
            }
        }

        consumer.Close();
    }

    private async Task HandleOrderCreated(TrackingDbContext db, string json)
    {
        var evt = JsonSerializer.Deserialize<OrderCreatedEvent>(json, _jsonOptions);

        if (evt == null || evt.OrderId == Guid.Empty)
        {
            _logger.LogError("❌ Deserialize failed or empty OrderId. Raw: {Json}", json);
            return;
        }

        _logger.LogInformation("🔍 Processing order.created: {OrderCode} ({OrderId})", evt.OrderCode, evt.OrderId);

        var existing = await db.TrackingRecords.FirstOrDefaultAsync(t => t.OrderId == evt.OrderId);
        if (existing != null)
        {
            _logger.LogWarning("⚠️ TrackingRecord already exists for {OrderId}", evt.OrderId);
            return;
        }

        var record = new TrackingRecord
        {
            OrderId = evt.OrderId,
            OrderCode = evt.OrderCode,
            CustomerName = evt.CustomerName,
            OriginAddress = evt.OriginAddress,
            DestinationAddress = evt.DestinationAddress,
            CurrentStatus = "Pending",
            Events = new List<TrackingEvent>
            {
                new() { Status = "Pending", Location = "Origin Hub", Note = "Order received and being processed" }
            }
        };

        db.TrackingRecords.Add(record);
        await db.SaveChangesAsync();
        _logger.LogInformation("✅ TrackingRecord created for {OrderCode}", evt.OrderCode);
    }

    private async Task HandleOrderStatusUpdated(TrackingDbContext db, string json)
    {
        var evt = JsonSerializer.Deserialize<OrderStatusUpdatedEvent>(json, _jsonOptions);

        if (evt == null || evt.OrderId == Guid.Empty)
        {
            _logger.LogError("❌ Deserialize failed. Raw: {Json}", json);
            return;
        }

        var record = await db.TrackingRecords
            .Include(t => t.Events)
            .FirstOrDefaultAsync(t => t.OrderId == evt.OrderId);

        if (record == null)
        {
            _logger.LogWarning("⚠️ No tracking record found for {OrderId}", evt.OrderId);
            return;
        }

        record.CurrentStatus = evt.Status;
        record.UpdatedAt = DateTime.UtcNow;
        record.Events.Add(new TrackingEvent
        {
            Status = evt.Status,
            Location = evt.Location,
            Note = evt.Note,
            OccurredAt = evt.UpdatedAt
        });

        await db.SaveChangesAsync();
        _logger.LogInformation("✅ Tracking updated for {OrderCode}: {Status}", evt.OrderCode, evt.Status);
    }
}