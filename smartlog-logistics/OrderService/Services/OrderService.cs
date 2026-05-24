using OrderService.DTOs;
using OrderService.Infrastructure.Kafka;
using OrderService.Models;
using OrderService.Repositories;
using Shared.Contracts;
using Shared.Events;

namespace OrderService.Services;

public class OrderService : IOrderService
{
    private readonly IOrderRepository _repo;
    private readonly IKafkaProducer _kafka;
    private readonly ILogger<OrderService> _logger;

    public OrderService(IOrderRepository repo, IKafkaProducer kafka, ILogger<OrderService> logger)
    {
        _repo = repo;
        _kafka = kafka;
        _logger = logger;
    }

    public async Task<(IEnumerable<OrderResponseDto> Items, int Total)> GetAllAsync(int page, int pageSize)
    {
        var orders = await _repo.GetAllAsync(page, pageSize);
        var total = await _repo.CountAsync();
        return (orders.Select(MapToDto), total);
    }

    public async Task<OrderResponseDto?> GetByIdAsync(Guid id)
    {
        var order = await _repo.GetByIdAsync(id);
        return order == null ? null : MapToDto(order);
    }

    public async Task<OrderResponseDto> CreateAsync(CreateOrderDto dto)
    {
        var order = new Order
        {
            OrderCode = GenerateOrderCode(),
            CustomerName = dto.CustomerName,
            CustomerPhone = dto.CustomerPhone,
            OriginAddress = dto.OriginAddress,
            DestinationAddress = dto.DestinationAddress,
            Items = dto.Items.Select(i => new OrderItem
            {
                ProductName = i.ProductName,
                Quantity = i.Quantity,
                Weight = i.Weight
            }).ToList()
        };

        order.TotalWeight = order.Items.Sum(i => i.Weight * i.Quantity);
        order.ShippingFee = CalculateShippingFee(order.TotalWeight);

        var created = await _repo.CreateAsync(order);

        // Publish Kafka event
        var evt = new OrderCreatedEvent
        {
            OrderId = created.Id,
            OrderCode = created.OrderCode,
            CustomerName = created.CustomerName,
            OriginAddress = created.OriginAddress,
            DestinationAddress = created.DestinationAddress,
            TotalWeight = created.TotalWeight,
            CreatedAt = created.CreatedAt
        };
        await _kafka.PublishAsync(KafkaTopics.OrderCreated, created.Id.ToString(), evt);

        _logger.LogInformation("Order {OrderCode} created and event published", created.OrderCode);
        return MapToDto(created);
    }

    public async Task<OrderResponseDto?> UpdateStatusAsync(Guid id, string status)
    {
        var order = await _repo.GetByIdAsync(id);
        if (order == null) return null;

        if (!Enum.TryParse<OrderStatus>(status, true, out var newStatus))
            throw new ArgumentException($"Invalid status: {status}");

        order.Status = newStatus;
        var updated = await _repo.UpdateAsync(order);

        var evt = new OrderStatusUpdatedEvent
        {
            OrderId = updated.Id,
            OrderCode = updated.OrderCode,
            Status = updated.Status.ToString(),
            Location = "Ho Chi Minh City Hub",
            Note = $"Status updated to {newStatus}",
            UpdatedAt = DateTime.UtcNow
        };
        await _kafka.PublishAsync(KafkaTopics.OrderStatusUpdated, updated.Id.ToString(), evt);

        return MapToDto(updated);
    }

    public async Task<bool> DeleteAsync(Guid id) => await _repo.DeleteAsync(id);

    private static OrderResponseDto MapToDto(Order o) => new()
    {
        Id = o.Id,
        OrderCode = o.OrderCode,
        CustomerName = o.CustomerName,
        CustomerPhone = o.CustomerPhone,
        OriginAddress = o.OriginAddress,
        DestinationAddress = o.DestinationAddress,
        TotalWeight = o.TotalWeight,
        ShippingFee = o.ShippingFee,
        Status = o.Status.ToString(),
        CreatedAt = o.CreatedAt,
        Items = o.Items.Select(i => new OrderItemDto
        {
            Id = i.Id,
            ProductName = i.ProductName,
            Quantity = i.Quantity,
            Weight = i.Weight
        }).ToList()
    };

    private static string GenerateOrderCode()
        => $"SML-{DateTime.UtcNow:yyyyMMdd}-{Guid.NewGuid().ToString("N")[..6].ToUpper()}";

    private static decimal CalculateShippingFee(decimal weight)
        => weight <= 1 ? 25000 : 25000 + (weight - 1) * 5000;
}
