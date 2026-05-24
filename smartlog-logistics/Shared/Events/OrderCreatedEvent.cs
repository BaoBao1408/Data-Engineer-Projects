namespace Shared.Events;

public class OrderCreatedEvent
{
    public Guid OrderId { get; set; }
    public string OrderCode { get; set; } = string.Empty;
    public string CustomerName { get; set; } = string.Empty;
    public string OriginAddress { get; set; } = string.Empty;
    public string DestinationAddress { get; set; } = string.Empty;
    public decimal TotalWeight { get; set; }
    public DateTime CreatedAt { get; set; }
}
