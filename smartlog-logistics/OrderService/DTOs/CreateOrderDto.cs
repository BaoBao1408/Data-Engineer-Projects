using System.ComponentModel.DataAnnotations;

namespace OrderService.DTOs;

public class CreateOrderDto
{
    [Required] public string CustomerName { get; set; } = string.Empty;
    [Required] public string CustomerPhone { get; set; } = string.Empty;
    [Required] public string OriginAddress { get; set; } = string.Empty;
    [Required] public string DestinationAddress { get; set; } = string.Empty;
    [Required][MinLength(1)] public List<CreateOrderItemDto> Items { get; set; } = new();
}

public class CreateOrderItemDto
{
    [Required] public string ProductName { get; set; } = string.Empty;
    [Range(1, int.MaxValue)] public int Quantity { get; set; }
    [Range(0.01, double.MaxValue)] public decimal Weight { get; set; }
}
