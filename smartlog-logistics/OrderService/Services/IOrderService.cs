using OrderService.DTOs;

namespace OrderService.Services;

public interface IOrderService
{
    Task<(IEnumerable<OrderResponseDto> Items, int Total)> GetAllAsync(int page, int pageSize);
    Task<OrderResponseDto?> GetByIdAsync(Guid id);
    Task<OrderResponseDto> CreateAsync(CreateOrderDto dto);
    Task<OrderResponseDto?> UpdateStatusAsync(Guid id, string status);
    Task<bool> DeleteAsync(Guid id);
}
