using OrderService.Models;

namespace OrderService.Repositories;

public interface IOrderRepository
{
    Task<IEnumerable<Order>> GetAllAsync(int page, int pageSize);
    Task<Order?> GetByIdAsync(Guid id);
    Task<Order?> GetByOrderCodeAsync(string orderCode);
    Task<Order> CreateAsync(Order order);
    Task<Order> UpdateAsync(Order order);
    Task<bool> DeleteAsync(Guid id);
    Task<int> CountAsync();
}
