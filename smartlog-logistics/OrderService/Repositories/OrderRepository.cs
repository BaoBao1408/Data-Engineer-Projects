using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Models;

namespace OrderService.Repositories;

public class OrderRepository : IOrderRepository
{
    private readonly OrderDbContext _db;

    public OrderRepository(OrderDbContext db)
    {
        _db = db;
    }

    public async Task<IEnumerable<Order>> GetAllAsync(int page, int pageSize)
    {
        return await _db.Orders
            .Include(o => o.Items)
            .OrderByDescending(o => o.CreatedAt)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();
    }

    public async Task<Order?> GetByIdAsync(Guid id)
        => await _db.Orders.Include(o => o.Items).FirstOrDefaultAsync(o => o.Id == id);

    public async Task<Order?> GetByOrderCodeAsync(string orderCode)
        => await _db.Orders.Include(o => o.Items).FirstOrDefaultAsync(o => o.OrderCode == orderCode);

    public async Task<Order> CreateAsync(Order order)
    {
        _db.Orders.Add(order);
        await _db.SaveChangesAsync();
        return order;
    }

    public async Task<Order> UpdateAsync(Order order)
    {
        order.UpdatedAt = DateTime.UtcNow;
        _db.Orders.Update(order);
        await _db.SaveChangesAsync();
        return order;
    }

    public async Task<bool> DeleteAsync(Guid id)
    {
        var order = await _db.Orders.FindAsync(id);
        if (order == null) return false;
        _db.Orders.Remove(order);
        await _db.SaveChangesAsync();
        return true;
    }

    public async Task<int> CountAsync() => await _db.Orders.CountAsync();
}
