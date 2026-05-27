using OrderService.Models;

namespace OrderService.Services;

public interface IAuthService
{
    Task<AppUser?> ValidateAsync(string username, string password);
    Task<AppUser> CreateUserAsync(string username, string password, string role, string fullName);
    Task<bool> ChangePasswordAsync(Guid userId, string newPassword);
    Task<List<AppUser>> GetAllUsersAsync();
    Task<bool> DeactivateUserAsync(Guid userId);
}