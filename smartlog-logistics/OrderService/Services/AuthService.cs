using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Models;

namespace OrderService.Services;

public class AuthService : IAuthService
{
    private readonly OrderDbContext _db;

    public AuthService(OrderDbContext db)
    {
        _db = db;
    }

    // Validate login — trả null nếu sai
    public async Task<AppUser?> ValidateAsync(string username, string password)
    {
        var user = await _db.Users
            .FirstOrDefaultAsync(u => u.Username == username && u.IsActive);

        if (user == null) return null;

        // BCrypt.Verify so sánh plaintext với hash trong DB
        return BCrypt.Net.BCrypt.Verify(password, user.PasswordHash)
            ? user
            : null;
    }

    // Tạo user mới — dùng khi onboard nhân viên
    public async Task<AppUser> CreateUserAsync(
        string username, string password, string role, string fullName)
    {
        var user = new AppUser
        {
            Username = username,
            PasswordHash = BCrypt.Net.BCrypt.HashPassword(password), // hash trước khi lưu
            Role = role,
            FullName = fullName
        };
        _db.Users.Add(user);
        await _db.SaveChangesAsync();
        return user;
    }

    // Đổi mật khẩu
    public async Task<bool> ChangePasswordAsync(Guid userId, string newPassword)
    {
        var user = await _db.Users.FindAsync(userId);
        if (user == null) return false;

        user.PasswordHash = BCrypt.Net.BCrypt.HashPassword(newPassword);
        await _db.SaveChangesAsync();
        return true;
    }

    // Xem danh sách user (cho admin panel)
    public async Task<List<AppUser>> GetAllUsersAsync()
        => await _db.Users.OrderBy(u => u.CreatedAt).ToListAsync();

    // Vô hiệu hoá user — không xóa, chỉ IsActive = false
    public async Task<bool> DeactivateUserAsync(Guid userId)
    {
        var user = await _db.Users.FindAsync(userId);
        if (user == null) return false;

        user.IsActive = false;
        await _db.SaveChangesAsync();
        return true;
    }
}