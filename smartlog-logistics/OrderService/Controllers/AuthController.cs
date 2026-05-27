using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using OrderService.Services;
using Shared.Auth;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace OrderService.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AuthController : ControllerBase
{
    private readonly IAuthService _auth;
    private readonly JwtSettings _jwt;

    public AuthController(IAuthService auth, JwtSettings jwt)
    {
        _auth = auth;
        _jwt = jwt;
    }

    // ── Login ──────────────────────────────────────────────
    [HttpPost("login")]
    public async Task<IActionResult> Login([FromBody] LoginDto dto)
    {
        var user = await _auth.ValidateAsync(dto.Username, dto.Password);
        if (user == null)
            return Unauthorized(new { Message = "Sai tài khoản hoặc mật khẩu" });

        var token = GenerateToken(user.Username, user.Role, user.Id);
        return Ok(new
        {
            Token = token,
            ExpiresIn = _jwt.ExpireMinutes * 60,
            User = new { user.Username, user.Role, user.FullName }
        });
    }

    // ── Tạo user mới (chỉ Admin) ───────────────────────────
    [HttpPost("users")]
    [Authorize(Roles = "Admin")]
    public async Task<IActionResult> CreateUser([FromBody] CreateUserDto dto)
    {
        var user = await _auth.CreateUserAsync(
            dto.Username, dto.Password, dto.Role, dto.FullName);

        return Created($"/api/auth/users/{user.Id}", new
        {
            user.Id,
            user.Username,
            user.Role,
            user.FullName,
            user.CreatedAt
        });
    }

    // ── Xem danh sách user (chỉ Admin) ────────────────────
    [HttpGet("users")]
    [Authorize(Roles = "Admin")]
    public async Task<IActionResult> GetUsers()
    {
        var users = await _auth.GetAllUsersAsync();
        return Ok(users.Select(u => new
        {
            u.Id, u.Username, u.Role, u.FullName, u.IsActive, u.CreatedAt
        }));
    }

    // ── Đổi mật khẩu ──────────────────────────────────────
    [HttpPatch("users/{id}/password")]
    [Authorize(Roles = "Admin")]
    public async Task<IActionResult> ChangePassword(
        Guid id, [FromBody] ChangePasswordDto dto)
    {
        var ok = await _auth.ChangePasswordAsync(id, dto.NewPassword);
        return ok ? NoContent() : NotFound();
    }

    // ── Vô hiệu hoá user ──────────────────────────────────
    [HttpDelete("users/{id}")]
    [Authorize(Roles = "Admin")]
    public async Task<IActionResult> Deactivate(Guid id)
    {
        var ok = await _auth.DeactivateUserAsync(id);
        return ok ? NoContent() : NotFound();
    }

    // ── Generate JWT ───────────────────────────────────────
    private string GenerateToken(string username, string role, Guid userId)
    {
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_jwt.Secret));
        var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

        var claims = new[]
        {
            new Claim(ClaimTypes.Name, username),
            new Claim(ClaimTypes.Role, role),
            new Claim(ClaimTypes.NameIdentifier, userId.ToString()),
            new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString())
        };

        var token = new JwtSecurityToken(
            issuer: _jwt.Issuer,
            audience: _jwt.Audience,
            claims: claims,
            expires: DateTime.UtcNow.AddMinutes(_jwt.ExpireMinutes),
            signingCredentials: creds
        );

        return new JwtSecurityTokenHandler().WriteToken(token);
    }
}

// DTOs
public record LoginDto(string Username, string Password);
public record CreateUserDto(string Username, string Password, string Role, string FullName);
public record ChangePasswordDto(string NewPassword);