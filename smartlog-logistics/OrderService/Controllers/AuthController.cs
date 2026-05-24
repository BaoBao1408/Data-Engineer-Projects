using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using Shared.Auth;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace OrderService.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AuthController : ControllerBase
{
    private readonly JwtSettings _jwt;

    public AuthController(JwtSettings jwt)
    {
        _jwt = jwt;
    }

    [HttpPost("login")]
    public IActionResult Login([FromBody] LoginDto dto)
    {
        // NOTE: In production, validate against real user DB
        // This is simplified for demo purposes
        if (dto.Username == "admin" && dto.Password == "smartlog123")
        {
            var token = GenerateToken(dto.Username, "Admin");
            return Ok(new { Token = token, ExpiresIn = _jwt.ExpireMinutes * 60 });
        }

        if (dto.Username == "driver" && dto.Password == "driver123")
        {
            var token = GenerateToken(dto.Username, "Driver");
            return Ok(new { Token = token, ExpiresIn = _jwt.ExpireMinutes * 60 });
        }

        return Unauthorized(new { Message = "Invalid credentials" });
    }

    private string GenerateToken(string username, string role)
    {
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_jwt.Secret));
        var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

        var claims = new[]
        {
            new Claim(ClaimTypes.Name, username),
            new Claim(ClaimTypes.Role, role),
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

public record LoginDto(string Username, string Password);
