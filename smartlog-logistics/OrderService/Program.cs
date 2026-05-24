using Amazon;
using Amazon.S3;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using OrderService.Data;
using OrderService.Infrastructure.AWS;
using OrderService.Infrastructure.Kafka;
using OrderService.Middleware;
using OrderService.Repositories;
using Shared.Auth;
using Shared.Config;
using System.Text;
using OrderService.Services;
var builder = WebApplication.CreateBuilder(args);

// ── Bind config sections ──────────────────────────────────
var jwtSettings = new JwtSettings();
builder.Configuration.GetSection("Jwt").Bind(jwtSettings);
builder.Services.AddSingleton(jwtSettings);

var awsSettings = new AwsSettings();
builder.Configuration.GetSection("AWS").Bind(awsSettings);
builder.Services.AddSingleton(awsSettings);

// ── PostgreSQL + EF Core ──────────────────────────────────
builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("DefaultConnection"),
        o => o.EnableRetryOnFailure(maxRetryCount: 3)
    ));

// ── AWS S3 (optional — skip if no credentials) ───────────
if (!string.IsNullOrEmpty(awsSettings.AccessKeyId))
{
    builder.Services.AddSingleton<IAmazonS3>(_ =>
        new AmazonS3Client(
            awsSettings.AccessKeyId,
            awsSettings.SecretAccessKey,
            RegionEndpoint.GetBySystemName(awsSettings.Region)));
    builder.Services.AddScoped<IS3Service, S3Service>();
}
else
{
    builder.Services.AddScoped<IS3Service, NoOpS3Service>();
}

// ── Kafka Producer ────────────────────────────────────────
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();

// ── Business Layer ────────────────────────────────────────
builder.Services.AddScoped<IOrderRepository, OrderRepository>();
builder.Services.AddScoped<IOrderService, OrderService.Services.OrderService>();

// ── JWT ───────────────────────────────────────────────────
builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuerSigningKey = true,
            IssuerSigningKey = new SymmetricSecurityKey(
                Encoding.UTF8.GetBytes(jwtSettings.Secret)),
            ValidateIssuer = true,
            ValidIssuer = jwtSettings.Issuer,
            ValidateAudience = true,
            ValidAudience = jwtSettings.Audience,
            ValidateLifetime = true,
            ClockSkew = TimeSpan.Zero
        };
    });
builder.Services.AddAuthorization();

// ── CORS for Angular :4200 and React :3000 ────────────────
builder.Services.AddCors(options =>
    options.AddPolicy("FrontendPolicy", policy =>
        policy.WithOrigins(
                "http://localhost:4200",
                "http://localhost:3000",
                "http://localhost",
                "http://frontend")
              .AllowAnyMethod()
              .AllowAnyHeader()
              .AllowCredentials()));

// ── Controllers + Swagger ─────────────────────────────────
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "Smartlog — Order Service",
        Version = "v1",
        Description = "REST API quản lý đơn hàng logistics"
    });
    c.AddSecurityDefinition("Bearer", new OpenApiSecurityScheme
    {
        Type = SecuritySchemeType.Http,
        Scheme = "bearer",
        BearerFormat = "JWT",
        Description = "Paste JWT token từ /api/auth/login"
    });
    c.AddSecurityRequirement(new OpenApiSecurityRequirement
    {{
        new OpenApiSecurityScheme
        {
            Reference = new OpenApiReference
            { Type = ReferenceType.SecurityScheme, Id = "Bearer" }
        },
        Array.Empty<string>()
    }});
});

// ── Build ─────────────────────────────────────────────────
var app = builder.Build();

// Auto-run EF migrations
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
    db.Database.Migrate();
}

// ── Pipeline ──────────────────────────────────────────────
app.UseMiddleware<ErrorHandlingMiddleware>();
app.UseSwagger();
app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "Order Service v1"));
app.UseCors("FrontendPolicy");
app.UseAuthentication();
app.UseAuthorization();
app.MapControllers();
app.MapGet("/health", () => Results.Ok(new
{
    Status = "Healthy",
    Service = "OrderService",
    Timestamp = DateTime.UtcNow
}));

app.Run();
