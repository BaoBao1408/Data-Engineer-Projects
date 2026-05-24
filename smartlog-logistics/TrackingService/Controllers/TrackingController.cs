using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TrackingService.Data;

namespace TrackingService.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TrackingController : ControllerBase
{
    private readonly TrackingDbContext _db;
    private readonly ILogger<TrackingController> _logger;

    public TrackingController(TrackingDbContext db, ILogger<TrackingController> logger)
    {
        _db = db;
        _logger = logger;
    }

    [HttpGet("{orderCode}")]
    public async Task<IActionResult> GetByOrderCode(string orderCode)
    {
        var record = await _db.TrackingRecords
            .Include(t => t.Events.OrderBy(e => e.OccurredAt))
            .FirstOrDefaultAsync(t => t.OrderCode == orderCode);

        if (record == null) return NotFound(new { Message = $"No tracking found for {orderCode}" });

        return Ok(new
        {
            record.OrderId,
            record.OrderCode,
            record.CustomerName,
            record.OriginAddress,
            record.DestinationAddress,
            record.CurrentStatus,
            record.CreatedAt,
            record.UpdatedAt,
            Timeline = record.Events.Select(e => new
            {
                e.Status,
                e.Location,
                e.Note,
                e.OccurredAt
            })
        });
    }

    [HttpGet]
    public async Task<IActionResult> GetAll([FromQuery] int page = 1, [FromQuery] int size = 10)
    {
        var total = await _db.TrackingRecords.CountAsync();
        var records = await _db.TrackingRecords
            .OrderByDescending(t => t.CreatedAt)
            .Skip((page - 1) * size).Take(size)
            .Select(t => new { t.OrderCode, t.CustomerName, t.CurrentStatus, t.CreatedAt })
            .ToListAsync();

        return Ok(new { Data = records, Total = total, Page = page });
    }
}
