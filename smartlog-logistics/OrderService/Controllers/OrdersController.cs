using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using OrderService.DTOs;
using OrderService.Infrastructure.AWS;
using OrderService.Services;

namespace OrderService.Controllers;

[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class OrdersController : ControllerBase
{
    private readonly IOrderService _service;
    private readonly IS3Service _s3;
    private readonly ILogger<OrdersController> _logger;

    public OrdersController(IOrderService service, IS3Service s3, ILogger<OrdersController> logger)
    {
        _service = service;
        _s3 = s3;
        _logger = logger;
    }

    // GET api/orders?page=1&pageSize=10
    [HttpGet]
    public async Task<IActionResult> GetAll([FromQuery] int page = 1, [FromQuery] int pageSize = 10)
    {
        var (items, total) = await _service.GetAllAsync(page, pageSize);
        return Ok(new
        {
            Data = items,
            Total = total,
            Page = page,
            PageSize = pageSize,
            TotalPages = (int)Math.Ceiling(total / (double)pageSize)
        });
    }

    // GET api/orders/{id}
    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetById(Guid id)
    {
        var order = await _service.GetByIdAsync(id);
        return order == null
            ? NotFound(new { Message = $"Order {id} not found" })
            : Ok(order);
    }

    // POST api/orders
    [HttpPost]
    [Authorize]
    public async Task<IActionResult> Create([FromBody] CreateOrderDto dto)
    {
        if (!ModelState.IsValid) return BadRequest(ModelState);
        var order = await _service.CreateAsync(dto);
        return CreatedAtAction(nameof(GetById), new { id = order.Id }, order);
    }

    // PATCH api/orders/{id}/status
    [HttpPatch("{id:guid}/status")]
    [Authorize]
    public async Task<IActionResult> UpdateStatus(Guid id, [FromBody] UpdateStatusDto dto)
    {
        var order = await _service.UpdateStatusAsync(id, dto.Status);
        return order == null ? NotFound() : Ok(order);
    }

    // POST api/orders/{id}/attachments — upload delivery photo to S3
    [HttpPost("{id:guid}/attachments")]
    [Authorize]
    public async Task<IActionResult> UploadAttachment(Guid id, IFormFile file)
    {
        if (file == null || file.Length == 0)
            return BadRequest(new { Message = "No file provided" });

        var order = await _service.GetByIdAsync(id);
        if (order == null) return NotFound();

        using var stream = file.OpenReadStream();
        var key = await _s3.UploadFileAsync(stream, file.FileName, file.ContentType);
        var url = await _s3.GetPresignedUrlAsync(key);

        _logger.LogInformation("📎 File uploaded for order {OrderId}: {Key}", id, key);

        return Ok(new UploadFileResponseDto
        {
            Key = key,
            Url = url,
            FileName = file.FileName,
            SizeBytes = file.Length
        });
    }

    // GET api/orders/attachment-url?key=xxx — get fresh presigned URL
    [HttpGet("attachment-url")]
    [Authorize]
    public async Task<IActionResult> GetAttachmentUrl([FromQuery] string key)
    {
        var url = await _s3.GetPresignedUrlAsync(key);
        return Ok(new { Url = url });
    }

    // DELETE api/orders/{id}
    [HttpDelete("{id:guid}")]
    [Authorize]
    public async Task<IActionResult> Delete(Guid id)
    {
        var result = await _service.DeleteAsync(id);
        return result ? NoContent() : NotFound();
    }
}

public record UpdateStatusDto(string Status);
