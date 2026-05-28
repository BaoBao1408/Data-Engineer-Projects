[ApiController]
[Route("api/[controller]")]
public class OrderController : ControllerBase
{
    // Controller actions here
    private readonly IOrderService _svc;
    public OrderController(IOrderService svc)
    {
        _svc = svc;
    }
    [HttpGet]
    [Authorize]
    public async Task<IActionResult> GetAll(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 10
    )
    {
        var result = await _svc.GetAllAsync(page, pageSize);
        return Ok(result);
    }
    
    [HttpPost]
    [Authorize]
    public async Task<IActionResult> Create([FromBody] CreateOrderDto dto)
    {
        if (!ModelState.IsValid) return BadRequest(ModelState);
        var order = await _svc.CreateAsync(dto);
        return CreatedAtAction(nameof(GetById),
            new { id = order.Id }, order);

    }
}