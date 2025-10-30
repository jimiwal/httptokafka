using HttpToKafka.Api.Kafka;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;
[ApiController]
[Route("api/admin")]
public sealed class AdminController : ControllerBase
{
    private readonly AdminClientService _admin;
    public AdminController(AdminClientService admin) => _admin = admin;

    [HttpGet("config")]
    [Authorize]
    public IActionResult Config() => Ok(_admin.GetSafeConfig());

    [HttpGet("topics")]
    [Authorize]
    public async Task<IActionResult> Topics(CancellationToken ct) => Ok(await _admin.ListTopicsAsync(ct));

    [HttpGet("topics/{topic}")]
    [Authorize]
    public async Task<IActionResult> Describe(string topic) => Ok(await _admin.DescribeTopicAsync(topic));
}
