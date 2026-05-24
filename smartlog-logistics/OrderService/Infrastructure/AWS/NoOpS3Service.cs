namespace OrderService.Infrastructure.AWS;

/// <summary>
/// Fallback S3 service used when AWS credentials are not configured.
/// Returns placeholder values so the app starts without AWS.
/// </summary>
public class NoOpS3Service : IS3Service
{
    private readonly ILogger<NoOpS3Service> _logger;

    public NoOpS3Service(ILogger<NoOpS3Service> logger)
    {
        _logger = logger;
    }

    public Task<string> UploadFileAsync(Stream fileStream, string fileName, string contentType)
    {
        _logger.LogWarning("S3 not configured. File '{FileName}' was not uploaded.", fileName);
        return Task.FromResult($"local/{Guid.NewGuid()}-{fileName}");
    }

    public Task<string> GetPresignedUrlAsync(string key, int expiryMinutes = 60)
    {
        return Task.FromResult($"http://localhost/files/{key}");
    }

    public Task DeleteFileAsync(string key)
    {
        _logger.LogWarning("S3 not configured. Delete '{Key}' skipped.", key);
        return Task.CompletedTask;
    }
}
