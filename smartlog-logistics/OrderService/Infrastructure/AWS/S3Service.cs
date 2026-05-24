using Amazon.S3;
using Amazon.S3.Model;
using Shared.Config;

namespace OrderService.Infrastructure.AWS;

public interface IS3Service
{
    Task<string> UploadFileAsync(Stream fileStream, string fileName, string contentType);
    Task<string> GetPresignedUrlAsync(string key, int expiryMinutes = 60);
    Task DeleteFileAsync(string key);
}

public class S3Service : IS3Service
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucket;
    private readonly ILogger<S3Service> _logger;

    public S3Service(IAmazonS3 s3Client, AwsSettings settings, ILogger<S3Service> logger)
    {
        _s3Client = s3Client;
        _bucket = settings.S3Bucket;
        _logger = logger;
    }

    public async Task<string> UploadFileAsync(Stream fileStream, string fileName, string contentType)
    {
        var key = $"orders/{DateTime.UtcNow:yyyy/MM/dd}/{Guid.NewGuid()}-{fileName}";

        var request = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = key,
            InputStream = fileStream,
            ContentType = contentType,
            ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256
        };

        await _s3Client.PutObjectAsync(request);
        _logger.LogInformation("✅ Uploaded file to S3: {Key}", key);
        return key;
    }

    public async Task<string> GetPresignedUrlAsync(string key, int expiryMinutes = 60)
    {
        var request = new GetPreSignedUrlRequest
        {
            BucketName = _bucket,
            Key = key,
            Expires = DateTime.UtcNow.AddMinutes(expiryMinutes)
        };
        return await Task.FromResult(_s3Client.GetPreSignedURL(request));
    }

    public async Task DeleteFileAsync(string key)
    {
        await _s3Client.DeleteObjectAsync(_bucket, key);
        _logger.LogInformation("🗑️ Deleted S3 file: {Key}", key);
    }
}
