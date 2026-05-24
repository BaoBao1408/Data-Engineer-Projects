namespace Shared.Config;

public class AwsSettings
{
    public string AccessKeyId { get; set; } = string.Empty;
    public string SecretAccessKey { get; set; } = string.Empty;
    public string Region { get; set; } = "ap-southeast-1";
    public string S3Bucket { get; set; } = string.Empty;
}
