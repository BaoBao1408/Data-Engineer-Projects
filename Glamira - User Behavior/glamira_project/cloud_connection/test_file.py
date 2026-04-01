from google.cloud import storage

client = storage.Client()
bucket = client.bucket("glamira-data-lake-qb")

blob = bucket.blob("raw/glamira/part_1.json")

# đọc 1 chunk nhỏ (~1KB)
data = blob.download_as_text(start=0, end=1000)

# lấy dòng đầu
first_line = data.split("\n")[0]

print(first_line)