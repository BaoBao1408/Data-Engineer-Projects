from google.cloud import storage

client = storage.Client()
bucket = client.bucket("glamira-data-lake-qb")

blob = bucket.blob("raw/test.txt")
blob.upload_from_string("hello")

print("Upload OK")