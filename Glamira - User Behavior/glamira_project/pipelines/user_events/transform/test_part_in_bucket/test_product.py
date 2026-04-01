from google.cloud import storage
import json

client = storage.Client()
bucket = client.bucket("glamira-data-lake-qb")

blob = bucket.blob("raw/products/part_1.jsonl")

# ✅ đọc đúng 1 dòng hoàn chỉnh
with blob.open("r") as f:
    for i in range(5):  # đọc 5 dòng đầu
        line = f.readline()

        if not line:
            break

        try:
            row = json.loads(line)
            print("✅ VALID ROW")
            print(row)
        except Exception as e:
            print("❌ ERROR ROW")
            print(line[:200])
            print(e)