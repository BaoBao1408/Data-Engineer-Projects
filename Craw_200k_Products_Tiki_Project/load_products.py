import os
import glob
import json
import psycopg2
from psycopg2 import OperationalError, DatabaseError
from psycopg2.extras import execute_values, Json

from config import load_config

OUTPUT_DIR = "output"
FILE_PATTERN = "products_*.json*"


def iter_products_from_file(path):
    try:    
        with open(path, "r", encoding="utf-8") as f:
            first_char = f.read(1)
            f.seek(0)

            if first_char == "[":  
                try:
                    data = json.load(f)
                except JSONDecodeError as e:
                    print(f"[ERROR] Invalid JSON in file {path}: {e}")
                    return
                    
                for obj in data:
                    if obj:
                        yield obj
            else:  
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except JSONDecodeError as e:
                        print(f"[WARN] Skip bad JSON line in {path}: {e}")
    except FileNotFoundError:
        print(f"[ERROR] File not found: {path}")
    except PermissionError:
        print(f"[ERROR] Permission denied when reading: {path}")

def insert_batch(cur, batch):
    execute_values(
        cur,
        """
        INSERT INTO tiki_products (id, name, url_key, price, description, images)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name        = EXCLUDED.name,
            url_key     = EXCLUDED.url_key,
            price       = EXCLUDED.price,
            description = EXCLUDED.description,
            images      = EXCLUDED.images;
        """,
        batch,
    )


def load_file(cur, path, batch_size=1000):
    print(f"Loading {os.path.basename(path)}")

    batch = []
    total = 0

    for obj in iter_products_from_file(path):
        row = (
            obj.get("id"),
            obj.get("name"),
            obj.get("url_key"),
            obj.get("price"),
            obj.get("description"),
            Json(obj.get("images", [])),
        )
        batch.append(row)

        if len(batch) >= batch_size:
            insert_batch(cur, batch)
            total += len(batch)
            batch.clear()

    if batch:
        insert_batch(cur, batch)
        total += len(batch)

    print(f"Done {os.path.basename(path)} → {total} rows")


def main():
    pattern = os.path.join(OUTPUT_DIR, FILE_PATTERN)
    files = sorted(glob.glob(pattern))

    if not files:
        print("Not Found any Files in Folder: output/")
        return

    print(f"TOTAL FILES: {len(files)}")

    config = load_config()

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for path in files:
                    try:
                        load_file(cur, path)
                        conn.commit()
                    except DatabaseError as e:
                        conn.rollback()
                        print(f"[ERROR] Failed to load file {path}: {e}")
        print("🎉 LOAD DATA SUCCESSFULLY!")
    except OperationalError as e:
        print(f"[ERROR] Could not connect to PostgreSQL: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error when loading data: {e}")

if __name__ == "__main__":
    main()
