import IP2Location
import csv
from pathlib import Path
from config.mongo_connection import connect_mongo
from tqdm import tqdm

CHECKPOINT_FILE = "data/processed_ip_location/checkpoint.txt"

SEEN_FILE = "data/processed_ip_location/seen_ips.txt"

def load_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE) as f:
            return int(f.read().strip())
    return 0


def save_checkpoint(i):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i))


def load_seen():
    if Path(SEEN_FILE).exists():
        return set(open(SEEN_FILE).read().splitlines())
    return set()

def append_seen(ip):
    with open(SEEN_FILE, "a") as f:
        f.write(ip + "\n")

def process_ip_locations(bin_file, output_location, db):

    collection = db["glamira_raw"]

    # 🔥 STREAMING CURSOR (KHÔNG BLOCK)
    cursor = collection.find(
        {"ip": {"$exists": True}},
        {"ip": 1},
        batch_size=1000,
        no_cursor_timeout=True
    )

    location = IP2Location.IP2Location(bin_file)

    start = load_checkpoint()
    print(f"Resume from index {start}")

    Path(output_location).parent.mkdir(parents=True, exist_ok=True)

    with open(output_location, "a", newline="", encoding="utf-8") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=["ip", "country", "region", "city"]
        )

        if start == 0:
            writer.writeheader()

        seen_ips = load_seen()

        for i, doc in enumerate(tqdm(cursor, desc="Streaming IPs")):

            if i < start:
                continue

            ip = doc.get("ip")

            if not ip:
                continue

            if ip in seen_ips:
                continue

            seen_ips.add(ip)
            append_seen(ip)           

            try:
                record = location.get_all(ip)

                writer.writerow({
                    "ip": ip,
                    "country": record.country_long,
                    "region": record.region,
                    "city": record.city
                })

            except Exception as e:
                print(f"Error with {ip}: {e}")

            if i % 1000 == 0:
                save_checkpoint(i)

        save_checkpoint(i)

    print("Finished")
    
if __name__ == "__main__":

    print("START SCRIPT")  # debug

    client, db = connect_mongo()

    process_ip_locations(
        "pipelines/ip_enrichment/ip_geolocation/IP-COUNTRY-REGION-CITY.BIN",
        "data/processed_ip_location/raw_ip_location.csv",
        db
    )