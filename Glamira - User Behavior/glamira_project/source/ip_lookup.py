import IP2Location
import csv
from pathlib import Path
from config.mongo_connection import connect_mongo
from tqdm import tqdm

CHECKPOINT_FILE = "data/processed_ip_location/checkpoint.txt"


def load_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE) as f:
            return int(f.read().strip())
    return 0


def save_checkpoint(i):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i))


def process_ip_locations(bin_file, output_location, db):

    collection = db["glamira_raw"]

    pipeline = [
        {"$match": {"ip": {"$exists": True}}},
        {"$group": {"_id": "$ip"}}
    ]

    cursor = collection.aggregate(
        pipeline,
        allowDiskUse=True,
        batchSize=500
    )

    location = IP2Location.IP2Location(bin_file)

    start = load_checkpoint()
    print(f"Resume from index {start}")

    with open(output_location, "a", newline="", encoding="utf-8") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=["ip", "country", "region", "city"]
        )

        if start == 0:
            writer.writeheader()

        total_ips = collection.count_documents({"ip": {"$exists": True}})

        for i, doc in enumerate(tqdm(cursor, total=total_ips)):

            ip = doc["_id"]

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

                save_checkpoint(i)

    print("Finished")


if __name__ == "__main__":

    db = connect_mongo()

    process_ip_locations(
        "source/ip_geolocation/IP-COUNTRY-REGION-CITY.BIN",
        "data/processed_ip_location/ip_locations.csv",
        db
    )