import IP2Location
import pandas as pd
from config.connect import connect

def process_ip_locations(bin_file, output_location, db):
    collection = db["summary"]
    #Read unique IPs
    ips = collection.aggregate([{"$group": {"_id": "$ip"}}])
    #print(list(ips))

    #Load IP2Location database
    location = IP2Location.IP2Location(bin_file)
    results = []
    for doc in ips:
        ip = doc["_id"]
        try:
            record = location.get_all(ip)
            data = {
                "ip": ip,
                "country": record.country_long,
                "region": record.region,
                "city": record.city
            }
            results.append(data)
        except Exception as e:
            print(f"Error with IP {ip}: {e}")

    #Save to csv
    df = pd.DataFrame(results)
    df.to_csv(output_location, index=False)