import argparse
from src.data.process_ip_locations import process_ip_locations
from etl.extract.extract_urls import product_map, save_urls
from src.data.product_collection import collect_product
from config.connect import connect

def parse_args():
    parser = argparse.ArgumentParser(
        description="Argparse for Project"
    )

    parser.add_argument(
        "--bin-file",
        default="IP-COUNTRY-REGION-CITY.BIN",
        #required=True,
        help="Path to IP2Location BIN file"
    )
    parser.add_argument(
        "--output-location-path",
        default="data/raw/ip_locations.csv",
        help="Path to output CSV location file"
    )
    parser.add_argument(
        "--output-product-path",
        default="data/raw/product_info.jsonl",
        help="Path to output CSV location file"
    )
    parser.add_argument(
        "--urls-path",
        default="data/raw/urls.jsonl",
        help="Path to output Json urls file"
    )
    parser.add_argument(
        "--failed-product-path",
        default="data/raw/error_403_id.txt",
        help="Path to list failed crawl product id"
    )
    return parser.parse_args()

if __name__ == "__main__":
    arg = parse_args()
    # client = connect()
    # db = client["countly"]
    #process_ip_locations(arg.bin_file, arg.output_location_csv, db)
    # products = product_map(db)
    # save_urls(products, arg.urls_path)
    collect_product(arg.urls_path, arg.output_product_path, arg.failed_product_path)
