import argparse

from scripts.run_event_pipeline import main as run_event_pipeline
from scripts.run_scraper import main as run_scraper
from scripts.run_ip_enrichment import main as run_ip_enrichment


def parse_args():

    parser = argparse.ArgumentParser(
        description="Glamira Data Pipeline"
    )

    parser.add_argument(
        "--events",
        action="store_true",
        help="Run event pipeline"
    )

    parser.add_argument(
        "--scraper",
        action="store_true",
        help="Run product scraper"
    )

    parser.add_argument(
        "--ip",
        action="store_true",
        help="Run IP enrichment"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run full pipeline"
    )

    return parser.parse_args()


def main():

    args = parse_args()

    print("\n==== Glamira Data Pipeline ====\n")

    if args.all:

        print("Step 1: Extract product URLs from events")
        run_event_pipeline()

        print("\nStep 2: Crawl product pages")
        run_scraper()

        print("\nStep 3: IP enrichment")
        run_ip_enrichment()

        print("\nPipeline completed\n")

        return

    if args.events:
        print("Running event pipeline")
        run_event_pipeline()

    if args.scraper:
        print("Running scraper")
        run_scraper()

    if args.ip:
        print("Running IP enrichment")
        run_ip_enrichment()

    print("\nFinished\n")


if __name__ == "__main__":
    main()