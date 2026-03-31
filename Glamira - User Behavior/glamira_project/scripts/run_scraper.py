from pipelines.product_scraper.extract.product_extractor import main as run_scraper


def main():

    print("Start product scraper")

    run_scraper()

    print("Scraper finished")


if __name__ == "__main__":
    main()