from pipelines.user_events.extract_product_urls import run as run_event_pipeline 


def main():

    print("Starting event pipeline")

    run_event_pipeline()

    print("Event pipeline finished")


if __name__ == "__main__":
    main()