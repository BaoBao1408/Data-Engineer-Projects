from pipelines.ip_enrichment.ip_lookup import main as run_ip_enrichment


def main():

    print("Start IP enrichment script")

    run_ip_enrichment()

    print("Finished IP enrichment")


if __name__ == "__main__":
    main()