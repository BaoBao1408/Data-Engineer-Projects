import logging
from google.cloud import bigquery

def gcs_to_bq(event, context):
    client = bigquery.Client()

    bucket = event['bucket']
    file_name = event['name']

    if not file_name.startswith("raw/products/"):
        return

    uri = f"gs://{bucket}/{file_name}"

    table_id = "glamira-data-foundation.glamira_raw.products_raw"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    logging.info(f"Loaded {file_name}")