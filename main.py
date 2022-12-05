
from google.cloud import bigquery
from datetime import date


def hello_pubsub(event, context):


    client = bigquery.Client()
    project = "bigquery-public-data"
    dataset_id = "baseball"
    table_id = "games_post_wide"
    bucket_name='download_test_data'
    today = date.today()
    datestr= today.strftime("%d%m%Y") 


    destination_uri = "gs://{}/{}".format(bucket_name, 'Data_{}.csv'.format(datestr))
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )
    return f'check the results in the logs'

    