from google.cloud import bigquery
import configs
import os


def bq_to_dataframe(query_string):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=configs.GOOGLE_APPLICATION_CREDENTIALS
    table_id = configs.TABLE_ID
    bqclient = bigquery.Client()

    
    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    return(dataframe.head())