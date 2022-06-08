from google.cloud import bigquery
import configs
import os
import pgn_game_parser
import configs

class BigQuery():
    """Class that handles interactions with bigquery, reading and writing"""

    def bq_to_dataframe(query_string):
        """Runs query against bigquery and returns a data frame of the results"""
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
        return(dataframe)


    def load_df_to_BQ(dataframe):
        """Loads dataframe to BigQuery table"""
        #load df to bigquery table
        # Construct a BigQuery client object.
        #https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
        
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=configs.GOOGLE_APPLICATION_CREDENTIALS
        table_id = configs.TABLE_ID
        client = bigquery.Client()

        #todo fix issue with insert misalignment. 
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("game_type",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("game_result",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("game_date",bigquery.enums.SqlTypeNames.STRING),	
                bigquery.SchemaField("game_time",bigquery.enums.SqlTypeNames.STRING),	
                bigquery.SchemaField("player_id_white",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("player_id_black",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("white_start_elo",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("black_start_elo",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("white_game_elo",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("black_game_elo",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("game_opening",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("game_time_control",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("game_termination",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("game_id",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("source_file_name",bigquery.enums.SqlTypeNames.STRING),
            ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_APPEND",
        )

        job = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )  # Make an API request.
        return job.result()  # Wait for the job to complete.

        ##current table rowcount
        #table = client.get_table(table_id)  # Make an API request.
        #print(
        #    "Loaded {} rows and {} columns to {}".format(
        #        table.num_rows, len(table.schema), table_id
        #    )
        #)