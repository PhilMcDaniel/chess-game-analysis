import os
import new_game_parser 
import bigquery_load_table



data.head(5)

bigquery_load_table.load_df_to_BQ(dataframe = parse_pgn_to_dict('Downloads/lichess_db_standard_rated_2014-08.pgn'))