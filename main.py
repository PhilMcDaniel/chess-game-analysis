import os
import new_game_parser as ngp
import bigquery_load_table as blt
import read_from_bigquery as rfb

#get list of files in Downloads folder
dir = 'Downloads/'
for filename in os.listdir(dir):
    fullpath = os.path.join(dir,filename)
    #print(fullpath)

    #only load .pgn files
    if fullpath.endswith(".pgn"):
        #load file into dataframe
        dataframe = ngp.parse_pgn_to_dict(fullpath)
        
        #load dataframe to bigquery
        blt.load_df_to_BQ(dataframe)
        print(f'{filename} has been loaded to bigquery successfully')
    else:
        continue

#send query to bigquery, will be used for dataviz
query = 'SELECT * FROM `valid-logic-327117.ChessGames.ChessGamesTable` LIMIT 1;'
rfb.bq_to_dataframe(query)