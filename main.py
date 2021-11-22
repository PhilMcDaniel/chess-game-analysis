import os
import new_game_parser as ngp
import bigquery_load_table as blt
import read_from_bigquery as rfb
import pandas as pd
import matplotlib.pyplot as plt

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
query = 'SELECT LEFT(game_date,7) yyyymm,COUNT(*) FROM `valid-logic-327117.ChessGames.ChessGamesTable` GROUP BY 1'
dataframe = rfb.bq_to_dataframe(query)
dataframe['yyyymm'] = pd.to_datetime(dataframe['yyyymm'], format='%Y.%m')

dataframe.head(100).sort_values(by="f0_", ascending=False)


ax = dataframe.plot.bar(x='yyyymm',y='f0_')
plt.show()