import os
import new_game_parser as ngp
import bigquery_load_table as blt
import read_from_bigquery as rfb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl

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
query = 'SELECT LEFT(game_date,7) yyyymm,COUNT(*) game_count FROM `valid-logic-327117.ChessGames.ChessGamesTable` GROUP BY 1'
dataframe = rfb.bq_to_dataframe(query)
dataframe['yyyymm'] = pd.to_datetime(dataframe['yyyymm'], format='%Y.%m')

dataframe = dataframe.head(100).sort_values(by="yyyymm", ascending=True)
dataframe.head(100)

ax = dataframe.plot.bar(x='yyyymm',y='game_count')
#format y axis to remove sci-not
ax.get_yaxis().set_major_formatter(
    mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
#format x axis to show only yyymm
#asdf
plt.show()