import os
import pandas as pd
import plotly.express as px

import pgn_game_parser as pgn
import interact_with_bigquery as bq
import split_file as split

#instantiate class to split files
file = split.FileSplit()

#instantiate BQ class
bigquery = bq.BigQuery()

source_file_name = 'lichess_db_standard_rated_2016-12.pgn'


#get number of games in file
results = file.count_games_lines(source_file_path='Downloads/{source_file_name}')
results

#Get number of files needed based on games, and games/file
files = file.calc_num_files(games = results['Games'],games_per_file = 1000000)
files

#create the empty shell files
file.create_split_files(number_of_files=files,target_directory='Downloads/TMP',original_filename=source_file_name[:-4])

#get list of files in a directory with a specific file type
file_list = file.files_in_dir(directory = '/Users/philmcdaniel/Documents/GitHub/chess-game-analysis/Downloads/TMP',file_type='pgn')
file_list

file.load_split_files(source_file = 'Downloads/{source_file_name}',target_file_list = file_list,games_per_file = 1000000)


#get list of files in Downloads folder
dir = 'Downloads/TMP'
for filename in os.listdir(dir):
    fullpath = os.path.join(dir,filename)
    #print(fullpath)

    #only load .pgn files
    if fullpath.endswith(".pgn"):
        #load file into dataframe
        dataframe = pgn.parse_pgn_to_dict(fullpath)
        
        #load dataframe to bigquery
        bigquery.load_df_to_BQ(dataframe)
        print(f'{filename} has been loaded to bigquery successfully')
    else:
        continue

#send query to bigquery, will be used for dataviz
query = 'SELECT LEFT(game_date,7) yyyymm,COUNT(*) game_count FROM `valid-logic-327117.ChessGames.ChessGamesTable` GROUP BY 1'
dataframe = bigquery.bq_to_dataframe(query)
dataframe['yyyymm'] = pd.to_datetime(dataframe['yyyymm'], format='%Y.%m')

dataframe = dataframe.sort_values(by="yyyymm", ascending=True).reset_index()
dataframe.head(100)


fig = px.bar(dataframe, x='yyyymm', y='game_count'
,labels={'yyyymm':'Game Month','game_count':'Game Count'}
,title='Chess Games Played Over Time'
)
fig.show()
