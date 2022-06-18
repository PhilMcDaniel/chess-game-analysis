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

source_file_name = 'lichess_db_standard_rated_2018-03.pgn'
games_file = 500000


#get number of games in file
results = file.count_games_lines(source_file_path=f'Downloads/{source_file_name}')
#results

#Get number of files needed based on games, and games/file
files = file.calc_num_files(games = results['Games'],games_per_file = games_file)
#files

#create the empty shell files
file.create_split_files(number_of_files=files,target_directory=f'Downloads/TMP',original_filename=source_file_name[:-4])

#get list of files in a directory with a specific file type
dir = os.path.join(os.getcwd(),'Downloads/TMP')
file_list = file.files_in_dir(directory = dir,file_type='pgn')
#file_list

file.load_split_files(source_file = f'Downloads/{source_file_name}',target_file_list = file_list,games_per_file = games_file)


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
        
        #remove files from TMP/DOWNLOADS after loading to BQ
        os.remove(os.path.join(dir,filename))
    else:
        continue
print(f"All split files processed")


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
