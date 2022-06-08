import os
import pgn_game_parser as pgn
import interact_with_bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.dates as mdates
import split_file as split

#instantiate class to split files
file = split.FileSplit()

#instantiate BQ class
bigquery = bq.BigQuery()

#get number of games in file
results = file.count_games_lines(source_file_path='Downloads/lichess_db_standard_rated_2013-08.pgn')
results

#Get number of files needed based on games, and games/file
files = file.calc_num_files(games = results['Games'],games_per_file = 100000)
files

#create the empty shell files
file.create_split_files(number_of_files=files,target_directory='Downloads/TMP',original_filename='lichess_db_standard_rated_2013-08')

#get list of files in a directory with a specific file type
file_list = file.files_in_dir(directory = '/Users/philmcdaniel/Documents/GitHub/chess-game-analysis/Downloads/TMP',file_type='pgn')
file_list

file.load_split_files(source_file = 'Downloads/lichess_db_standard_rated_2013-08.pgn',target_file_list = file_list,games_per_file = 100000)


#get list of files in Downloads folder
dir = 'Downloads/'
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

dataframe = dataframe.head(100).sort_values(by="yyyymm", ascending=True)
dataframe.head(100)
dataframe.dtypes

ax = dataframe.plot.bar(x='yyyymm',y='game_count')
#format y axis to remove sci-not
ax.get_yaxis().set_major_formatter(
    mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
#format x axis to show only yyymm
#close, but not perfect yet
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax.set_xlabel("Date")  # Naming the x-axis

#plt.xticks(rotation=85)
#asdf
plt.show()