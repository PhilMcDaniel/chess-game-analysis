import os
import logging
from time import perf_counter

from numpy import full
import pandas as pd
import plotly.express as px

import pgn_game_parser as pgn
import split_file as split
import support as sup

logging.basicConfig(level=logging.NOTSET)
#logging.disable()

support = sup.Support()

source_file_name = support.full_path('test.txt')
print(source_file_name)
games_file = 500000


def main():
    
    #instantiate class to split files
    file = split.FileSplit()
    
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
    for filename in os.listdir(dir):
        fullpath = os.path.join(dir,filename)
        #print(fullpath)

        #only load .pgn files
        if fullpath.endswith(".pgn"):
            #load file into dataframe
            dataframe = pgn.parse_pgn_to_dict(fullpath)
            
            #remove files from TMP/DOWNLOADS after loading to BQ
            os.remove(fullpath)
        else:
            continue
    print(f"All split files processed")


if __name__ == "__main__":
    main()