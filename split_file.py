import os
import math

class FileSplit:
    """Reads downloaded pgn file from Lichess & splits into smaller files for parallel processing"""

    def count_games_lines(self,source_file_path):
        """Count the number of lines & chess games in the source file"""
            
        #read from source_file_path
        with open(source_file_path,'r') as file:
            gamecnt = 0
            linescnt = 0

            #write lines to new file
            for line in file:
                line = line.strip()
                linescnt += 1
            
            #keep track of games
                if line == '1-0' or line == '0-1' or line[:2] == '1.':
                    gamecnt+=1
           
            #once games_per_file is reached, open and write into new file
        return({"Lines":linescnt,"Games":gamecnt})


    #funtion for calculating number of files needed based on number of games per file
    def calc_num_files(self,games,files_per_game):
        """calculate the number of files needed based on the number of games per file"""
        return(print(math.ceil(games/files_per_game)))

    #function for creating # of split files
    def create_split_files(self,number_of_files,directory):
        """create the necessary number of empty files"""
        for filenum in range(number_of_files):
            with open(f"{directory}/lichess_db_standard_rated_2013-08_SPLIT_{filenum}.pgn",'w') as split_file:
                pass

    #function for writing to split files



file = FileSplit()

results = file.count_games_lines(source_file_path='Downloads/TMP/lichess_db_standard_rated_2013-08.pgn')
results

files = file.calc_num_files(1001,10)
files


file.create_split_files(number_of_files=20,directory='Downloads/TMP')