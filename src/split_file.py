import os
import math

class FileSplit:
    """Reads downloaded and decompressed pgn file from Lichess & splits into smaller files for parallel processing"""

    def count_games_lines(self,source_file_path):
        """Count the number of lines & chess games in the source file"""
            
        #read from source_file_path
        with open(source_file_path,'r') as file:
            gamecnt = 0
            linescnt = 0

            #loop through file
            for line in file:
                line = line.strip()
                linescnt += 1
            
            #keep track of games
                if line == '1-0' or line == '0-1' or line[:2] == '1.':
                    gamecnt+=1
           
            #once games_per_file is reached, open and write into new file
        return({"Lines":linescnt,"Games":gamecnt})


    #funtion for calculating number of files needed based on number of games per file
    def calc_num_files(self,games,games_per_file):
        """calculate the number of files needed based on the number of games per file"""
        return((math.ceil(games/games_per_file)))

    #function for creating # of split files
    def create_split_files(self,number_of_files,target_directory,original_filename):
        """create the necessary number of empty files"""
        for filenum in range(number_of_files):
            with open(f"{target_directory}/{original_filename}_SPLIT_{filenum}.pgn",'w') as split_file:
                pass

    #function for opening the split files

    #list of files in a directory
    def files_in_dir(self,directory,file_type):
        """For a given directory, return a list of files with a given file type"""
        file_list = []
        for file in os.listdir(directory):
            if file.endswith(file_type):
                file_list.append(file)
            else:
                pass
        #these are string so it doesn't really work, but at least its not random order
        file_list.sort()
        return(file_list)
    
    #function for writing to split files
    def load_split_files(self,source_file,target_file_list,games_per_file):
        """reads a source file and loads separate files up to a chess game limit"""
        #open source file
        with open(source_file,'r') as source_file:
            gamecnt=0

            #list of target files
            for target_file in target_file_list:
                #reset game counter
                gamecnt=0
                #print(f"Writing data into {target_file}")
                
                #open target file
                with open(f"Downloads/TMP/{target_file}",'w') as split_file:
                    
                    #loop through source file and write to target file until gamecnt == games_per_file
                    for line in source_file:
                        split_file.write(line)
                        
                        line=line.strip()
                        #keep track of games
                        if line == '1-0' or line == '0-1' or line[:2] == '1.':
                            gamecnt+=1
                        if gamecnt == games_per_file:
                            break
                print(f"Wrote {gamecnt} games to {target_file}")

