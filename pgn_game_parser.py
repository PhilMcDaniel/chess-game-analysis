import pandas as pd
import logging
import sys
import os
import json

from support import measure_time, does_file_exist,full_path


logging.basicConfig(level=logging.NOTSET)
#logging.disable()


class PGNS():
    """
    Class to represent the PGN file of chess games
    """

    
    def count_chess_moves(self,game_string):
        """
        Calculate the number of moves in a chess game from standard algebraic notation,
        handling annotations and variations.
        
        Args:
            game_string (str): The chess game in standard algebraic notation
            
        Returns:
            int: The number of moves played
        """
        # Remove evaluations in curly braces
        while '{' in game_string and '}' in game_string:
            start = game_string.find('{')
            end = game_string.find('}') + 1
            game_string = game_string[:start] + game_string[end:]
        
        # Split into tokens
        tokens = game_string.split()
        
        # Remove the game result if present
        if tokens[-1] in ["1-0", "0-1", "1/2-1/2", "*"]:
            tokens = tokens[:-1]
        
        # Get the last move number
        for token in reversed(tokens):
            # Handle both "5." and "5..." format
            if token.endswith('.'):
                try:
                    return int(token[:-1])
                except ValueError:
                    continue
            elif "..." in token:
                try:
                    return int(token.split('.')[0])
                except (ValueError, IndexError):
                    continue
        
        return 0


    @measure_time
    def parse_pgn_to_dict(self,file_name):
        '''    
        Parameters:
            file_name - The file name of the pgn
        Returns:
            data - dict: Dictionary of games from the pgn file
        Sets:
            PGNS.lines
            PGNS.games
        Raises:
            
        '''
        
        line_num = 0
        results = 0
        data = {}
        with open(file_name,'r') as file:
            for line in file:
                line = line.strip()
                line_num += 1

                #parse game_type
                if (line[:6] == '[Event'):
                    game_type = line[8:-2]
                #parse game_id
                if (line[:5] == '[Site'):
                    game_id = line[7:-2]
                #parse player_id_white
                if (line[:7] == '[White '):
                    player_id_white = line[8:-2]
                #parse player_id_black
                if (line[:7] == '[Black '):
                    player_id_black = line[8:-2]      
                #parse game_result    
                if (line[:7] == '[Result'):
                    game_result = line[9:-2]
                    if (game_result == '1-0'):
                        game_result = 'White'
                    elif (game_result == '0-1'):
                        game_result = 'Black'
                    else:  
                        game_result = 'Draw'
                #parse game_date
                if (line[:8] == '[UTCDate'):
                    game_date = line[10:-2]
                #parse game_time
                if (line[:8] == '[UTCTime'):
                    game_time = line[10:-2]
                
                #parse white_start_elo
                if (line[:9] == '[WhiteElo'):
                    white_start_elo = line[12:-2]
                #parse black_start_elo
                if (line[:9] == '[BlackElo'):
                    black_start_elo = line[12:-2]
                #parse white_game_elo
                if (line[:16] == '[WhiteRatingDiff'):
                    white_game_elo = line[18:-2]
                #parse black_game_elo
                if (line[:16] == '[BlackRatingDiff'):
                    black_game_elo = line[18:-2]
                #parse game_opening
                if (line[:8] == '[Opening'):
                    game_opening = line[10:-2]        
                #parse game_time_control
                if (line[:12] == '[TimeControl'):
                    game_time_control = line[14:-2] 
                #parse game_termination
                if (line[:12] == '[Termination'):
                    game_termination = line[14:-2]

                #game moves
                if (line[:2] == '1.'):
                    game_length = self.count_chess_moves(game_string = line)

                #null handling for missing source data
                    try: white_game_elo
                    except NameError: white_game_elo = '0'
                    try: black_game_elo
                    except NameError: black_game_elo = '0'
                #insert in dictionary now that we have a full "game" worth of data
                    data[game_id] = {
                                    'game_type':game_type
                                    ,'game_result':game_result
                                    ,'game_date':game_date
                                    ,'game_time':game_time
                                    ,'player_id_white':player_id_white
                                    ,'player_id_black':player_id_black
                                    ,'white_start_elo':white_start_elo
                                    ,'black_start_elo':black_start_elo
                                    ,'white_game_elo':white_game_elo
                                    ,'black_game_elo':black_game_elo
                                    ,'game_opening':game_opening
                                    ,'game_time_control':game_time_control
                                    ,'game_termination':game_termination
                                    ,'game_length':game_length
                                    ,'source_file':os.path.basename(file_name)
                                    }
                if line[:7] == '[Result':
                    results+=1
                else:
                    continue
            
            self.lines = line_num
            self.games = len(data)
            self.dict_size = round(sys.getsizeof(data)/ (1024 ** 2),1)


        logging.info(f"{self.lines} rows have been parsed")
        logging.info(f"{self.games} games have been parsed")
        logging.info(f"{results} results have been parsed")
        logging.info(f"data dictionary size: {self.dict_size} MB")

        return data
    
    @measure_time
    def dict_to_pddf(self,dict):
        """
        Parameters:
            dict - A dictionary of chess games
        Returns:
            games_pddf - pandas dataframe: Games from pgn flattened to 1 row per game
        Sets:
        Raises:  
        """
        
        games_pddf = pd.DataFrame.from_dict(dict, orient='index')
        self.pddf_size = round((games_pddf.memory_usage(deep=True).sum() / (1024 ** 2)),2)
        logging.info(f"Dataframe size: {self.pddf_size} MB")
        return games_pddf
    
    @measure_time
    def pddf_to_parquet(self,pddf,parquet_file_name):
        self.parquet_file_name = f"{full_path(parquet_file_name)}.parquet"
        # Ensure that the passed object is a pandas DataFrame
        if isinstance(pddf, pd.DataFrame):
            
            if does_file_exist(self.parquet_file_name):
                logging.info(f"Parquet file already exists: {self.parquet_file_name}")
            else:
                # Save the DataFrame as a .parquet file
                pddf.to_parquet(self.parquet_file_name)
                logging.info(f"Dataframe has been written to {self.parquet_file_name}")
        else:
            raise Exception(f"The provided pddf was not a dataframe: {type(pddf)}")

        self.parquet_size = round((os.path.getsize(parquet_file_name) / (1024 ** 2)),2)

        logging.info(f"Parquet file size: {self.parquet_size} MB")
        return self
    
    @measure_time
    def get_longest_game(self,pddf):
        # Find the row with the maximum 'game_length'
        longest_game_row = pddf.loc[pddf['game_length'].idxmax()]
        
        # Extract the game_id from the index and the game_length
        game_id = longest_game_row.name  # Index as game_id
        length = int(longest_game_row['game_length'])
        
        result = (length, game_id)
        logging.info(f"The longest game was {length} moves: {game_id}")
        
        return result
    
    @measure_time
    def intermediate_metrics(self,pddf,target_file_name):
        target_file_name = full_path(target_file_name)
        data={"test":"testval"}
        # Write JSON data to a file
        with open(target_file_name, "w") as json_file:
            json.dump(data, json_file, indent=4)

        logging.info(f"Data written to {target_file_name}")