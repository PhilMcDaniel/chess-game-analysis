import pandas as pd
import logging
import os

from support import measure_time


logging.basicConfig(level=logging.NOTSET)
#logging.disable()


class PGNS():
    """
    Class to represent the PGN file of chess games
    """

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
                                    ,'source_file':os.path.basename(file_name)
                                    }
                if line[:7] == '[Result':
                    results+=1
                else:
                    continue

            self.lines = line_num
            self.games = len(data)

        
        logging.info(f"{self.lines} rows have been parsed")
        logging.info(f"{self.games} games have been parsed")
        logging.info(f"{results} results have been parsed")

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
        
        return games_pddf
    
    @measure_time
    def pddf_to_parquet(self,pddf,parquet_file_name):
        # Ensure that the passed object is a pandas DataFrame
        if isinstance(pddf, pd.DataFrame):
            # Save the DataFrame as a .parquet file
            # TODO: move to /data/ folder for now
            pddf.to_parquet(f"{parquet_file_name}.parquet")
            logging.info(f"dataframe has been written to {parquet_file_name}.parquet")
        else:
            raise Exception("The provided ")

        return self