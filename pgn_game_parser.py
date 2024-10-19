import pandas as pd
import logging
import os
from time import perf_counter


logging.basicConfig(level=logging.NOTSET)
#logging.disable()


class PGNS():
    ...

    def __init__(self):
        ...


def parse_pgn_to_dict(filename):
    '''Parses out the information from a .pgn file into a dictionary and then a dataframe'''
    
    line_num = 0
    results = 0
    data = {}
    t1_start = perf_counter()
    with open(filename,'r') as file:
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
                                }
            if line[:7] == '[Result':
                results+=1
            else:
                continue
        t1_end = perf_counter()
        t1_total = t1_end - t1_start

    games_processed = len(data)
    logging.info(f"{line_num} rows have been parsed")
    logging.info(f"{games_processed} games have been parsed")
    logging.info(f"{results} results have been parsed")
    logging.info(f"File Processing Duration: {t1_total} seconds")

    #data['https://lichess.org/j1dkb5dw']
    #return data
    #load dict data to datafame

    t2_start = perf_counter()
    df = pd.DataFrame.from_dict(data,orient='index')
    df['game_id']=df.index
    df['source_file_name'] = filename
    df.reset_index(drop=True)
    df = df[['game_type','game_result','game_date', 'game_time', 'player_id_white',
        'player_id_black', 'white_start_elo', 'black_start_elo',
        'white_game_elo', 'black_game_elo', 'game_opening', 'game_time_control',
        'game_termination', 'game_id','source_file_name']]
    t2_end = perf_counter()
    t2_total = t2_end - t2_start
    logging.info(f"Dataframe From Dictionary Duration: {t2_total} seconds")
    #df.describe()
    #df.columns
    #df.dtypes
    return df