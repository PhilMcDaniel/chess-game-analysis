import os

gamecnt = 0
filecnt = 1
games_per_file = 10000
with open('Downloads/tmp/lichess_db_standard_rated_2016-01.pgn','r') as file:
    for line in file:
        line = line.strip()
        with open(f'Downloads/split_files/split_lichess_db_standard_rated_2016-01_{filecnt}.pgn', 'a+') as the_file:
            the_file.write(line+'\n')
            if line == '1-0' or line == '0-1' or line[:2] == '1.':
                gamecnt+=1
                if gamecnt % games_per_file == 0:
                    filecnt+=1