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


file_name = sup.full_path('sample.pgn')


@sup.measure_time
def main():
    
    #TODO: download file
    #TODO: decompress file

    file = pgn.PGNS()
    # parse .pgn to dict of games
    games = file.parse_pgn_to_dict(file_name)
    # turn dict of games into pddf
    games_pddf = file.dict_to_pddf(games)
    print(games['https://lichess.org/szom2tog'])
    # write pddf to .parquet
    file.pddf_to_parquet(games_pddf,file_name)
    
    #TODO upload to cloud

if __name__ == "__main__":
    main()

