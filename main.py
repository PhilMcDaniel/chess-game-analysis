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



def main():
    
    file = pgn.PGNS()
    file.parse_pgn_to_dict(file_name)



if __name__ == "__main__":
    main()