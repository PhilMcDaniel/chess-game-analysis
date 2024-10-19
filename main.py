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

source_file_name = support.full_path('sample.pgn')



def main():
    
   print(source_file_name)


if __name__ == "__main__":
    main()