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

file_name = support.full_path('sample.pgn')



def main():
    
    
    line_num = 0
    with open(file_name,'r') as file:
        for line in file:
            line = line.strip()
            line_num += 1
    print(f"file: {file_name} has {line_num} lines.")



if __name__ == "__main__":
    main()