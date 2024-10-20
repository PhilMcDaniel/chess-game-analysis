import os
import logging
import pandas as pd


import pgn_game_parser as pgn
import split_file as split
import support as sup
import download_decompress as down_decomp

logging.basicConfig(level=logging.NOTSET)
#logging.disable()


#file_name = sup.full_path('sample.pgn')


@sup.measure_time
def main():
    
    #TODO: download file
    dd = down_decomp.DownloadDecompressFile()
    dd.download_file(url='https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.zst',filename='201301.pgn.zst')
    #TODO: decompress file
    file_name = dd.decompress_zst(input_filename='201301.pgn.zst', output_filename='201301.pgn')

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