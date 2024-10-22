import os
import logging
import pandas as pd


import pgn_game_parser as pgn
import split_file as split
import support as sup
import download_decompress as down_decomp

logging.basicConfig(level=logging.NOTSET)
#logging.disable()



pgn_url = 'https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.zst'

@sup.measure_time
def main():
    
    # download file
    dd = down_decomp.DownloadDecompressFile()
    
    downloaded_file = dd.download_file(url=pgn_url)
    # decompress file
    decompressed_file_name = dd.decompress_zst(input_filename=dd.downloaded_filename)

    file = pgn.PGNS()
    # parse .pgn to dict of games
    games = file.parse_pgn_to_dict(decompressed_file_name)
    # turn dict of games into pddf
    games_pddf = file.dict_to_pddf(games)
    # write pddf to .parquet
    file.pddf_to_parquet(games_pddf,decompressed_file_name)
    
    #TODO upload to cloud

    #print single row for debugging
    print(games['https://lichess.org/szom2tog'])

if __name__ == "__main__":
    main()