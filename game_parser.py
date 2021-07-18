import download_decompress as dd
import os
import pandas as pd

# https://database.lichess.org/

# load source files into list
sources = []
with open('pgn_source.txt','r') as file:
    for line in file:
        sources.append(line.strip())


dir = 'resources/'

# totals across multiple files
openings = {}
total_games = 0
total_lines = 0

for source in reversed(sources):
    filename = dir+source[38:]
    # filename after it is decompressed (remove .bz2)
    decomp_filename = filename[:-4]
    yyyymm = filename[-15:-8]
        
    # download and decompress
    dd.download_file(source,filename)
    dd.bz2_decompress(filename)

    # read source .pgn file line by line
    with open(decomp_filename,'r') as file:
        file_games = 0
        file_lines = 0
        for line in file:
            line = line.strip()
            file_lines += 1
            total_lines += 1
            # maintain dict with counts of each opening
            if line[:8] == '[Opening':
                opening = line[10:-2]
                if opening in openings.keys() and yyyymm in openings[opening].keys():
                    openings[opening][yyyymm] = openings[opening][yyyymm]+1
                elif opening in openings.keys() and yyyymm not in openings[opening].keys():
                    openings[opening][yyyymm] = 1
                else:
                    openings[opening] = {yyyymm:1}
            # Count number of games. Using each occurance of "[Result"
            if line[:7] == '[Result':
                file_games += 1
                total_games += 1
            else:
                pass
            
    print(f"File lines: {file_lines}, File games: {file_games}")
    print(f"Total lines: {total_lines},Total games: {total_games}")

    # delete .pgn & .bz2 files
    for filename in os.listdir(dir):
        if filename.endswith(".bz2") or filename.endswith(".pgn"):
            fullpath = os.path.join(dir,filename) 
            os.remove(fullpath)
            print(f"{fullpath} deleted")
        else:
            continue

# openings

# get openings into dataframe
