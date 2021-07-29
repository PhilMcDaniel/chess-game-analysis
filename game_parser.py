import download_decompress as dd
import os
import pandas as pd
import time
import logging
import matplotlib.pyplot as plt
import matplotlib as mpl


logging.basicConfig(level=logging.NOTSET)
#logging.disable()

# https://database.lichess.org/

start = time.perf_counter()

#make dir to hold downloads
dir = 'Downloads'
if not os.path.exists(dir):
    os.mkdir(dir)

# load source files into list
sources = []
with open('pgn_source.txt','r') as file:
    for line in file:
        sources.append(line.strip())


# totals across multiple files
openings = {}
total_games = 0
total_lines = 0

for source in reversed(sources):
    
    loopstart = time.perf_counter()

    filename = os.path.join(dir,source[38:])
    # filename after it is decompressed (remove .bz2)
    decomp_filename = filename[:-4]
    yyyymm = filename[-15:-8] # '2013-01'
    yyyymm = yyyymm[:4]+yyyymm[5:7] # 201301
        
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
            
    logging.info(f"File lines: {file_lines}, File games: {file_games}")
    logging.info(f"Total lines: {total_lines},Total games: {total_games}")

    # delete .pgn & .bz2 files
    for filename in os.listdir(dir):
        if filename.endswith(".bz2") or filename.endswith(".pgn"):
            fullpath = os.path.join(dir,filename) 
            os.remove(fullpath)
            logging.info(f"{fullpath} deleted")
        else:
            continue
    
    loopend = time.perf_counter()
    logging.info(f"{yyyymm} Execution time: {round((loopend - loopstart),2)} seconds")

    # get openings into dataframe
    df = pd.DataFrame.from_dict(openings, orient='index').reset_index()
    df = df.rename(columns={"index":"Opening"})

    # turn columns for YYYYMM values into rows
    dfraw = df.melt(id_vars = ['Opening'],value_vars=df.columns,var_name="YYYYMM",value_name="Count")
    #dfraw

    # if csv exists load and combine with above dataframe
    if os.path.isfile('openings_by_yyyymm.csv'):
        olddf = pd.read_csv('openings_by_yyyymm.csv',sep=',')
        combined_raw = pd.concat([dfraw,olddf])
    else:
        combined_raw = dfraw

    # put opening by yyyymm into csv
    combined_raw.to_csv('openings_by_yyyymm.csv', sep=',', encoding='utf-8',index=False)

    # add processed urls to text file to track
    with open("processed_files.txt", "a") as processed_file:
        processed_file.write(f"{source}\n")

# openings
end = time.perf_counter()
logging.info(f"Total execution time: {round((end - start),2)} seconds")


#agregate to view top overall openings
dfaggregate = combined_raw.groupby(['Opening']).sum().reset_index().sort_values(by=["Count"],ascending=False)
dfaggregate
#plot top 100 openings
ax = dfaggregate.head(50).plot.barh(x='Opening',y='Count')
#invert so that y axis descends
ax.invert_yaxis()
#update so that scientific notation is not used
ax.ticklabel_format(style='plain',axis='x')
#add commas to x axis
ax.get_xaxis().set_major_formatter(
    mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

plt.show()