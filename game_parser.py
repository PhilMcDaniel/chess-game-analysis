import download_decompress as dd
import os
import pandas as pd

# https://database.lichess.org/
# https://database.lichess.org/standard/lichess_db_standard_rated_2018-06.pgn.bz2
dir = 'resources/'
url = 'https://database.lichess.org/standard/lichess_db_standard_rated_2013-07.pgn.bz2'
filename = dir+url[38:]
decomp_filename = filename[:-4]

dd.download_file(url,filename)
dd.bz2_decompress(filename)

#delete original .pgn.bz2 file
for filename in os.listdir(dir):
    if filename.endswith(".bz2"):
        fullpath = os.path.join(dir,filename) 
        os.remove(fullpath)
        print(f"{fullpath} deleted")
    else:
        continue

openings = dict()
games = 0
total_lines = 0
#read 1 line at a time
with open(decomp_filename,'r') as file:
    for line in file:
        line = line.strip()
        total_lines += 1
        #maintain dict with counts of each opening
        if line[:8] == '[Opening':
            opening = line[10:-2]
            #if key exists in dict, add 1 to count
            if opening in openings:
                openings.update({opening:openings[opening]+1})
            else:
            #add key to dict if it doesn't already exist
                openings[opening]=1

        #Count number of games. Using each occurance of "[Result"
        if line[:7] == '[Result':
            games +=1
        else:
            pass
    file.close()

#total_lines
#games
#openings

#parse and count the openings
openings_df = pd.DataFrame.from_dict(openings,orient='index',columns=['Count'])
openings_df.head(10)
openings_df.sort_values(by='Count',ascending=False)
#parse and count results by color
#parse and count results by rating
