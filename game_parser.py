import download_decompress as dd
import os

# https://database.lichess.org/
# https://database.lichess.org/standard/lichess_db_standard_rated_2018-06.pgn.bz2
dir = 'resources/'
url = 'https://database.lichess.org/standard/lichess_db_standard_rated_2015-02.pgn.bz2'
filename = dir+url[38:]


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
games = {'Games':0}
#read 1 line at a time
with open("resources/Jan2013.pgn",'r') as file:
    for line in file:
        line = file.readline()
        #maintain dict with counts of each opening
        if line[:8] == '[Opening':
            opening = line[10:-3].rstrip()
            #if key exists in dict, add 1 to count
            if opening in openings:
                openings.update({opening:openings[opening]+1})
            else:
            #add key to dict if it doesn't already exist
                openings[opening]=1

        #Count number of games. game line starts with "1."
        if line[:2] == '1.':
            games['Games'] +=1

        else:
            pass

games
openings

#parse and count the openings

#parse and count results by color
#parse and count results by rating
