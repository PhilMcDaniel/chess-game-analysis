import download_decompress as dd
import os

url = 'https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.bz2'
filename = 'resources/Jan2013.pgn.bz2'

dd.download_file(url,filename)
dd.bz2_decompress(filename)

#delete original .pgn.bz2 file
dir = 'resources/'
for filename in os.listdir(dir):
    if filename.endswith(".bz2"):
        fullpath = os.path.join(dir,filename) 
        os.remove(fullpath)
        print(f"{fullpath} deleted")
    else:
        continue

#read all lines into list
with open("resources/Jan2013.pgn",'r') as file:
    lines = file.readlines()
    for line in lines[:18]:
        print(line.rstrip())
print(len(lines))

#read 1 line at a time
with open("resources/Jan2013.pgn",'r') as file:
    for i in range(18):
        line = file.readline()
        print(i)
        print(line.rstrip())

#parse and count the openings

#parse and count results by color
#parse and count results by rating
