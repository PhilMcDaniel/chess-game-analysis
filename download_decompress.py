import os
import requests
import time
import bz2

#download method
def download_file(url, filename):
    ''' Downloads file from the url and save it as filename '''
    # check if file already exists
    if not os.path.isfile(filename):
        print('Downloading file')
        response = requests.get(url)
        # Check if the response is ok (200)
        if response.status_code == 200:
            # Open file and write the content
            with open(filename, 'wb') as file:
                # A chunk of 128 bytes
                for chunk in response:
                    file.write(chunk)
                time.sleep(1)
        print("Download complete")
    else:
        print('File exists')

# decompress
def bz2_decompress(filename):
    '''Decompresses a .bz2 file'''
    #only decompress file if the referenced file exists, and there isn't an existing non-.bs2 file in the dir
    if os.path.isfile(filename) and not os.path.isfile(filename[:-4]):
        #take off last 4 chars of filename (strip '.bz2')
        print('Starting decompression')
        zipfile = bz2.BZ2File(filename)
        data = zipfile.read()
        newfilename = filename[:-4]
        open(newfilename, 'wb').write(data)
        print('Decompression complete')
    elif os.path.isfile(filename[:-4]):
        print('Decompressed file already exists')
    else:
        print('File does not exist')