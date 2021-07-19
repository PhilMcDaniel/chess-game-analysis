import os
import requests
import time
import bz2
import logging

logging.basicConfig(level=logging.NOTSET)
#logging.disable()

#download method
def download_file(url, filename):
    ''' Downloads file from the url and save it as filename '''
    # check if file already exists
    if not os.path.isfile(filename):
        logging.info('Downloading file')
        response = requests.get(url)
        # Check if the response is ok (200)
        if response.status_code == 200:
            # Open file and write the content
            with open(filename, 'wb') as file:
                # A chunk of 128 bytes
                for chunk in response:
                    file.write(chunk)
                time.sleep(1)
        logging.info("Download complete")
    else:
        logging.info('File exists')

# decompress
def bz2_decompress(filename):
    '''Decompresses a .bz2 file'''
    #only decompress file if the referenced file exists, and there isn't an existing non-.bs2 file in the dir
    if os.path.isfile(filename) and not os.path.isfile(filename[:-4]):
        #take off last 4 chars of filename (strip '.bz2')
        logging.info('Starting decompression')
        zipfile = bz2.BZ2File(filename)
        data = zipfile.read()
        newfilename = filename[:-4]
        open(newfilename, 'wb').write(data)
        logging.info('Decompression complete')
    elif os.path.isfile(filename[:-4]):
        logging.info('Decompressed file already exists')
    else:
        logging.info('File does not exist')