import os
import requests
import time
import zstandard as zstd
import logging
from support import measure_time

logging.basicConfig(level=logging.NOTSET)
#logging.disable()


class DownloadDecompressFile():
    """
    Class that handles downloading and decompressing files
    """
    ...
    
    def __init__(self):
        ...

    #download method
    @measure_time
    def download_file(self,url, filename):
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
        return self

    # decompress
    @measure_time
    def decompress_zst(self,input_filename,output_filename):
        '''
        Decompresses a .zst file
        '''
        #TODO: error handling if input_filename DNE
        #TODO: check if output_filename alreadyexists

        # Open the .zst file in binary mode
        logging.info(f"Starting decompression of: {input_filename}")
        with open(input_filename, 'rb') as compressed_file:
            dctx = zstd.ZstdDecompressor()
        
            # Open the output file and decompress into it
            with open(output_filename, 'wb') as decompressed_file:
                dctx.copy_stream(compressed_file, decompressed_file)
        logging.info(f"Decompress complete on {input_filename}:{output_filename}")
        return self