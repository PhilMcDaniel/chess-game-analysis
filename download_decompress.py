import os
import requests
import time
import zstandard as zstd
import logging
from support import measure_time,full_path

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
        """ 
        Downloads file from the url and save it as filename 
        """
        filename = full_path(filename)
        # check if file already exists
        if not os.path.isfile(filename):
            logging.info(f'Downloading file:{filename}')
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
            logging.info('Downloaded file already exists')
        return self

    # decompress
    @measure_time
    def decompress_zst(self,input_filename,output_filename):
        """
        Decompresses a .zst file
        """
        input_filename = full_path(input_filename)
        output_filename = full_path(output_filename)

        # raise exception if input_filename dne
        if not os.path.isfile(input_filename):
            raise Exception(f"Downloaded file does not exist: {input_filename}")

        # only decompress if the output_filename doesn't exist
        if not os.path.isfile(output_filename):
            # Open the .zst file in binary mode
            logging.info(f"Starting decompression of: {input_filename}")
            with open(input_filename, 'rb') as compressed_file:
                dctx = zstd.ZstdDecompressor()
            
                # Open the output file and decompress into it
                with open(output_filename, 'wb') as decompressed_file:
                    dctx.copy_stream(compressed_file, decompressed_file)
            logging.info(f"Decompress complete on {input_filename} to {output_filename}")
        else:
            logging.info(f"Decompressed file already exists")
        return output_filename