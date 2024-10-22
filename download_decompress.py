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
    def download_file(self,url):
        """ 
        Downloads file from the url and save it to the ./data/ directory 
        """
        self.downloaded_filename = full_path(url.rsplit('/', 1)[1])
        # check if file already exists
        if not os.path.isfile(self.downloaded_filename):
            logging.info(f'Downloading file: {self.downloaded_filename}')
            response = requests.get(url)
            # Check if the response is ok (200)
            if response.status_code == 200:
                # Open file and write the content
                with open(self.downloaded_filename, 'wb') as file:
                    # A chunk of 128 bytes
                    for chunk in response:
                        file.write(chunk)
                    time.sleep(1)
            logging.info(f"Download complete: {self.downloaded_filename}")
        else:
            logging.info(f'Downloaded file: {self.downloaded_filename} already exists')
        
        compressed_file_size = round((os.path.getsize(self.downloaded_filename) / (1024 ** 2)),2)
        logging.info(f"Downloaded file size: {compressed_file_size} MB")
        
        return self.downloaded_filename

    # decompress
    @measure_time
    def decompress_zst(self,input_filename):
        """
        Decompresses a .zst file from the ./data/ directory and saves as a .pgn
        """
        self.input_filename = full_path(input_filename)
        self.decompressed_filename = self.input_filename.removesuffix('.zst')

        # raise exception if input_filename dne
        if not os.path.isfile(self.input_filename):
            raise Exception(f"Downloaded file does not exist: {self.input_filename}")

        # only decompress if the output_filename doesn't exist
        if not os.path.isfile(self.decompressed_filename):
            # Open the .zst file in binary mode
            logging.info(f"Starting decompression of: {self.input_filename}")
            with open(self.input_filename, 'rb') as compressed_file:
                dctx = zstd.ZstdDecompressor()
            
                # Open the output file and decompress into it
                with open(self.decompressed_filename, 'wb') as decompressed_file:
                    dctx.copy_stream(compressed_file, decompressed_file)
            logging.info(f"Decompress complete on {self.input_filename} to {self.decompressed_filename}")
        else:
            logging.info(f"Decompressed file: {self.decompressed_filename} already exists")
        
        decompressed_file_size = round((os.path.getsize(self.decompressed_filename) / (1024 ** 2)),2)
        logging.info(f"Decompressed file size: {decompressed_file_size} MB")
        
        return self.decompressed_filename