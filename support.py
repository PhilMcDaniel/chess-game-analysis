import os
import time
import logging

logging.basicConfig(level=logging.NOTSET)

def full_path(file_name):
    """
    Returns the full file path from the file_name
    Parameters:
        file_name - The file name of the pg
    Returns:
        full_path - The full path of the file
    Raises:
        Exception - If the file does not exist at the expected location
    """
    
    #should exist down one level in the data folder
    expected_dir = os.path.join(os.getcwd(), 'data')
    #if doesn't exist, create the folder
    os.makedirs(expected_dir, exist_ok=True)
    full_path = os.path.join(expected_dir, file_name) 
    
    #raise an error if the file doesn't exist
    if os.path.isfile(full_path):
        return full_path
    else:
        raise Exception(f"File does not exist at the expected location:{full_path}")
    
def measure_time(func):
    """
    Function to be used as a decorator to measure the duration of other functions
    """
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        logging.info(f"{func.__name__} took {end_time - start_time:.4f} seconds to run.")
        return result
    return wrapper