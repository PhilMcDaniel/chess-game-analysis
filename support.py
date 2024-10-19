import os

class Support():
    ...



    def __init__(self):
        ...


    def full_path(self, filename):
        """
        
        Returns the full file path from the filename
        Parameters:
            filename - The filename of the pg
        Returns:
            full_path - The full path of the file
        Raises:
            Exception - If the file does not exist at the expected location

        """
        
        # make sure the file exists in the expected location
        # should exist done one level in the Downloads folder


        #TODO create data folder if it doesn't exist
    
        expected_dir = os.path.join(os.getcwd(), 'data')
        full_path = os.path.join(expected_dir, filename) 
        if os.path.isfile(full_path):
            return full_path
        else:
            raise Exception(f"File does not exist at the expected location:{full_path}")