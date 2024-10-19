import os

class Support():
    ...



    def __init__(self):
        ...


    def full_path(self, file_name):
        """
        
        Returns the full file path from the file_name
        Parameters:
            file_name - The file name of the pg
        Returns:
            full_path - The full path of the file
        Raises:
            Exception - If the file does not exist at the expected location

        """
        
        # make sure the file exists in the expected location
        # should exist down one level in the data folder


        #TODO create data folder if it doesn't exist
    
        expected_dir = os.path.join(os.getcwd(), 'data')
        full_path = os.path.join(expected_dir, file_name) 
        if os.path.isfile(full_path):
            return full_path
        else:
            raise Exception(f"File does not exist at the expected location:{full_path}")