import os
import shutil
import pandas as pd
import glob
from multiprocessing import Pool
from distutils.dir_util import copy_tree

from postgresql_client import df_to_postgresql_bulk

# wrap your csv importer in a function that can be mapped

class CSV_Loader:
    def __init__(self, files_to_load_path, loaded_files_path, failure_files_path):
        self.files_to_load_path = files_to_load_path
        self.loaded_files_path = loaded_files_path
        self.failure_files_path = failure_files_path


    def read_csv(self, filename):
        'converts a filename to a pandas dataframe'
        try:
            return pd.read_csv(filename)
        except:
            old_file_name = filename
            new_file = filename.replace(self.files_to_load_path[:-1],self.failure_files_path[:-1])
            shutil.move(old_file_name , new_file)



    def delete_files_from_folder(self, folder, file_extention):
        filelist = [f for f in os.listdir(folder) if f.endswith(file_extention)]
        for f in filelist:
            os.remove(os.path.join(folder, f))

    def main(self):
        success = 0
        copy = 0
        # get a list of file names
        files = glob.glob(files_to_load_path+"*.csv")
        file_list = [filename for filename in files if filename.split('.')[1]=='csv']
        if len(file_list)>0:
            # set up your pool
            with Pool(processes=8) as pool: # or whatever your hardware can support

                # have your pool map the file names to dataframes

                df_list = pool.map(self.read_csv, file_list)

                # reduce the list of dataframes to a single dataframe
                if isinstance(df_list, list):
                    combined_df = pd.concat(df_list, ignore_index=True)
                else:
                    pass

                #insert data into postgresql: here you can add try except for loggin
                try:
                    if isinstance(combined_df, pd.DataFrame):
                        df_to_postgresql_bulk(combined_df, 'test', 'localhost', 'postgres', 'postgres', 'postgres', type='append')
                        success = 1
                    else:
                        pass
                except:
                    #insert log here
                    print("--")

                if success == 1:
                    try:
                        #insert log here
                        copy_tree(files_to_load_path[:-1], loaded_files_path[:-1])
                        copy = 1
                    except:
                        #log
                        pass

                if copy == 1:
                    try:
                        #insert log here TODO
                        self.delete_files_from_folder(files_to_load_path, ".csv")
                    except:
                        #insert log here
                        pass
        else:
            print("No Files")
            # insert log here TODO
if __name__ == '__main__':
    files_to_load_path = "C:/to_load_files/"
    loaded_files_path = "C:/loaded_files/"
    failure_files_path = "C:/failure_files/"
    csv_tran = CSV_Loader(files_to_load_path, loaded_files_path, failure_files_path)
    csv_tran.main()