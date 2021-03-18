import os
import pandas as pd

file_path = '/home/hadoop/data/school_data/data/test1'
files = os.listdir(file_path)

files = sorted(files, key=lambda i: len(i), reverse=False)
file_store = '/home/hadoop/data/school_data_csv/'
for file in files:
    path = file_path + '/' + file
    data = pd.read_csv(path, sep='\t', warn_bad_lines=False ,error_bad_lines= False)
    path_store = file_store + file[:-3] + 'csv'
    data.to_csv(path_store)

