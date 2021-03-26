import csv
import pandas as pd

csv.field_size_limit(500 * 1024 * 1024)

path = '/home/hadoop/data/new_csv/HosPrescriptionDetail.csv'
path01 = '/home/hadoop/data/new_csv/HosPrescriptionDetail01.csv'

df = pd.read_csv(path, dtype='object', error_bad_lines=False, engine='python',  chunksize=1000000)

