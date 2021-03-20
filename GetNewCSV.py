import pandas as pd

path01 = '/home/hadoop/data/new_csv/HosRegister.csv'
path_PersonalRecord = '/home/hadoop/data/new_csv/PersonalRecord.csv'
path_Canton = '/home/hadoop/data/new_csv/Canton.csv'
path_HosCostMain = '/home/hadoop/data/new_csv/HosCostMain.csv'
path_DataDictionary = '/home/hadoop/data/new_csv/DataDictionary.csv'
# 大表路径
path = '/home/hadoop/data/test/test.csv'

data01 = pd.read_csv(path, dtype='object', error_bad_lines=False)
print(data01.shape[0])
# data02 = pd.read_csv(path_DataDictionary, dtype='object', error_bad_lines=False)

# 删除某一列
# data01.drop('PersonalTypeName_x', axis=1, inplace=True)
# data01.drop('PersonalTypeName_y', axis=1, inplace=True)
# data01.to_csv(path)

# 去重复
# data02.drop_duplicates(subset=['CantonCode'], keep='first', inplace=True)
#
# data01.set_index('IDCardCode', inplace=True)
# data02.set_index('IDCardCode', inplace=True)
# result = data01.join(data02, on='IDCardCode')
# result.to_csv(path)
