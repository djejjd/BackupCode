import pandas as pd

path01 = '/home/hadoop/data/new_csv/HosRegister.csv'
path_PersonalRecord = '/home/hadoop/data/new_csv/PersonalRecord.csv'
path_Canton = '/home/hadoop/data/new_csv/Canton.csv'
path_HosCostMain = '/home/hadoop/data/new_csv/HosCostMain.csv'
path_DataDictionary = '/home/hadoop/data/new_csv/DataDictionary.csv'
path_HosPrescriptionDetail = '/home/hadoop/data/new_csv/HosPrescriptionDetail.csv'
path_DiagnoseTreatItem = '/home/hadoop/data/new_csv/DiagnoseTreatItem.csv'
path_DrugCatalog = '/home/hadoop/data/new_csv/DrugCatalog.csv'

# 大表路径
path = '/home/hadoop/data/test/test.csv'
path_2 = '/home/hadoop/data/new_csv/HosPrescriptionDetail.csv'

data01 = pd.read_csv(path_2, dtype='object', error_bad_lines=False, engine='python')
print(list(data01))
# size = 1000000
#
# data02 = pd.read_csv(path_HosPrescriptionDetail, dtype='object', error_bad_lines=False, na_values='NULL',
#                      engine='python', chunksize=size)
#
# csv.field_size_limit(500 * 1024 * 1024)


# def change(n):
#     if n[-2:] == '.0':
#         return n[:-2]
#     return n


# 修改列中值
# data02['DiseaseCode'] = data02['DiseaseCode'].astype(str)
# data02['DiseaseCode'] = data02['DiseaseCode'].apply(change)

# data02.loc[data02['Sex'] == '1', 'Sex'] = '男'
# data02.loc[data02['Sex'] == '2', 'Sex'] = '女'
# data02.loc[data02['Sex'] == '男性', 'Sex'] = '男'
# data02.loc[data02['Sex'] == '女性', 'Sex'] = '女'
# data02.to_csv(path)
# print(data02.DiseaseCode)

# 删除某一列
# print(data02.columns.values.tolist())
# data01.drop('OutHosDate', axis=1, inplace=True)
# data01.drop('InHosDate', axis=1, inplace=True)
# data01.to_csv(path, index=False)

# 去重复
data02.drop_duplicates(subset=['IDCardCode'], keep='first', inplace=True)
#
data01.set_index('IDCardCode', inplace=True)
data02.set_index('IDCardCode', inplace=True)
result = pd.merge(data01, data02, on='IDCardCode', how='left')
# result.to_csv(path_2)
