import pandas as pd

path = '/home/hadoop/data/school_data/Join_PersonalRecord.txt'

data = pd.read_csv(path, sep='\t', dtype='object',
                   names=['PR_PersonalCode_Vc', 'PR_FamilyCode_Vc', 'PR_Name_Vc', 'PR_Gender_Vc', 'PR_Age_Int',
                          'PR_CantonCode_Ch', 'PR_PersonalType_Vc', 'PR_IDCardCode_Vc', 'PR_Brithday_Vc', 'PR_Folk_Vc',
                          'DT'])
path_store = '/home/hadoop/data/school_data_csv/Join_PersonalRecord.csv'
data.to_csv(path_store)

#
# 加表头
# path = '/home/hadoop/data/school_data_csv/Join_PersonalRecord.csv'
# df = pd.read_csv(path, header=None, dtype='object',
#                  names=['id', 'PR_PersonalCode_Vc', 'PR_FamilyCode_Vc', 'PR_Name_Vc', 'PR_Gender_Vc', 'PR_Age_Int',
#                         'PR_CantonCode_Ch', 'PR_PersonalType_Vc', 'PR_IDCardCode_Vc', 'PR_Brithday_Vc', 'PR_Folk_Vc',
#                         'DT'])
# df = pd.read_csv(path, header=None, dtype='object',
#                  names=['id', 'HR_HosRegisterCode_Vc', 'CantonCode_Ch', 'HR_FamilyCode_Vc', 'HR_PersonalCode_Vc',
#                         'HR_Name', 'HR_Sex', 'HR_Age', 'HR_CertificateCode_Vc', 'HR_InstitutionCode_Ch',
#                         'HR_DiseaseCode_Vc', 'HR_OperationCode_Vc', 'HR_InHosDate_Dt', 'HR_OutHosDate_Dt',
#                         'HR_RegisterDate', 'DT'])
# df.to_csv(path, index=False)
