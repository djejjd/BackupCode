import os
import pandas as pd

file_path = '/home/hadoop/data/school_data'
files = os.listdir(file_path)
name_lists = [
    ['Ctn_CantonCode_Ch', 'Ctn_CantonName_Vc', 'Ctn_ZoneCode_Ch', 'Ctn_UpperCode_Ch', 'Ctn_Level_Ch', 'Ctn_AllName_Vc',
     'DT'],
    ['DC_DrugCode_Vc', 'DC_DrugName_Vc', 'DC_DrugBrevityCode_Vc', 'DC_OtherName_Vc', 'DC_DosageTypeCode_Vc',
     'DC_DrugType_Vc', 'DC_DrugCatalog_Vc', 'DC_Usage_Vc', 'DC_Memo_Vc', 'DC_CatalogClass_Vc', 'DC_CompRatio_Dec',
     'DC_CompRatio_Type', 'DC_UseDrugClass_Vc', 'DC_RelativeCode_Vc', 'DC_DrugCode_Xt', 'Con_Country_Tag'],
    ['HR_HosRegisterCode_Vc', 'CantonCode_Ch', 'HR_FamilyCode_Vc', 'HR_PersonalCode_Vc',
     'HR_Name', 'HR_Sex', 'HR_Age', 'HR_CertificateCode_Vc', 'HR_InstitutionCode_Ch',
     'HR_DiseaseCode_Vc', 'HR_OperationCode_Vc', 'HR_InHosDate_Dt', 'HR_OutHosDate_Dt',
     'HR_RegisterDate', 'DT'],
    ['HCM_HosRegisterCode_Vc', 'HCM_TotalFee_Dec', 'HCM_RealComp_Dec', 'HCM_SelfPay_Dec', 'HCM_SettlementDate_Dt',
     'HCM_Ecbc_dec', 'HCM_Zfdbbz_dec', 'HCM_Mzbcje_dec', 'DT'],
    ['PR_PersonalCode_Vc', 'PR_FamilyCode_Vc', 'PR_Name_Vc', 'PR_Gender_Vc', 'PR_Age_Int',
     'PR_CantonCode_Ch', 'PR_PersonalType_Vc', 'PR_IDCardCode_Vc', 'PR_Brithday_Vc', 'PR_Folk_Vc',
     'DT'],
    ['Dictionary_Type',
     'Dictionary_Code',
     'Dictionary_Desc',
     'DT'],
    ['DT_ItemCode_Vc',
     'DT_ItemName_Vc',
     'DT_ItemBrevityCode_Vc',
     'DT_ItemType_Vc',
     'DT_CompRatio_Dec',
     'DT_CompRatio_Type',
     'DT_RelativeCode_Vc'],
    ['HP_HosRegisterCode_Vc',
     'HPD_PrescriptionCode_Vc',
     'HPD_ItemIndex_int',
     'HPD_ItemCode_Vc',
     'HPD_ItemName_Vc',
     'HPD_ItemType_Vc',
     'HPD_DrugCatalog_Vc',
     'HPD_Count_Dec',
     'HPD_FeeSum_Dec',
     'HPD_AllowedComp_Dec',
     'HPD_UnallowedComp_Dec',
     'HPD_CompRatio_Dec',
     'DT']]
files = sorted(files, key=lambda i: len(i), reverse=False)
file_store = '/home/hadoop/data/school_data_csv/'
i = 0
name = []
for file in files:
    path = file_path + '/' + file
    name = name_lists[i]
    data = pd.read_csv(path, sep='\t', dtype='object', warn_bad_lines=False, error_bad_lines=False, names=name)
    path_store = file_store + file[:-3] + 'csv'
    data.to_csv(path_store)
    i = i + 1
