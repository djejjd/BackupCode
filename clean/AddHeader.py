import os
from hdfs.client import Client
import pyarrow as pa
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
from pyspark.sql.functions import split


# 将txt文件转为csv并加表头
def add_header(path_file, name_lists):
    # 获得hdfs中txt文件的路径以及文件名
    files = client.list(path_file, status=False)
    files = sorted(files, key=lambda i: len(i), reverse=False)
    i = 0
    for file in files:
        # 构造读取txt文件的path路径
        file_txt = "hdfs://localhost:9000" + path_file + "/" + file
        df = hiveCtx.read.text(file_txt)
        # 获得每个csv表的表头
        list_name = name_lists[i]
        # 给每个列名分配序号用于后面添加表名
        df_split = df.withColumn("value", split(df['value'], '\t'))
        # 将txt文件的每一行按照\t进行划分并以list形式存放
        data = sc.parallelize([list_name[i] for i in range(len(list_name))]).zipWithIndex().collect()
        # 添加表名并按照表名将列分割开
        for name, index in data:
            df_split = df_split.withColumn(name, df_split['value'].getItem(index))
        # 丢弃原来的value列
        df_split = df_split.drop(df_split['value'])
        # 构造存储路线
        file_save = "hdfs://localhost:9000/csv/" + file[:-4]
        df_split.write.format('csv').option("header", "true").mode("overwrite").save(file_save)
        # file_save = "hdfs://localhost:9000/test/"+file[:-4]
        # df_split.write.format("parquet").mode("overwrite").save(file_save)
        i += 1


if __name__ == '__main__':
    conf = SparkConf().setAppName("spark_example")
    sc = SparkContext(conf=conf)
    hiveCtx = HiveContext(sc)
    path = "/txt"
    # 连接hdfs
    client = Client("http://192.168.191.82:50070", root="/", timeout=10000, session=False)
    # 表头
    name_lists = [
        ['Ctn_CantonCode_Ch', 'Ctn_CantonName_Vc', 'Ctn_ZoneCode_Ch', 'Ctn_UpperCode_Ch', 'Ctn_Level_Ch',
         'Ctn_AllName_Vc',
         'DT'],
        ['DC_DrugCode_Vc', 'DC_DrugName_Vc', 'DC_DrugBrevityCode_Vc', 'DC_OtherName_Vc', 'DC_DosageTypeCode_Vc',
         'DC_DrugType_Vc', 'DC_DrugCatalog_Vc', 'DC_Usage_Vc', 'DC_Memo_Vc', 'DC_CatalogClass_Vc', 'DC_CompRatio_Dec',
         'DC_CompRatio_Type', 'DC_UseDrugClass_Vc', 'DC_RelativeCode_Vc', 'DC_DrugCode_Xt', 'Con_Country_Tag'],
        ['HCM_HosRegisterCode_Vc', 'HCM_TotalFee_Dec', 'HCM_RealComp_Dec', 'HCM_SelfPay_Dec', 'HCM_SettlementDate_Dt',
         'HCM_Ecbc_dec', 'HCM_Zfdbbz_dec', 'HCM_Mzbcje_dec', 'DT'],
        ['HR_HosRegisterCode_Vc', 'CantonCode_Ch', 'HR_FamilyCode_Vc', 'HR_PersonalCode_Vc',
         'HR_Name', 'HR_Sex', 'HR_Age', 'HR_CertificateCode_Vc', 'HR_InstitutionCode_Ch',
         'HR_DiseaseCode_Vc', 'HR_OperationCode_Vc', 'HR_InHosDate_Dt', 'HR_OutHosDate_Dt',
         'HR_RegisterDate', 'DT'],
        ['PR_PersonalCode_Vc', 'PR_FamilyCode_Vc', 'PR_Name_Vc', 'PR_Gender_Vc', 'PR_Age_Int',
         'PR_CantonCode_Ch', 'PR_PersonalType_Vc', 'PR_IDCardCode_Vc', 'PR_Brithday_Vc', 'PR_Folk_Vc',
         'DT'],
        ['DT_ItemCode_Vc',
         'DT_ItemName_Vc',
         'DT_ItemBrevityCode_Vc',
         'DT_ItemType_Vc',
         'DT_CompRatio_Dec',
         'DT_CompRatio_Type',
         'DT_RelativeCode_Vc'],
        ['Dictionary_Type',
         'Dictionary_Code',
         'Dictionary_Desc',
         'DT'],
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
    add_header(path, name_lists)

'''
data = df.withColumn('Ctn_CantonCode_Ch', df['value'].getItem('Ctn_CantonCode_Ch'))\
                 .withColumn('Ctn_CantonName_Vc', df['value'].getItem('Ctn_CantonName_Vc'))\
                 .withColumn('Ctn_ZoneCode_Ch', df['value'].getItem('Ctn_ZoneCode_Ch'))\
                 .withColumn('Ctn_UpperCode_Ch', df['value'].getItem('Ctn_UpperCode_Ch'))\
                 .withColumn('Ctn_Level_Ch', df['value'].getItem('Ctn_Level_Ch'))\
                 .withColumn('Ctn_AllName_Vc', df['value'].getItem('Ctn_AllName_Vc'))\
                 .withColumn('DT', df['value'].getItem('DT'))'''
