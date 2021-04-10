from hdfs.client import Client
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row

conf = SparkConf().setAppName("spark_example")
sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)


# 删除多余列
def drop_clos_data(data_all, drop_lists):
    for name in drop_lists:
        data_all = data_all.drop(name)
    return data_all


def get_union_data():
    # 读取文件
    data_HosRegister = hiveCtx.read.format('csv').option('header', 'true').load(path_HosRegister)
    data_HosCostMain = hiveCtx.read.format('csv').option('header', 'true').load(path_HosCostMain)
    data_Canton = hiveCtx.read.format('csv').option('header', 'true').load(path_Canton)
    data_DiagnoseTreatItem = hiveCtx.read.format('csv').option('header', 'true').load(
        path_DiagnoseTreatItem)
    data_DrugCatalog = hiveCtx.read.format('csv').option('header', 'true').load(path_DrugCatalog)
    data_PersonalRecord = hiveCtx.read.format('csv').option('header', 'true').load(path_PersonalRecord)
    data_HosPrescriptionDetail = hiveCtx.read.format('csv').option('header', 'true').load(path_HosPrescriptionDetail)
    data_DataDictionary = hiveCtx.read.format('csv').option('header', 'true').load(path_DataDictionary)

    # 删除无用列
    data_HosRegister = drop_clos_data(data_HosRegister, drop_clo_HosRegister)
    data_HosCostMain = drop_clos_data(data_HosCostMain, drop_clo_HosCostMain)
    data_Canton = drop_clos_data(data_Canton, drop_clo_Canton)
    data_DiagnoseTreatItem = drop_clos_data(data_DiagnoseTreatItem, drop_clo_DiagnoseTreatItem)
    data_DrugCatalog = drop_clos_data(data_DrugCatalog, drop_clo_DrugCatalog)
    data_PersonalRecord = drop_clos_data(data_PersonalRecord, drop_clo_PersonalRecord)
    data_HosPrescriptionDetail = drop_clos_data(data_HosPrescriptionDetail, drop_clo_HosPrescriptionDetail)

    # 合并HosRegister和HosCostMain表
    data = data_HosRegister.join(data_HosCostMain, on='HosRegisterCode', how="left_outer").dropDuplicates(
        subset=['HosRegisterCode'])
    # 合并PersonalRecord
    data_PersonalRecord = data_PersonalRecord.dropDuplicates(subset=['CertificateCode'])
    data = data.join(data_PersonalRecord, on='CertificateCode', how="left_outer").dropDuplicates(
        subset=['HosRegisterCode'])
    # 合并Canton
    data_Canton = data_Canton.dropDuplicates(subset=['CantonCode'])
    data = data.join(data_Canton, on='CantonCode', how="left_outer").dropDuplicates(subset=['HosRegisterCode'])

    # 合并DataDictionary
    # 提取U103-01指定人员类型
    data_dd = data_DataDictionary[(data_DataDictionary['Type'].isin('U103-01'))].dropDuplicates(
        subset=['Code', 'Desc']).drop('DT', 'Type')
    data_dd = data_dd.withColumnRenamed('Code', 'PersonalType')
    data = data.join(data_dd, on='PersonalType', how="left_outer").dropDuplicates(subset=['HosRegisterCode'])

    # 合并HosPrescriptionDetail
    data = data.join(data_HosPrescriptionDetail, on='HosRegisterCode', how='left_outer')
    # 添加药物和耗材的项目类型(ItemType_Name)以及费用类型(Expense_Type_Name)
    data_IN = data_DataDictionary[(data_DataDictionary['Type'].isin('U609-02'))].dropDuplicates(
        subset=['Code', 'Desc']).drop('DT', 'Type')
    data_IN = data_IN.withColumnRenamed('Code', 'ItemType')
    data_IN = data_IN.withColumnRenamed('Desc', 'ItemType_Name')
    data = data.join(data_IN, on='ItemType', how='left_outer')

    data_ETN = data_DataDictionary[(data_DataDictionary['Type'].isin('U608-04'))].dropDuplicates(
        subset=['Code', 'Desc']).drop('DT', 'Type')
    data_ETN = data_ETN.withColumnRenamed('Code', 'DrugCatalog')
    data_ETN = data_ETN.withColumnRenamed('Desc', 'Expense_Type_Name')
    data = data.join(data_ETN, on='DrugCatalog', how='left_outer')

    # 合并DiagnoseTreatItem和DrugCatalog并合并到大表中
    data_Drug_Diagnose = data_DrugCatalog.select('*').union(data_DiagnoseTreatItem.select('*'))
    data = data.join(data_Drug_Diagnose, on='ItemCode', how='left_outer')
    data.show(5)
    # 存储
    data.write.format('csv').option("header", "true").mode("overwrite").save(path_all)


if __name__ == '__main__':
    '''
        注意：
        先在hdfs中创建存储最终表的文件夹，然后授权可以进行读写
        hdfs dfs -chmod 777 /result/all_form
    '''
    # csv文件和最终合成的大表的存放的路径
    base_path = "hdfs://localhost:9000/csv/"
    path_all = "hdfs://localhost:9000/result/all_form"

    # 构造存放csv文件的路径
    path_HosRegister = base_path + 'Comp_HosRegister'
    path_HosCostMain = base_path + 'Comp_HosCostMain'
    path_Canton = base_path + 'Join_Canton'
    path_DiagnoseTreatItem = base_path + 'CFG_DiagnoseTreatItem'
    path_DrugCatalog = base_path + 'CFG_DrugCatalog'
    path_PersonalRecord = base_path + 'Join_PersonalRecord'
    path_HosPrescriptionDetail = base_path + 'Comp_HosPrescriptionDetail'
    path_DataDictionary = base_path + 'Global_DataDictionary'

    client = Client("http://192.168.191.82:50070", root="/", timeout=10000, session=False)

    # 要丢弃的列
    drop_clo_HosRegister = ['CantonCode', 'FamilyCode', 'PersonalCode', 'InstitutionCode', 'OperationCode']
    drop_clo_HosCostMain = ['SettlementDate', 'Ecbc', 'Zfdbbz', 'Mzbcje', 'DT']
    drop_clo_Canton = ['CantonName', 'ZoneCode', 'UpperCode', 'Level', 'DT']
    drop_clo_DiagnoseTreatItem = ['ItemCode_Vc', 'ItemBrevityCode', 'ItemType', 'CompRatio']
    drop_clo_DrugCatalog = ['DrugCode', 'DrugBrevityCode', 'OtherName', 'DosageTypeCode', 'DrugType', 'DrugCatalog',
                            'Usage', 'Memo', 'CatalogClass', 'CompRatio', 'UseDrugClass', 'RelativeCode', 'Country']
    drop_clo_PersonalRecord = ['PersonalCode', 'FamilyCode', 'Name', 'Gender', 'Age', 'Brithday', 'Folk', 'DT']
    drop_clo_HosPrescriptionDetail = ['PrescriptionCode', 'ItemIndex', 'DT']

    get_union_data()
