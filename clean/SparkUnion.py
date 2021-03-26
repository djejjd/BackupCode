import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row

conf = SparkConf().setAppName("spark_example")
sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)

path = '/home/hadoop/data/school_data_csv/test.csv'
path_HosRegister = '/home/hadoop/data/new_csv/HosRegister.csv'
path_PersonalRecord = '/home/hadoop/data/new_csv/PersonalRecord.csv'
path_Canton = '/home/hadoop/data/new_csv/Canton.csv'
path_HosCostMain = '/home/hadoop/data/new_csv/HosCostMain.csv'
path_DataDictionary = '/home/hadoop/data/new_csv/DataDictionary.csv'
path_HosPrescriptionDetail = '/home/hadoop/data/new_csv/HosPrescriptionDetail.csv'
path_DiagnoseTreatItem = '/home/hadoop/data/new_csv/DiagnoseTreatItem.csv'
path_DrugCatalog = '/home/hadoop/data/new_csv/DrugCatalog.csv'


def get_union_data():
    data_HosRegister = hiveCtx.read.csv(path_HosRegister, header=True)
    data_PersonalRecord = hiveCtx.read.csv(path_PersonalRecord, header=True)

    # 合并HosRegister和PersonalRecord
    data = data_HosRegister.join(data_PersonalRecord, on="IDCardCode", how="left_outer").dropDuplicates(
        subset=['HosRegisterCode'])

    # 合并Canton
    data_Canton = hiveCtx.read.csv(path_Canton, header=True)
    data = data.join(data_Canton, on="CantonCode", how="left_outer").dropDuplicates(subset=['HosRegisterCode'])

    # 合并HosCostMain
    data_HosCostMain = hiveCtx.read.csv(path_HosCostMain, header=True)
    data = data.join(data_HosCostMain, on="HosRegisterCode", how="left_outer").dropDuplicates(
        subset=['HosRegisterCode'])

    # 合并DataDictionary
    data_DataDictionary = hiveCtx.read.csv(path_DataDictionary, header=True)
    data = data.join(data_DataDictionary, on="PersonalType", how="left_outer").dropDuplicates(
        subset=['HosRegisterCode'])

    data.toPandas().to_csv(path, header=True, index=False)


def get_join_data():
    data_HosPrescriptionDetail = hiveCtx.read.csv(path_HosPrescriptionDetail, header=True)
    data_DiagnoseTreatItem = hiveCtx.read.csv(path_DiagnoseTreatItem, header=True)
    data_DrugCatalog = hiveCtx.read.csv(path_DrugCatalog, header=True)

    path01 = '/home/hadoop/data/school_data_csv/test02'
    # 合并HosPrescriptionDetail、DrugCatalog、DiagnoseTreatItem
    data = data_HosPrescriptionDetail.join(data_DiagnoseTreatItem, on="ItemCode", how="left_outer")
    data = data_HosPrescriptionDetail.join(data_DrugCatalog, on="ItemCode", how="left_outer")

    # 速度慢
    data.coalesce(2).write.mode("overwrite").option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").option(
        "header", "true").option("name", "test02").csv(path01)

    # print(data.count())
    # data.toPandas().to_csv(path01, header=True, index=False)


# 合并最后两张大表
def get_union_all():
    path = '/home/hadoop/data/school_data_csv/test.csv'
    path01 = '/home/hadoop/data/school_data_csv/test01.csv'
    path02 = '/home/hadoop/data/school_data_csv/test03'
    data = hiveCtx.read.csv(path, header=True)
    data01 = hiveCtx.read.csv(path01, header=True)
    all_data = data.join(data01, on="HosRegisterCode", how="left_outer")
    # all_data.show(10000)
    # print(all_data.count())
    all_data.coalesce(5).write.mode("overwrite").option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").option(
        "header", "true").csv(path02)


def get_count():
    path = '/home/hadoop/data/school_data_csv/test03/part-00000-89a8fda1-9f29-4c48-8d8a-8ac5717f6314-c000.csv'
    data = hiveCtx.read.csv(path, header=True)
    # data.show(1000)
    # print(data.count())


if __name__ == '__main__':
    # get_join_data()
    get_union_all()
    # get_count()