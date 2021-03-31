import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def print_data(path01, path02):
    # data01 = spark.read.format('parquet').load(path01)
    data02 = spark.read.format('parquet').load(path02).dropDuplicates(subset=['HosRegisterCode'])
    # data01.where((data01['注射用胞磷胆碱钠'] != 0) | (data01['木瓜丸'] != 0) | \
    #              (data01['一次性鼻胃肠管'] != 0) | (data01['卡前列素氨丁三醇注射液'] != 0)).show(10)
    data02.where(data02.PersonalType == '17').show(50)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "100") \
        .getOrCreate()

    # 数据集
    path_data = 'hdfs://localhost:9000/data/data_tree'

    # 大表
    path_par = 'hdfs://localhost:9000/result/form_par'
    # 打印结果
    print_data(path_data, path_par)
