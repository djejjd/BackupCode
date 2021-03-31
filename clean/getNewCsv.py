from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType


def getCanton(path):
    data = spark.read.format('csv').option('header', 'true') \
        .load(path) \
        .dropDuplicates(subset=['HosRegisterCode'])
    data = data.withColumn('Age', )


def getData(path):
    data = spark.read.format('parquet').load(path)
    # data = data.where((data.Age == '2017') | (data.Age == '男')).dropDuplicates(subset=['HosRegisterCode'])
    # print(data.count())
    data.show()

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "1000") \
        .getOrCreate()

    inputFile = 'hdfs://localhost:9000/csv/Comp_HosRegister'
    inputFile2 = 'hdfs://localhost:9000/result/form_par'
    # getCanton(inputFile)
    getData(inputFile2)
