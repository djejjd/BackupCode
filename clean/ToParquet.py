from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
import pyspark.sql.functions as F


# 修改年龄
def getNewAge(path, path1):
    # 150221 1940 0212472x
    data = spark.read.format('csv').option('header', 'true').load(path)

    data = data.withColumn("Born", F.split('CertificateCode', '\d{7}.$')) \
        .withColumn('Born', F.concat_ws("", "Born")) \
        .withColumn('Born', F.split('Born', '\d{6}')) \
        .withColumn('Born', F.concat_ws("", "Born"))

    data = data.withColumn("Age", F.when((data.DT - data.Born) != data.Age, data.DT - data.Born).otherwise(data.Age)) \
        .drop('Born')
    data.show()
    data.write.format('parquet').mode("overwrite").save(path1)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "100") \
        .getOrCreate()

    path_csv = 'hdfs://localhost:9000/result/cleaned_form'

    path_par = 'hdfs://localhost:9000/result/form_par'
    getNewAge(path_csv, path_par)
    # data_csv = spark.read.format('csv').option('header', 'true').load(path_csv)
    # data_csv.write.format('parquet').mode("overwrite").save(path_par)
