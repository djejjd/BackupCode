import time
import pandas as pd
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row

spark = SparkSession.builder \
    .master("local") \
    .appName("example") \
    .config("spark.debug.maxToStringFields", "100") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.default.parallelism", "600") \
    .config("spark.sql.auto.repartition", "true") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

starttime = time.time()

inputFile01 = 'hdfs://localhost:9000/result/form_par'
inputs01 = spark.read.format('parquet').load(inputFile01)
inputs01.createOrReplaceTempView("tweets01")
endtime = time.time()
print("1: ", endtime - starttime)
# testDF['Age'], testDF['Sex'], testDF['HosRegisterCode']
# testDF["CertificateCode"], testDF['Desc'], testDF['AllName'], testDF["Name"],
starttime = time.time()
testDF = spark.sql(
    """SELECT CertificateCode, Desc, AllName, Name, Age, Sex, HosRegisterCode FROM tweets01 WHERE tweets01.Name= '柳三女'""") \
    .withColumn("id", F.monotonically_increasing_id())
endtime = time.time()
print("2: ", endtime - starttime)

start = time.time()
testDF = testDF.select('*').where((testDF.id >= 0) & (testDF.id < 20))
endt = time.time()
print('lll: ', endt - start)

start = time.time()
ddl = testDF.toPandas()
end = time.time()
print("ddl: ", end - start)

start = time.time()

list_persons = map(lambda row: row.asDict(), testDF.collect())

end = time.time()
print(type(list_persons))
print(list_persons)
print("ddl: ", end - start)

# starttime = time.time()
json_list = []
for a, b, c, d, e, f, g in zip(testDF["CertificateCode"], testDF['Desc'], testDF['AllName'], testDF["Name"], testDF['Age'], testDF['Sex'], testDF['HosRegisterCode']):
    json_dict = {'CertificateCode': a, 'Desc': b, 'AllName': c, 'Name': d, 'Age': e, 'Sex': f,
                 'HosRegisterCode': g}
    json_list.append(json_dict)

print(type(json_list))
print(json_list)
# endtime = time.time()
# print("3: ", endtime - starttime)
# print(json_list)
# print(tst)
