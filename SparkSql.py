from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext

# 利用Spark Sql从hdfs中进行查询
# 单表查询
'''
conf = SparkConf().setMaster("local").setAppName("First_App")
sc = SparkContext(conf=conf)

sqlc = SQLContext(sc)

path01 = 'hdfs://localhost:9000/user/hadoop/csv/Comp_HosRegister.csv'
path02 = 'hdfs://localhost:9000/user/hadoop/csv/Join_PersonalRecord.csv'
Join_PersonalRecord= sqlc.read.csv(path01)
Comp_HosRegister= sqlc.read.csv(path02)
Join_PersonalRecord.show(10)
# Join_PersonalRecord.select("_c1", "_c3", "_c4", "_c5").where("_c3 = '张菊华'").show()
'''

# 多表查询
conf = SparkConf().setMaster("local").setAppName("First_App")
spark = SparkSession.builder.appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

path02 = "/home/hadoop/data/school_data_csv/Join_PersonalRecord.csv"
path01 = 'hdfs://localhost:9000/user/hadoop/csv/Comp_HosRegister.csv'
# path02 = 'hdfs://localhost:9000/user/hadoop/csv/Join_PersonalRecord.csv'
Join_PersonalRecord = spark.read.csv(path01, header=True)
Comp_HosRegister = spark.read.csv(path02, header=True)

Join_PersonalRecord.createOrReplaceTempView("test01")
Comp_HosRegister.createOrReplaceTempView("test02")
#
# testDF = spark.sql("SELECT * FROM test01 where test01.HR_Name='柳三女'")
testDF01 = spark.sql("select * from test02")
# testDF02 = spark.sql(
    # "select test01.HR_PersonalCode_Vc, test02.PR_Name_Vc from test01, test02 where test01.HR_PersonalCode_Vc = test02.PR_PersonalCode_Vc")
# testDF.show(3)
testDF01.show(20)
# testDF02.show(30)
