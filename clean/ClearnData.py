import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
import pyspark.sql.functions as F

'''
# 入院时间(OutHosDate)存在空值，删除
# 日期相减得负值，删除
# 新添一行住院日期(DaysInHos)
# 性别(Sex)统一为男，女
# 疾病编码(DiseaseCode)为空，删除
# 疾病编码(DiseaseCode)去除小数点
# 所有的null替换为0
'''


def search(file):
    starttime = time.time()
    dataFile = spark.read.format('csv').option('header', 'true').load(file)
    dataFile.createOrReplaceTempView("tweets01")
    dataTweets = spark.sql("""SELECT * from tweets01 where tweets01.OutHosDate is null""")
    endtime = time.time()
    dataTweets.show(1000)
    print(endtime - starttime)


# 修改性别
def changeSex(file):
    df = spark.read.format('csv').option('header', 'true').load(file)
    # df.show(10)
    df = df.replace({'1': '男', '2': '女', '男性': '男', '女性': '女'}, subset=['Sex'])
    changeOutHosDate(df)


# 处理出院日期
def changeOutHosDate(data):
    data.show()
    data = data.na.drop(subset=['OutHosDate'])
    addDaysInHos(data)
    # data = data.fillna({'DrugName': '0', 'CompRatio_Type': '0'})


# 添加住院天数
def addDaysInHos(data):
    # 删除时分秒
    data = data.withColumn('OutHosDate', F.to_date(data.OutHosDate.substr(1, 10)))
    data = data.withColumn('InHosDate', F.to_date(data.InHosDate.substr(1, 10)))
    # 计算天数
    data = data.withColumn('DaysInHos', F.datediff(data['OutHosDate'], data['InHosDate']))
    # 删除天数为负数的行
    data = data.select('*').where(data.DaysInHos >= 0).dropDuplicates(subset=['HosRegisterCode'])
    # 删除OutHosDate和InHosDate
    # data = data.drop('OutHosDate', 'InHosDate').dropDuplicates(subset=['HosRegisterCode'])
    data.show(50)
    dealDiseaseCode(data)


# 处理疾病编码
def dealDiseaseCode(data):
    # 去空值
    data = data.na.drop(subset=["DiseaseCode"])
    # 去小数点
    data = data.withColumn("DiseaseCode", data.DiseaseCode.substr(1, 5))
    getNewAge(data)


# 修改年龄
def getNewAge(data):
    # 150221 1940 0212472x
    # data = spark.read.format('parquet').load(path)

    data = data.withColumn("Born", F.split('CertificateCode', '\d{7}.$')) \
        .withColumn('Born', F.concat_ws("", "Born")) \
        .withColumn('Born', F.split('Born', '\d{6}')) \
        .withColumn('Born', F.concat_ws("", "Born"))

    data = data.withColumn("Age", F.when((data.DT - data.Born) != data.Age, data.DT - data.Born).otherwise(data.Age)) \
        .drop('Born')
    data.show(30)
    dealNull(data)


# 剩余null替换为0
def dealNull(data):
    data = data.fillna('0')
    data.write.format('parquet').mode("overwrite").save(inputFile)
    # data.write.format('csv').option("header", "true").mode("overwrite").save(inputFile2)


if __name__ == '__main__':
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

    inputFile = 'hdfs://localhost:9000/result/form_par_new'
    inputFile2 = 'hdfs://localhost:9000/result/all_form'
    changeSex(inputFile2)
