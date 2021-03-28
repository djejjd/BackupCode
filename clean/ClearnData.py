import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
from pyspark.sql.functions import to_date, datediff

conf = SparkConf().setAppName("spark_example").set("spark.default.parallelism", '16')
sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)

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
    dataFile = hiveCtx.read.format('csv').option('header', 'true').load(file)
    dataFile.createOrReplaceTempView("tweets01")
    dataTweets = hiveCtx.sql("""SELECT * from tweets01 where tweets01.OutHosDate is null""")
    endtime = time.time()
    dataTweets.show(1000)
    print(endtime - starttime)


# 修改性别
def changeSex(file):
    df = hiveCtx.read.format('csv').option('header', 'true').load(file)
    df = df.replace({'1': '男', '2': '女', '男性': '男', '女性': '女'})
    changeOutHosDate(df)


# 处理出院日期
def changeOutHosDate(data):
    # data = hiveCtx.read.format('csv').option('header', 'true').load(file)
    data = data.na.drop(subset=['OutHosDate'])
    addDaysInHos(data)
    # data = data.fillna({'DrugName': '0', 'CompRatio_Type': '0'})


# 添加住院天数
def addDaysInHos(data):
    # data = hiveCtx.read.format('csv').option('header', 'true').load(file)
    # 删除时分秒
    data = data.withColumn('OutHosDate', to_date(data.OutHosDate.substr(1, 10)))
    data = data.withColumn('InHosDate', to_date(data.OutHosDate.substr(1, 10)))
    # 计算天数
    data = data.withColumn('DaysInHos', datediff(data['OutHosDate'], data['InHosDate']))
    # 删除天数为负数的行
    data = data.select('*').where(data.DaysInHos >= 0)
    # 删除OutHosDate和InHosDate
    data = data.drop('OutHosDate').drop('InHosDate')
    dealDiseaseCode(data)


# 处理疾病编码
def dealDiseaseCode(data):
    # data = hiveCtx.read.format('csv').option('header', 'true').load(file)
    # 去空值
    data = data.na.drop(subset=["DiseaseCode"])
    # 去小数点
    data = data.withColumn("DiseaseCode", data.DiseaseCode.substr(1, 5))
    dealNull(data)


# 剩余null替换为0
def dealNull(data):
    # data = hiveCtx.read.format('csv').option('header', 'true').load(file)
    data = data.fillna('0')
    data.write.format('csv').option("header", "true").mode("overwrite").save(inputFile2)


if __name__ == '__main__':
    inputFile = 'hdfs://localhost:9000/result/all_form'
    inputFile2 = 'hdfs://localhost:9000/result/cleaned_form'
    changeSex(inputFile)

