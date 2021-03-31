import CreateData as CD
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

"""
要用到的部分表头：
    TotalFee：总费用
    RealComp：实际补偿费用
    SelfPay：患者自付费用
    Count：某种药的开药数目
    FeeSum：费用金额
    AllowedComp：可补偿金额
    UnallowedComp：不可补偿金额
    CompRatio：报销比例
    DT：数据年份
"""


# 功能
# 统计近三年的医保费用开支(以年为单位显示)--->RealComp, DT
# TODO:统计近三年药品开支情况(以年为单位显示)--->FeeSum, Count, DT
# TODO:统计近三年医保药品开支情况(即支付金额,数量;以年为单位显示)--->AllowedComp, CopRatio, Count, DT
# TODO:统计近三年扶贫人口患病的地理分布,年龄分布
# TODO:统计近三年某些地区的扶贫人口的患病年龄分布以及用药类型
# 机器学习
# TODO:预测未来几年医保扶贫费用的持续开支情况
# TODO:预测未来几年扶贫人口患病人群主要用药情况(纳入医保补偿范围)
# TODO:预测未来几年扶贫人口患病人群的分布位置


# 查询统计近几年的医保开销
def getDataMed(path):
    data = spark.read.format('parquet').load(path).select('HosRegisterCode', 'RealComp', 'DT')
    medFee = data.dropDuplicates(subset=['HosRegisterCode']).drop(
        'HosRegisterCode')
    # 计算费用
    allFee = medFee.groupby('DT').agg(F.sum('RealComp').alias('Fee')).orderBy(medFee.DT.desc())
    # 避免科学计数法显示
    allFee = allFee.withColumn('Fee', allFee.Fee.cast(DecimalType(18, 2)))
    allFee.show()


# 获得近三年的建档立卡人口的药品补助开支情况
def getDataDrugAllowed(path):
    # 获得人员类型（建档立卡）,根据人员类型统计用药费用
    data = spark.read.format('parquet').load(path).select('PersonalType', 'AllowedComp', 'DT')
    data = data.where(data.PersonalType == '17').groupby('DT').agg(F.sum(data.AllowedComp).alias('Fee')).orderBy(
        data.DT.desc())
    fee = data.withColumn('Fee', data.Fee.cast(DecimalType(18, 2)))
    fee.show()


# 获得建档立卡人口的用药情况
def getDataDrugPoor(path):
    data = spark.read.format('parquet').load(path).select('PersonalType', 'DrugName', 'Count', 'DT')



if __name__ == '__main__':
    # spark = SparkSession.builder \
    #     .master("local") \
    #     .appName("example") \
    #     .config("spark.debug.maxToStringFields", "100") \
    #     .config("spark.sql.shuffle.partitions", "400") \
    #     .config("spark.default.parallelism", "600") \
    #     .config("spark.sql.auto.repartition", "true") \
    #     .config("spark.sql.execution.arrow.enabled", "true") \
    #     .enableHiveSupport() \
    #     .getOrCreate()

    # 总表路径
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "1000") \
        .getOrCreate()

    inputFile = 'hdfs://localhost:9000/result/form_par'
    # getDataMed(inputFile)
    getDataDrugAllowed(inputFile)
    getDataDrugPoor(inputFile)
