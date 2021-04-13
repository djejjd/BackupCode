import time
import CreateData as CD
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType

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
'''
# TODO:统计近三年的医保费用开支(以年为单位显示)--->RealComp, DT
# TODO:统计近三年药品开支情况(以年为单位显示)--->FeeSum, Count, DT 
# TODO:统计近三年医保药品开支情况(即支付金额,数量;以年为单位显示)--->AllowedComp, CopRatio, Count, DT
# TODO:统计近三年扶贫人口患病的地理分布,年龄分布
'''


# TODO: 按月分类统计药品（分甲、乙、丙）
# TODO：三年的扶贫费用开支对比
# TODO：多做对比突出显示扶贫

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
    data = data.where(data.PersonalType == '17') \
        .groupby('DT') \
        .agg(F.sum(data.AllowedComp)
             .alias('Fee')) \
        .orderBy(
        data.DT.desc())
    fee = data.withColumn('Fee', data.Fee.cast(DecimalType(18, 2)))
    fee.show()


# 获取🥇三年来建档立卡人口的药品使用情况
def getDataDrugPoor(path, tt):
    data = spark.read.format('parquet').load(path)
    # 处理甲类药,获得甲类药每年的开销以及每年使用的数量
    data01 = data.select('PersonalType', 'DrugName', 'DT', 'Count', 'FeeSum', 'AllowedComp', 'CompRatio',
                         'CompRatio_Type')
    # & (data01.CompRatio_Type == '{}'.format(tt))

    data01 = data01.where((data01.PersonalType == '17') & (data01.DrugName != '0')) \
        .withColumn("Count", data01.Count.cast(IntegerType())) \
        .withColumn("FeeSum", data01.FeeSum.cast(IntegerType())) \
        .withColumn("DrugName", CD.changeNameUDF(data01.DrugName))
    # data01.show()
    data01 = data01.drop('PersonalType', 'CompRatio_Type', 'CompRatio', 'AllowedComp')

    data01_Fee = data01.drop("Count") \
        .groupby("DrugName") \
        .pivot("DT", ['2017', '2018', '2019']) \
        .agg(F.sum('FeeSum')) \
        .fillna(0)
    data01_Fee = data01_Fee.orderBy(data01_Fee['2019'].desc())
    data_fee = []
    for i in data01_Fee.head(20):
        dd = {'drugName': i['DrugName']}
        tt = [i['2017'], i['2018'], i['2019']]
        dd['drugFee'] = tt
        data_fee.append(dd)

    # data01_Count = data01.drop("FeeSum")\
    #     .groupby("DrugName")\
    #     .pivot("DT", ['2017', '2018', '2019'])\
    #     .agg(F.sum('Count'))\
    #     .fillna(0)
    # data01_Count = data01_Count.orderBy(data01_Count['2019'].desc())
    # data01_Count.show()


# 统计近三年患病扶贫人口的地理分布：总共21335人次
def getDataPlacePoor(path):
    path01 = 'hdfs://localhost:9000/csv/Join_Canton'
    data01 = spark.read.format('csv').option('header', 'true').load(path01) \
        .where('Level= 04').drop('UpperCode', 'AllName', 'DT') \
        .withColumn('CantonCode', F.split('CantonCode', '\d{4}$')) \
        .withColumn('CantonCode', F.concat_ws("", "CantonCode"))

    data01 = data01.withColumn("CantonName", F.when(data01.CantonName == '建档立卡人员', '固阳县建档立卡人员') \
                               .otherwise(data01.CantonName))

    data = spark.read.format('parquet').load(path) \
        .select('PersonalType', 'AllName', 'DT', 'HosRegisterCode', 'CantonCode') \
        .dropDuplicates(subset=['HosRegisterCode']) \
        .where('PersonalType = 17') \
        .withColumn('CantonCode', F.split('CantonCode', '\d{4}$')) \
        .withColumn('CantonCode', F.concat_ws("", "CantonCode"))

    data = data.join(data01, on='CantonCode', how='left_outer') \
        .drop('PersonalType', 'Level', 'CantonCode', 'AllName', 'ZoneCode', 'HosRegisterCode') \
        .dropDuplicates(subset=['HosRegisterCode']) \
        .withColumn('Times', F.lit(1)) \
        .groupby('CantonName') \
        .pivot('DT', ['2017', '2018', '2019']) \
        .agg(F.sum('Times')) \
        .fillna(0)
    data = data.orderBy(data['2019'].desc())
    data.show(50)
    data.groupby().sum().show()  # 和为21335


# 统计近三年患病扶贫人口的年龄分布
def getDataAgePoor(path, year):
    start = time.time()
    data = spark.read.format('parquet').load(path)
    end = time.time()
    # print(end - start)

    start = time.time()
    data = data.where("DT == {}".format(year)) \
        .select('PersonalType', 'DT', 'HosRegisterCode', 'Age', 'Sex') \
        .dropDuplicates(subset=['HosRegisterCode']) \
        .where('PersonalType = 17') \
        .drop("HosRegisterCode", "PersonalType") \
        .withColumn('Count', F.lit(1))
    end = time.time()
    # print(end - start)

    start = time.time()
    data = data.withColumn('Age', data.Age.cast(IntegerType())).drop("DT")
    data.show()
    data_new = data.groupby("Age") \
        .pivot("Sex", ['男', '女']) \
        .agg(F.sum('Count')) \
        .fillna(0)
    end = time.time()
    # print(end - start)

    # start = time.time()
    # men_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # women_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # for i in data_new.collect():
    #     p = int(i['Age'] / 10)
    #     if p > 9:
    #         p = 9
    #     men_list[p] += int(i['男'])
    #     women_list[p] += int(i['女'])
    # end = time.time()
    # print(end - start)

    data.createOrReplaceTempView("form")
    start = time.time()
    df = spark.sql("""
    Select Sex,
           sum(case when Age > 90 then 1 else 0 end) as A,
           sum(case when Age between 80 and 89 then 1 else 0 end) as B,
           sum(case when Age between 70 and 79 then 1 else 0 end) as C,
           sum(case when Age between 60 and 69 then 1 else 0 end) as D,
           sum(case when Age between 50 and 59 then 1 else 0 end) as E,
           sum(case when Age between 40 and 49 then 1 else 0 end) as F,
           sum(case when Age between 30 and 39 then 1 else 0 end) as G,
           sum(case when Age between 20 and 29 then 1 else 0 end) as H,
           sum(case when Age between 10 and 19 then 1 else 0 end) as I,
           sum(case when Age between 0 and 9 then 1 else 0 end) as J
    from form
    group by Sex""")
    end = time.time()
    print(end - start)

    start = time.time()
    df_col = df.collect()
    men_list = [df_col[1][1], df_col[1][2], df_col[1][3], df_col[1][4], df_col[1][5], df_col[1][6], df_col[1][7],
                df_col[1][8], df_col[1][9], df_col[1][10], ]
    women_list = [df_col[2][1], df_col[2][2], df_col[2][3], df_col[2][4], df_col[2][5], df_col[2][6], df_col[2][7],
                  df_col[2][8], df_col[2][9], df_col[2][10], ]
    end = time.time()
    print(end - start)
    print(men_list)

    # data_women = data.where(data.Sex == '女').drop('Sex')
    # data_women = data_women.groupBy('Age') \
    #     .pivot('DT', ['2017', '2018', '2019']) \
    #     .agg(F.sum('Count'))\
    #     .fillna(0)

    # data_men = data.where(data.Sex == '男').drop('Sex')
    # data_men = data_men.groupBy('Age') \
    #     .pivot('DT', ['2017', '2018', '2019']) \
    #     .agg(F.sum('Count')).fillna('0') \
    #     .fillna(0)
    # data = data.withColumn('Age', data.Age.cast(IntegerType())) \
    #     .orderBy(data['2019'].desc())
    # data.show()
    # data.groupby().sum().show()


# TODO:统计近三年某些地区的扶贫人口的患病年龄分布以及用药类型
def getDataAgeInPlace(path):
    data = spark.read.format('parquet').load(path)


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

    # 总表路径
    # spark = SparkSession.builder \
    #     .master("local") \
    #     .appName("example") \
    #     .config("spark.debug.maxToStringFields", "1000") \
    #     .getOrCreate()

    inputFile = 'hdfs://localhost:9000/result/form_par'
    # getDataMed(inputFile)
    # getDataDrugAllowed(inputFile)
    # getDataDrugPoor(inputFile, tt="乙类")
    # getDataPlacePoor(inputFile)
    getDataAgePoor(inputFile, '2017')
