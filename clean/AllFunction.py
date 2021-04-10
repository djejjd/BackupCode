import time
from collections import Counter
import CreateData as CD
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType


# TODO: 按月分类统计药品（分甲、乙、丙）
# TODO：三年的扶贫费用开支对比
# TODO：多做对比突出显示扶贫


def read_data_content(path):
    # data = spark.read.format('parquet').load(path).dropDuplicates(subset=['HosRegisterCode', 'CertificateCode'])
    # print(data.dtypes)
    # data.show()
    path = "hdfs://localhost:9000/csv/Join_Canton"
    data = spark.read.format('csv').option('header', 'true').load(path).where('Level = "02"')
    data.show()


# TODO: 按月分类统计药品（分甲、乙、丙）
def get_data_temp(data):
    data_poor = data.withColumn('Count', F.when(data.Count <= 0, 0).otherwise(data.Count))

    data_nums = data_poor.groupby("DrugName") \
        .pivot('Month', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) \
        .agg(F.sum('Count')) \
        .fillna(0)
    data_nums = data_nums.withColumn('Sum', F.lit(0))
    data_nums = data_nums.withColumn('Sum', F.when(data_nums.Sum == 0,
                                                   data_nums['1'] + data_nums['2'] + data_nums['3'] + data_nums['4'] +
                                                   data_nums['5'] + data_nums['6'] + data_nums['7'] + data_nums['8'] +
                                                   data_nums['9'] + data_nums['10'] + data_nums['11'] + data_nums[
                                                       '12']))
    # data_nums = data_nums.orderBy(data_nums['Sum'].desc())
    return data_nums


# TODO：对比贫困人口和非贫困人口的用药区别
def get_data_drug_nums(path, year, choice, num):
    start = time.time()
    data = spark.read.format('parquet').load(path)
    data = data.select('PersonalType', 'RegisterDate', 'DT', 'DrugName', 'Count', 'CompRatio_Type') \
        .where("CompRatio_Type = '" + choice + "'") \
        .where("DT = {}".format(year)) \
        .drop('DT', 'CompRatio_Type')
    # 处理RegisterDate,提取月份
    data = data.withColumn('RegisterDate', data.RegisterDate.substr(6, 2)) \
        .withColumnRenamed('RegisterDate', 'Month')

    data = data.withColumn('Month', data.Month.cast(IntegerType())) \
        .withColumn('Count', data.Count.cast(IntegerType())) \
        .withColumn('DrugName', CD.changeNameUDF(data.DrugName)) \
        .where('DrugName != "0"')
    data_poor = data.where('PersonalType = 17').drop('PersonalType')
    data_not_poor = data.where('PersonalType != 17').drop('PersonalType')

    df_not_poor = get_data_temp(data_not_poor)
    df_poor = get_data_temp(data_poor)

    df_poor = df_poor.orderBy(df_poor['Sum'].desc())
    all_drug_nums_poor = [['药名', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']]
    all_drug_nums_not_poor = [['药名', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']]

    t = 0
    df_poor = df_poor.collect()

    for i in df_poor:
        if t == int(num):
            break
        t += 1
        temp_poor = []
        temp_not_poor = []
        try:
            j = df_not_poor.select('*').where("DrugName = '" + str(i['DrugName']) + "'").collect()[0]
            temp_poor.append(i['DrugName'])
            temp_not_poor.append(i['DrugName'])
            for p in range(1, 13):
                temp_poor.append(i[p])
                temp_not_poor.append(j[p])
        except Exception as e:
            temp_poor.append(i['DrugName'])
            temp_not_poor.append(i['DrugName'])
            for p in range(1, 13):
                temp_poor.append(i[p])
                temp_not_poor.append(0)
        all_drug_nums_poor.append(temp_poor)
        all_drug_nums_not_poor.append(temp_not_poor)

    all_nums = {
        'poor': all_drug_nums_poor,
        'not_poor': all_drug_nums_not_poor
    }
    '''
    ['药名', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'] 
    ['复方丹参滴丸', 57, 44, 56, 44, 32, 78, 54, 43, 32, 45, 67, 78],
    '''


# 获得年龄数据
def get_data_age_poor(path, year):
    data = spark.read.format('parquet').load(path)

    data = data.where("DT == {}".format(year)) \
        .select('PersonalType', 'DT', 'HosRegisterCode', 'Age', 'Sex') \
        .dropDuplicates(subset=['HosRegisterCode']) \
        .drop("HosRegisterCode") \
        .withColumn('Count', F.lit(1))

    data_poor = data.withColumn('Age', data.Age.cast(IntegerType())).where('PersonalType = 17').drop("DT",
                                                                                                     "PersonalType")
    data_not_poor = data.withColumn('Age', data.Age.cast(IntegerType())).where('PersonalType != 17').drop("DT",
                                                                                                          "PersonalType")

    # men_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # women_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # for i in data_new.collect():
    #     p = int(i['Age'] / 10)
    #     if p > 9:
    #         p = 9
    #     men_list[p] += int(i['男'])
    #     women_list[p] += int(i['女'])

    data_poor.createOrReplaceTempView("form")
    data_not_poor.createOrReplaceTempView("form1")
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

    men_list = []
    women_list = []

    start = time.time()
    df_col = df.collect()
    end = time.time()

    for j in range(1, 11):
        men_list.append(df_col[1][j])
        women_list.append(df_col[2][j])
        # men_list = [df_col[1][1], df_col[1][2], df_col[1][3], df_col[1][4], df_col[1][5], df_col[1][6], df_col[1][7],
        #             df_col[1][8], df_col[1][9], df_col[1][10], ]
        # women_list = [df_col[2][1], df_col[2][2], df_col[2][3], df_col[2][4], df_col[2][5], df_col[2][6], df_col[2][7],
        #       df_col[2][8], df_col[2][9], df_col[2][10], ]
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


# 搜索
def get_data_info(path, keyWords, searchContent, page, limit):
    testDF = spark.read.format('parquet').load(path)
    testDF.createOrReplaceTempView('tweets')
    if keyWords == 'name':
        testDF = spark.sql(
            "SELECT CertificateCode, Desc, AllName, Name, Age, Sex, HosRegisterCode, OutHosDate, InHosDate, DaysInHos "
            "FROM tweets WHERE tweets.Name='{}'".format(searchContent)) \
            .dropDuplicates(subset=['HosRegisterCode'])
    elif keyWords == 'id':
        testDF = spark.sql(
            "SELECT CertificateCode, Desc, AllName, Name, Age, Sex, HosRegisterCode, OutHosDate, InHosDate, DaysInHos "
            "FROM tweets WHERE tweets.CertificateCode='{}'".format(searchContent)) \
            .dropDuplicates(subset=['HosRegisterCode'])
    elif keyWords == 'hosid':
        testDF = spark.sql(
            "SELECT CertificateCode, Desc, AllName, Name, Age, Sex, HosRegisterCode, OutHosDate, InHosDate, DaysInHos "
            "FROM tweets "
            "WHERE tweets.HosRegisterCode='{}'".format(searchContent)) \
            .dropDuplicates(subset=['HosRegisterCode'])

    testDF = testDF.withColumn('InHosDate', F.date_format(testDF.InHosDate, "yyyy-MM-dd"))
    testDF = testDF.withColumn('OutHosDate', F.date_format(testDF.OutHosDate, "yyyy-MM-dd"))
    testDF.show(testDF.count())

    testDF = testDF.toPandas()

    t = 0
    json_list = []
    register_list = []
    id_list = []
    for a, b, c, d, e, f, g, h, i, j in zip(testDF["CertificateCode"], testDF['Desc'], testDF['AllName'],
                                            testDF["Name"], testDF['Age'], testDF['Sex'], testDF['HosRegisterCode'],
                                            testDF['InHosDate'], testDF['OutHosDate'], testDF['DaysInHos']):
        if a in id_list:
            # 该CertificateCode曾经添加过，再出现就说明出现了新的HosRegisterCode
            i = id_list.index(a)
            # 获得在json_list中的位置
            num = Counter(register_list)[a]
            temp = {'HosRegisterCode': g, 'InHosDate': h, 'OutHosDate': i, 'DayInHos': j}
            tt = json_list[num]['Info']
            tt.append(temp)
            json_list[num]['Times'] += 1
        else:
            # 该CertificateCode未曾添加过
            id_list.append(a)
            temp = {'HosRegisterCode': g, 'InHosDate': h, 'OutHosDate': i, 'DayInHos': j}
            json_dict = {'Name': d, "Age": e, "Sex": f, "Desc": b, "CertificateCode": a, "Times": 1, "AllName": c,
                         'Info': [temp]}
            json_list.append(json_dict)
        t += 1
        if t == 20:
            break

    for i in json_list:
        print(i)

        # TODO：3. 住院编码 只有一人， 且只有一次记录
        '''
        
        查询方法：
        1. 姓名  多个人， 多次记录
        2. 身份证号 只有一人， 有多次记录
        
        
        
        数据格式1：
        {
            'Name': None,
            'Age': None,
            "Sex": None,
            "Desc": None,
            "CertificateCode": None,
            "Times": None,
            "AllName": None,
            "Info": [
                {"HosRegisterCode": None, "InHosDate": None, "OutHosDate": None, "DayInHos": None},
                {"HosRegisterCode": None, "InHosDate": None, "OutHosDate": None, "DayInHos": None},
                {"HosRegisterCode": None, "InHosDate": None, "OutHosDate": None, "DayInHos": None},
            ]
        }'''


# 获得药品费用数据
def drug_fee_temp(data):
    data = data.withColumn("RegisterDate", data.RegisterDate.substr(6, 2)) \
        .withColumnRenamed('RegisterDate', 'Month') \
        .drop('CompRatio_Type')

    data = data.withColumn("Month", data.Month.cast(IntegerType()))

    data = data.groupBy("DT") \
        .pivot("Month", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) \
        .agg(F.sum("FeeSum")) \
        .fillna(0)

    data = data.orderBy(data['DT'].asc())
    fee_list = []
    for i in data.collect():
        temp = []
        for j in range(1, 13):
            temp.append(i[j])
        fee_list.append(temp)
    return fee_list


def get_data_drug_fee(path, classKey, feeKey):
    # 按月展示
    inputs = spark.read.format('parquet').load(path)
    data = inputs.select('RegisterDate', 'CompRatio_Type', 'DT', feeKey, 'PersonalType') \
        .where('CompRatio_Type = "{}"'.format(str(classKey)))
    data01 = data.where(data.PersonalType == "17")
    list01 = drug_fee_temp(data01)
    data02 = data.where(data.PersonalType != "17")
    list02 = drug_fee_temp(data02)

    fee_list = list01 + list02
    print(fee_list)


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
    # read_data_content(inputFile)
    get_data_drug_nums(inputFile, '2017', '丙类', 5)
    # get_data_age_poor(inputFile, '2017')
    # get_data_info(inputFile, 'name', '柳三女', 1, 20)
    # get_data_drug_fee(inputFile, '甲类', 'FeeSum')
