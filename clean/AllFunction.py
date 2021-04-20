import time
import pymysql
import CreateData as CD
from collections import Counter
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
    """
    将药品按月分类计算并计算总和
    @param data: 传入的dataframe
    @return: 处理的结果的dataframe
    """
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
    data_nums = data_nums.orderBy(data_nums['Sum'].desc())
    return data_nums


# 连接Mysql数据库
def get_conn():
    """
    连接Mysql
    @return: 连接的接口
    """
    db = pymysql.connect(host='localhost', user='warren', password='123456', db='spark',
                         port=3306, charset='utf8')
    return db


def drug_nums_to_mysql(path, year, choice, num):
    """
    将历年药品每月用量及总和导入Mysql
    @param path: 文件路径
    @param year: 年份
    @param choice: 药品类型
    @param num: 展示数量
    @return: null
    """

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

    conn = get_conn()
    cur = conn.cursor()
    t = 0
    df_poor = df_poor.collect()

    for i in df_poor:
        print(str(t))
        if t == int(num):
            break
        t += 1
        temp_poor = []
        temp_not_poor = []

        try:
            j = df_not_poor.select('*').where(df_not_poor.DrugName == str(i['DrugName'])).collect()[0]
            print(i)
            temp_poor.append(i['DrugName'])
            temp_not_poor.append(i['DrugName'])
            # 0 --> 建档立卡
            # 1 --> 非建档立卡
            cur.execute(
                "INSERT INTO drugNumList VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (i["DrugName"], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], year, 0,
                 choice, i['Sum']))
            cur.execute(
                "INSERT INTO drugNumList VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (i["DrugName"], j[1], j[2], j[3], j[4], j[5], j[6], j[7], j[8], j[9], j[10], j[11], j[12], year, 1,
                 choice, j['Sum']))
        except Exception as e:
            print(e)
            temp_poor.append(i['DrugName'])
            temp_not_poor.append(i['DrugName'])
            cur.execute(
                "INSERT INTO drugNumList VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (i["DrugName"], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], year, 0,
                 choice, i['Sum']))
            cur.execute(
                "INSERT INTO drugNumList VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (i["DrugName"], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, year, 1, choice, 0))
        conn.commit()

    # all_nums = {
    #     'poor': all_drug_nums_poor,
    #     'not_poor': all_drug_nums_not_poor
    # }
    # cur.close()
    # conn.close()

    '''
    ['药名', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'] 
    ['复方丹参滴丸', 57, 44, 56, 44, 32, 78, 54, 43, 32, 45, 67, 78],
    '''


def get_drug_nums_mysql(year, choice, num, personalType):
    """
    @param year: 年份
    @param choice: 药品类别
    @param num: 导入数量
    @param personalType: 人员类型
    @return: null
    """
    conn = get_conn()
    cur = conn.cursor()
    if str(year) != 'all':
        cur.execute("SELECT drugNumList.drugName, drugNumList.1, drugNumList.2, drugNumList.3, drugNumList.4, "
                    "drugNumList.5, drugNumList.6, drugNumList.7, drugNumList.8, drugNumList.9, drugNumList.10, "
                    "drugNumList.11, drugNumList.12 FROM drugNumList "
                    "WHERE year=%s AND type=%s AND drugType=%s",
                    (int(year), personalType, choice))
    else:
        cur.execute("SELECT a.* FROM drugNumList a "
                    "WHERE type=%s AND drugType=%s "
                    "AND (SELECT COUNT(*) "
                    "FROM drugNumList b "
                    "WHERE b.drugName=a.drugName AND b.year=a.year AND b.sum >= a.sum"
                    ")<=2 "
                    "ORDER BY year, sum DESC ",
                    (personalType, choice))
        results = cur.fetchall()
        result_list = []
        i = 0
        print(list(results[0]))

    cur.close()
    conn.close()


# 获得年龄数据
def get_data_age_poor(path, year):
    data = spark.read.format('parquet').load(path)

    data = data.where("DT == {}".format(year)) \
        .select('PersonalType', 'DT', 'HosRegisterCode', 'Age', 'Sex') \
        .dropDuplicates(subset=['HosRegisterCode']) \
        .drop("HosRegisterCode") \
        .withColumn('Count', F.lit(1))

    data_poor = data.withColumn('Age', data.Age.cast(IntegerType())).where('PersonalType = 17') \
        .drop("DT", "PersonalType")
    data_not_poor = data.withColumn('Age', data.Age.cast(IntegerType())).where('PersonalType != 17') \
        .drop("DT", "PersonalType")

    data_poor = data_poor.groupby("Age") \
        .pivot("Sex", ['男', '女']) \
        .agg(F.sum('Count')) \
        .fillna(0)

    data_not_poor = data_not_poor.groupby("Age") \
        .pivot("Sex", ['男', '女']) \
        .agg(F.sum('Count')) \
        .fillna(0)

    man_list_poor = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    women_list_poor = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    man_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    women_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for i, j in zip(data_poor.collect(), data_not_poor.collect()):
        p = int(i['Age'] / 10)
        q = int(j['Age'] / 10)
        if p > 9 or q > 9:
            p = 9
            q = 9
        man_list_poor[p] += int(i['男'])
        women_list_poor[p] += int(i['女'])

        man_list[q] += int(j['男'])
        women_list[q] += int(j['女'])
    men_list_poor = [-x for x in man_list_poor]
    men_list = [-x for x in man_list]

    # data_poor.createOrReplaceTempView("form")
    # data_not_poor.createOrReplaceTempView("form1")
    # df = spark.sql("""
    # Select Sex,
    #        sum(case when Age > 90 then 1 else 0 end) as A,
    #        sum(case when Age between 80 and 89 then 1 else 0 end) as B,
    #        sum(case when Age between 70 and 79 then 1 else 0 end) as C,
    #        sum(case when Age between 60 and 69 then 1 else 0 end) as D,
    #        sum(case when Age between 50 and 59 then 1 else 0 end) as E,
    #        sum(case when Age between 40 and 49 then 1 else 0 end) as F,
    #        sum(case when Age between 30 and 39 then 1 else 0 end) as G,
    #        sum(case when Age between 20 and 29 then 1 else 0 end) as H,
    #        sum(case when Age between 10 and 19 then 1 else 0 end) as I,
    #        sum(case when Age between 0 and 9 then 1 else 0 end) as J
    # from form
    # group by Sex""")


# 搜索
def get_data_info(path, keyWords, searchContent, page, limit):
    testDF = spark.read.format('parquet').load(path)
    start = time.time()
    testDF.createOrReplaceTempView('tweets')
    if keyWords == 'name':
        testDF = spark.sql(
            "SELECT CertificateCode, Desc, AllName, Name, Age, Sex, HosRegisterCode, OutHosDate, InHosDate, "
            "DaysInHos, TotalFee "
            "FROM tweets WHERE tweets.Name='{}'".format(searchContent))
    elif keyWords == 'id':
        testDF = spark.sql(
            "SELECT CertificateCode, Desc, AllName, Name, Age, Sex, HosRegisterCode, OutHosDate, InHosDate, "
            "DaysInHos, TotalFee "
            "FROM tweets WHERE tweets.CertificateCode='{}'".format(searchContent)) \
            .dropDuplicates(subset=['HosRegisterCode'])
    elif keyWords == 'hosid':
        testDF = spark.sql(
            "SELECT CertificateCode, Desc, AllName, Name, Age, Sex, HosRegisterCode, OutHosDate, InHosDate, "
            "DaysInHos, TotalFee "
            "FROM tweets "
            "WHERE tweets.HosRegisterCode='{}'".format(searchContent)) \
            .dropDuplicates(subset=['HosRegisterCode'])


# 获得药品费用数据
def drug_fee_temp(data, personalType, feeKey, classKey):
    """
    获取不同人员患者药品费用
    @param data: dataframe
    @param personalType: 人员类型
    @param feeKey: 费用类型
    @param classKey: 药品类型
    @return: list
    """
    data = data.withColumn("RegisterDate", data.RegisterDate.substr(6, 2)) \
        .withColumnRenamed('RegisterDate', 'Month') \
        .drop('PersonalType', "CompRatio_Type")

    data = data.withColumn("Month", data.Month.cast(IntegerType()))

    data = data.groupBy("DT") \
        .pivot("Month", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) \
        .agg(F.sum(str(feeKey))) \
        .fillna(0)
    conn = get_conn()
    cur = conn.cursor()

    data = data.orderBy(data['DT'].asc())
    data.show()
    fee_list = []
    for i in data.collect():
        temp = []
        for j in range(1, 13):
            temp.append(i[j])
        print(temp)
        cur.execute("INSERT INTO drugFeeList VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], int(i[0]),
                        str(classKey),
                        str(feeKey), personalType))
        conn.commit()
        fee_list.append(temp)

    cur.close()
    conn.close()
    fee_list = []

    return fee_list


# 获得药品费用数据
def get_data_drug_fee(path, classKey, feeKey):
    """
    获取药品费用数据
    @param path: 文件路径
    @param classKey: 药品类型
    @param feeKey: 费用类型
    @return: list
    """
    # 按月展示
    inputs = spark.read.format('parquet').load(path)
    data = inputs.select('RegisterDate', 'CompRatio_Type', 'DT', feeKey, 'PersonalType') \
        .where('CompRatio_Type = "{}"'.format(str(classKey)))
    # 0 建档立卡
    data01 = data.where(data.PersonalType == "17")
    list01 = drug_fee_temp(data01, 0, feeKey, classKey)
    # 1 非建档立卡
    data02 = data.where(data.PersonalType != "17")
    list02 = drug_fee_temp(data02, 1, feeKey, classKey)


# 获得患者住院的药品信息
def get_info_drug(path_info, path_data):
    data_info = spark.read.format('parquet').load(path_info)
    data_drug = spark.read.format('parquet').load(path_data)

    data_info.createOrReplaceTempView("info")
    data_drug.createOrReplaceTempView("drug")

    start = time.time()
    result = spark.sql(f"""
                       SELECT 
                       drug.HosRegisterCode, drug.ItemName, drug.Expense_Type_Name, drug.DrugName, 
                       drug.CompRatio_Type, drug.Count, drug.FeeSum, drug.UnallowedComp 
                       FROM 
                       info , drug 
                       WHERE 
                       info.HosRegisterCode = drug.HosRegisterCode  AND info.HosRegisterCode = 'J8030410000002030002' 
                       """)
    end = time.time()
    print(str(end - start))
    print(result.count())
    result.show()


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
    dataFile = 'hdfs://localhost:9000/result/drugNotesPar'
    allFile = 'hdfs://localhost:9000/result/form_par_all'
    # get_info_drug(inputFile, dataFile)
    # read_data_content(inputFile)
    # get_data_drug_nums(inputFile, '2017', '甲类', 50)
    # get_data_age_poor(inputFile, '2017')
    # get_data_info(inputFile, 'name', '柳三女', 1, 20)
    # get_drug_nums_mysql(2017, '甲类', 10, 0)
    # drug_nums_to_mysql(allFile, 2017, "乙类", 20)
    get_data_drug_fee(allFile, "甲类", "FeeSum")
    get_data_drug_fee(allFile, "乙类", "FeeSum")
    get_data_drug_fee(allFile, "丙类", "FeeSum")
    get_data_drug_fee(allFile, "甲类", "AllowedComp")
    get_data_drug_fee(allFile, "乙类", "AllowedComp")
    get_data_drug_fee(allFile, "丙类", "AllowedComp")
    get_data_drug_fee(allFile, "甲类", "UnallowedComp")
    get_data_drug_fee(allFile, "乙类", "UnallowedComp")
    get_data_drug_fee(allFile, "丙类", "UnallowedComp")

    '''
    1 2 3 4 5 6 7 8 9 10 11 12 year class feeType personalType
    '''

    '''
    class: (药品类型)
    甲类
    乙类
    丙类
    all 
    
    feeType: (费用类型)
    FeeSum
    AllowedComp
    UnallowedComp
    
    personType: (人员类型)
    0
    1
    '''
