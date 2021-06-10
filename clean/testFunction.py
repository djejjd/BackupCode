import time
import json
import pymysql
from pymongo import MongoClient
from collections import Counter
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, LongType, StringType


def print_data(path01, path02):
    # data01 = spark.read.format('csv').option("header", "true").load(path02).where("Level = '03'")
    # data01.show()
    # data01 = spark.read.format('parquet').load(path01).dropDuplicates(subset=['HosRegisterCode'])
    data01 = spark.read.format('parquet').load(path01)
    data02 = spark.read.format('parquet').load(path02)
    print(data01.count(), data02.count())


def saveMongodb(data):
    if db[MONGO_TABLE].insert(data):
        print("成功存储: ", data)
    else:
        print("未成功存储: ", data)


# 计算扶贫费用
def getFeePoor(data, personalType, feeKey):
    data = data.withColumn("Month", data.Month.cast(IntegerType()))

    data = data.groupBy("DT") \
        .pivot("Month", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) \
        .agg(F.sum(str(feeKey))) \
        .fillna(0)

    data = data.orderBy(data['DT'].asc())
    data.show()

    # client = MongoClient("localhost", 27017)
    # db = client['Spark']
    # table = db['feeInfo']

    for i in data.rdd.collect():
        temp = []
        for j in range(1, 13):
            temp.append(round(i[j], 2))

        dictFee = {
            'year': i[0],
            'fee': temp,
            'feeType': feeKey,
            'personalType': personalType
        }
        saveMongodb(dictFee)


def getFee(path):
    data = spark.read.format("parquet") \
        .load(path) \
        .select("RegisterDate", "DT", "TotalFee", "RealComp", "PersonalType")
    data = data.withColumn("RegisterDate", data.RegisterDate.substr(6, 2)) \
        .withColumnRenamed('RegisterDate', 'Month')
    # 17-->贫困户
    df = data.where(data.PersonalType == "17")
    getFeePoor(df.drop("RealComp", "PersonalType"), 0, 'TotalFee')
    getFeePoor(df.drop("TotalFee", "PersonalType"), 0, 'RealComp')

    # 非贫困户
    df = data.where(data.PersonalType != '17')
    getFeePoor(df.drop("RealComp", "PersonalType"), 1, 'TotalFee')
    getFeePoor(df.drop("TotalFee", "PersonalType"), 1, 'RealComp')


def get_data_info(path, keyWords, searchContent, page, limit):
    testDF = spark.read.format('parquet').load(path)
    start = time.time()
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
    end = time.time()
    print("1: " + str(end - start))
    testDF.show(testDF.count())

    start = time.time()
    testDF = testDF.toPandas()
    end = time.time()
    print("2: " + str(end - start))

    start = time.time()
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
    end = time.time()
    print("3: " + str(end - start))
    for i in json_list:
        print(i)


def get_conn():
    db = pymysql.connect(host='localhost', user='warren', password='123456', db='spark',
                         port=3306, charset='utf8')
    return db


# 增加药品信息
def get_data_drug(path_drug, path_person):
    # conn = get_conn()
    # cur = conn.cursor()

    data_drug = spark.read.format('csv').option("header", "true").load(path_drug) \
        .select("HosRegisterCode", "ItemName", "Expense_Type_Name", "DrugName", "CompRatio_Type", "Count", "FeeSum",
                "UnallowedComp")
    # allFee.Fee.cast(DecimalType(18, 2))
    data_drug = data_drug.withColumn("HosRegisterCode", data_drug.HosRegisterCode.cast(StringType())) \
        .withColumn("Count", data_drug.Count.cast(IntegerType())) \
        .withColumn("FeeSum", data_drug.FeeSum.cast(FloatType())) \
        .withColumn("UnallowedComp", data_drug.UnallowedComp.cast(FloatType()))

    file_save = '/home/hadoop/data/csv'
    data_drug.repartition(5).write.format('csv').option("header", "true").mode("overwrite").save(file_save)

    # prop = {
    #     'user': 'root',
    #     'password': '123456',
    #     'driver': 'com.mysql.jdbc.Driver'
    # }
    # data_drug.write.jdbc("jdbc:mysql://localhost:3306/test", "drugNameList", "append", prop)

    # file_save = '/home/hadoop/data/csv'
    # data_drug.write.format('csv').option("header", "true").mode("overwrite").save(file_save)

    # data_drug = data_drug.collect()
    # print("-----------")
    # for i in data_drug:
    #     cur.execute("INSERT INTO drugNameList VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
    #                 (int(i["HosRegisterCode"]), str(i["ItemName"]), str(i["Expense_Type_Name"]), str(i["DrugName"]),
    #                  str(i["CompRatio_Type"]), int(i["Count"]), float(i["FeeSum"]), float(i["UnallowedComp"])))
    # conn.commit()
    # cur.close()
    # conn.close()

    '''
    住院编码          项目名称   费用类型           药品名      药品类别      数量   总费用  自付金额
    HosRegisterCode ItemName Expense_Type_Name DrugName CompRatio_Type Count FeeSum UnallowedComp 
    '''


def get_result():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM drugNumList")

    result = cur.fetchall()
    for i in result:
        print(i[0], i[1])
        break
    cur.close()
    conn.close()


def insertAge():
    conn = get_conn()
    cur = conn.cursor()
    # value = {'data': [6280, 1343, 6428, 4990, 8905, 11893, 11914, 8161, 5211, 411]}
    # value = json.dumps(value)
    # cur.execute("INSERT INTO peopleAgeList VALUES (%s, %s, %s, %s)",
    #             (value, 2017, 1, 'women'))
    # conn.commit()
    cur.execute("SELECT json_extract(data, '$.data') "
                "FROM peopleAgeList "
                "WHERE year = %s ",
                2017)
    results = cur.fetchall()
    print(results[0][0])
    man_list_poor = list(map(int, results[0][0][1:-1].split(', ')))
    print(man_list_poor)

    cur.close()
    conn.close()


def putDiseaseInfoIntoMongodb():
    path = '/home/hadoop/data_school/new_csv/diseaseInfo.csv'


def calDrugFee(data, personalType, drugType, year):
    data = data.withColumn("Month", data.Month.cast(IntegerType()))

    data = data.groupBy("ItemName") \
        .pivot("Month", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) \
        .agg(F.sum("FeeSum")) \
        .fillna(0)

    data_nums = data.withColumn('Sum', F.lit(0))
    data_nums = data_nums.withColumn('Sum', F.when(data_nums.Sum == 0,
                                                   data_nums['1'] + data_nums['2'] + data_nums['3'] + data_nums['4'] +
                                                   data_nums['5'] + data_nums['6'] + data_nums['7'] + data_nums['8'] +
                                                   data_nums['9'] + data_nums['10'] + data_nums['11'] + data_nums[
                                                       '12']))
    data_nums = data_nums.orderBy(data_nums['Sum'].desc())

    for i in data_nums.rdd.collect():
        temp = []
        for j in range(1, 13):
            temp.append(round(i[j], 2))

        dictFee = {
            'drugName': i[0],
            'fee': temp,
            'sum': i[13],
            'drugType': drugType,
            'year': year,
            'personalType': personalType
        }
        saveMongodb(dictFee)


def getDrugFee(path):
    data = spark.read.format('parquet') \
        .load(path) \
        .select("ItemName", "FeeSum", "DT", "RegisterDate", "CompRatio_Type", "PersonalType")
    data = data.where(data.CompRatio_Type != '0') \
        .withColumn("RegisterDate", data.RegisterDate.substr(6, 2)) \
        .withColumnRenamed('RegisterDate', 'Month')

    # 处理贫困人口药品费用
    df = data.where((data.DT == '2017') & (data.PersonalType == 17) & (data.CompRatio_Type == "甲类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "甲类", 2017)
    df = data.where((data.DT == '2018') & (data.PersonalType == 17) & (data.CompRatio_Type == "甲类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "甲类", 2018)
    df = data.where((data.DT == '2019') & (data.PersonalType == 17) & (data.CompRatio_Type == "甲类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "甲类", 2019)

    df = data.where((data.DT == '2017') & (data.PersonalType == 17) & (data.CompRatio_Type == "乙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "乙类", 2017)
    df = data.where((data.DT == '2018') & (data.PersonalType == 17) & (data.CompRatio_Type == "乙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "乙类", 2018)
    df = data.where((data.DT == '2019') & (data.PersonalType == 17) & (data.CompRatio_Type == "乙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "乙类", 2019)

    df = data.where((data.DT == '2017') & (data.PersonalType == 17) & (data.CompRatio_Type == "丙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "丙类", 2017)
    df = data.where((data.DT == '2018') & (data.PersonalType == 17) & (data.CompRatio_Type == "丙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "丙类", 2018)
    df = data.where((data.DT == '2019') & (data.PersonalType == 17) & (data.CompRatio_Type == "丙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 0, "丙类", 2019)

    # 处理非贫困人口药品费用
    df = data.where((data.DT == '2017') & (data.PersonalType != 17) & (data.CompRatio_Type == "甲类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "甲类", 2017)
    df = data.where((data.DT == '2018') & (data.PersonalType != 17) & (data.CompRatio_Type == "甲类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "甲类", 2018)
    df = data.where((data.DT == '2019') & (data.PersonalType != 17) & (data.CompRatio_Type == "甲类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "甲类", 2019)

    df = data.where((data.DT == '2017') & (data.PersonalType != 17) & (data.CompRatio_Type == "乙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "乙类", 2017)
    df = data.where((data.DT == '2018') & (data.PersonalType != 17) & (data.CompRatio_Type == "乙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "乙类", 2018)
    df = data.where((data.DT == '2019') & (data.PersonalType != 17) & (data.CompRatio_Type == "乙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "乙类", 2019)

    df = data.where((data.DT == '2017') & (data.PersonalType != 17) & (data.CompRatio_Type == "丙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "丙类", 2017)
    df = data.where((data.DT == '2018') & (data.PersonalType != 17) & (data.CompRatio_Type == "丙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "丙类", 2018)
    df = data.where((data.DT == '2019') & (data.PersonalType != 17) & (data.CompRatio_Type == "丙类"))
    calDrugFee(df.drop("CompRatio_Type", "PersonalType", "DT"), 1, "丙类", 2019)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "100") \
        .getOrCreate()

    MONGO_URL = "mongodb://localhost:27017"
    MONGO_DB = "Spark"
    MONGO_TABLE = "drugFeeList"
    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]

    # 数据集
    path_data = 'hdfs://localhost:9000/data/data_tree'

    # 大表
    # path_par = 'hdfs://localhost:9000/result/form_par'
    path_par = 'hdfs://localhost:9000/result/form_par_all'
    inputFile = 'hdfs://localhost:9000/result/form_par_new'
    # inputFile = 'hdfs://localhost:9000/csv/Join_Canton'
    # inputFile = 'hdfs://localhost:9000/result/all_form'
    getDrugFee(inputFile)
    # insertAge()
    # putDiseaseInfoIntoMongodb()
    # print_data(path_par, inputFile)
    # getFee(inputFile)
    # print_data(path_par, inputFile)
    # get_data_drug(path_par, inputFile)
    # get_result()
    # get_data_info(inputFile, 'name', '柳三女', 1, 20)

'''
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
        {
            "HosRegisterCode": None, "InHosDate": None, "OutHosDate": None, "DayInHos": None, "AllFee":None, "DrugList":[
            {"DrugName": None, "DrugType": None, "DrugCount": None, "DrugFee": None},
            {"DrugName": None, "DrugType": None, "DrugCount": None, "DrugFee": None}]
        },
        
        {"HosRegisterCode": None, "InHosDate": None, "OutHosDate": None, "DayInHos": None},
        {"HosRegisterCode": None, "InHosDate": None, "OutHosDate": None, "DayInHos": None},
    ]
}
'''
