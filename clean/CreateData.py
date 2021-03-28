import re
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row


# 清洗列表中的药品名
def changeDrugName(name):
    name = strQ2B(name)
    return name


# 清洗药品名
# def strQ2B(ustring):
def strQ2B(s):
    n = ''
    for char in s:
        num = ord(char)
        if num == 0x3000:        #将全角空格转成半角空格
            num = 32
        elif 0xFF01 <=num <= 0xFF5E:       #将其余全角字符转成半角字符
            num -= 0xFEE0
        num = chr(num)
        n += num
    ss = re.sub('\(.*\)', '', n).replace(' ', '')
    return ss


def create_data(path):
    data_par = spark.read.format('parquet').load(path)
    list_name = data_par.select('DrugName').dropDuplicates(subset=['DrugName']).toPandas().values.tolist()
    list_name = sum(list_name, [])
    names = []
    # 清洗药品名，删除同名药品
    for i in list_name:
        tt = strQ2B(i)
        names.append(tt)
    names = list(set(names))

    # 提取总表中的住院等级码和药品名
    data = data_par.select('HosRegisterCode', 'DrugName').where(data_par['DrugName'] != '0')
    data = data.withColumn("DrugName", changeNameUDF(data.DrugName))
    # '''
    # TODO: 未对总表的药品名进行清洗
    # '''
    # 给所有使用过的药品记1
    data = data.withColumn('Times', f.lit(1))
    # 以住院登记码做聚合，将药品名一列转为行，并进行匹配计算次数
    data_pivot = data.groupBy('HosRegisterCode').pivot('DrugName', names).agg(f.sum('Times')).fillna(0)
    # 存储结果
    data_pivot.write.format('parquet').mode("overwrite").save(path_csv)


# 测试是否正确
def test(path1, path2):
    inputs01 = spark.read.format('parquet').load(path1)
    # 总表的柳三女的相关数据
    df = inputs01.select('DrugName', 'HosRegisterCode').where(inputs01.HosRegisterCode == 'J9042610000000290002')
    df.show(100)
    df = df.select('DrugName').where("DrugName like '低钙腹膜%'")
    df.show(50)

    # 数据集里的柳三女的相关数据
    data = spark.read.format('parquet').load(path2)
    data = data.select("HosRegisterCode", "低钙腹膜透析液", "葡萄糖注射液", "参附注射液").where(
        data.HosRegisterCode == 'J9042610000000290002')
    data.show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "1000") \
        .getOrCreate()

    path_par = 'hdfs://localhost:9000/result/form_par'
    # 存放数据集（仅含有住院等级码，药品名，以及每种药的使用次数）
    path_csv = 'hdfs://localhost:9000/data/data_tree'

    # 注册自定义函数
    changeNameUDF = f.udf(changeDrugName, StringType())

    # create_data(path_par)
    # 测试数据集是否有问题
    test(path_par, path_csv)
