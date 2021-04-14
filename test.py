import pandas as pd
import pymysql
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

'''
part-00002-f040da2a-5abd-4025-a8ba-5b2c48a1531a-c000.csv
'''


def get_new_par_drug():
    path = '/home/hadoop/data/csv'
    df = spark.read.format('csv').option('header', 'true').load(path)
    path1 = 'hdfs://localhost:9000/result/drugNotesPar'
    df.write.format('parquet').mode("overwrite").save(path1)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("example") \
        .config("spark.debug.maxToStringFields", "100") \
        .getOrCreate()

    get_new_par_drug()

# conn = pymysql.connect(host='localhost', user='warren', password='123456', db='spark',
#                        port=3306, charset='utf8')
# cur = conn.cursor()
#
# i = 0
# for index in data.index:
#     row = data.loc[index].values[0].split(',')
#     a = str(row[0])
#     b = str(row[1])
#     c = str(row[2])
#     d = str(row[3])
#     e = str(row[4])
#     f = int(row[5])
#     g = float(row[6])
#     h = float(row[7])
#     try:
#         cur.execute("INSERT INTO drugNameList VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
#                     (str(a), str(b), str(c), str(d), str(e), int(f), float(g), float(h)))
#         i += 1
#         print(str(i))
#         conn.commit()
#     except:
#         list_error = [a, b, c, d, e, f, g]
#         path = '/home/hadoop/data/csv/error.txt'
#         with open(path, 'w') as f:
#             f.write('\n'.join(list_error))
#         print(i)
#         print(str(a), str(b), str(c), str(d), str(e), str(f), str(g))
#
# cur.close()
# conn.close()
