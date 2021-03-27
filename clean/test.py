from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
from pyspark.sql.functions import split

conf = SparkConf().setAppName("spark_example")
sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)

ll = ['Type', 'Code', 'Desc', 'DT']
file_txt = 'hdfs://localhost:9000/txt/Global_DataDictionary.txt'

df = hiveCtx.read.text(file_txt)

# 将txt文件的每一行按照\t进行划分并以list形式存放
df_split = df.withColumn("value", split(df['value'], '\t'))
# 给每个列名分配序号用于后面添加表名
data = sc.parallelize([ll[i] for i in range(len(ll))]).zipWithIndex().collect()
# 添加表名并按照表名将列分割开
for name, index in data:
    df_split = df_split.withColumn(name, df_split['value'].getItem(index))
# 丢弃原来的value列
df_split = df_split.drop(df_split['value'])
df = df_split
data = df[(df['Type'].isin('U103-01'))].dropDuplicates(subset=['Type', 'Code', 'Desc'])
data01 = df[(df['Type'].isin('U609-02'))].dropDuplicates(subset=['Type', 'Code', 'Desc'])
pd = data.select('*').union(data01.select('*'))
data02 = df[(df['Type'].isin('U608-04'))].dropDuplicates(subset=['Type', 'Code', 'Desc'])
pd = pd.select('*').union(data02.select('*'))

pd.show(100)
# 构造存储路线
file_save = "hdfs://localhost:9000/csv/Global_DataDictionary"
pd.write.format('csv').option("header", "true").mode("overwrite").save(file_save)