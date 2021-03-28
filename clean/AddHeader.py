import re
from hdfs.client import Client
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
from pyspark.sql.functions import split


# 将txt文件转为csv并加表头
def add_header(path_file, path_csv,name_lists):
    # 获得hdfs中txt文件的路径以及文件名
    files = client.list(path_file, status=False)
    files = sorted(files, key=lambda i: len(i), reverse=False)
    i = 0
    for file in files:
        # 构造读取txt文件的path路径
        file_txt = "hdfs://localhost:9000" + path_file + "/" + file
        df = hiveCtx.read.text(file_txt)
        # 获得每个csv表的表头
        list_name = name_lists[i]
        # 将txt文件的每一行按照\t进行划分并以list形式存放
        df_split = df.withColumn("value", split(df['value'], '\t'))
        # 给每个列名分配序号用于后面添加表名
        data = sc.parallelize([list_name[i] for i in range(len(list_name))]).zipWithIndex().collect()
        # 添加表名并按照表名将列分割开
        for name, index in data:
            df_split = df_split.withColumn(name, df_split['value'].getItem(index))
        # 丢弃原来的value列
        df_split = df_split.drop(df_split['value'])
        # 构造存储路线
        df_split.show(2)
        #
        file_save = "hdfs://localhost:9000" + path_csv + '/' + file[:-4]
        df_split.write.format('csv').option("header", "true").mode("overwrite").save(file_save)
        i += 1


if __name__ == '__main__':
    '''
        注意：
        1. 需要将path = '/txt'换成自己的txt文件路径，需要指明存储csv文件的路径
        2. 先在hdfs中创建存储csv的文件夹，然后授权可以进行读写
            hdfs dfs -chmod 777 /csv(这是我的，集群上面不知道好不好用)
        
    '''
    conf = SparkConf().setAppName("spark_example")
    sc = SparkContext(conf=conf)
    hiveCtx = HiveContext(sc)
    # hdfs中存储txt文件的文件夹，只需要指明文件夹即可
    path_txt = "/txt"
    path_csv = '/csv'
    # 连接hdfs,用于获得Hdfs中的txt文件名，同时构造读写路径
    client = Client("http://192.168.191.82:50070", root="/", timeout=10000, session=False)
    # 表头
    name_lists = [
        ['CantonCode', 'CantonName', 'ZoneCode', 'UpperCode', 'Level',
         'AllName',
         'DT'],
        ['DrugCode', 'DrugName', 'DrugBrevityCode', 'OtherName', 'DosageTypeCode',
         'DrugType', 'DrugCatalog', 'Usage', 'Memo', 'CatalogClass', 'CompRatio',
         'CompRatio_Type', 'UseDrugClass', 'RelativeCode', 'ItemCode', 'Country'],
        ['HosRegisterCode', 'TotalFee', 'RealComp', 'SelfPay', 'SettlementDate',
         'Ecbc', 'Zfdbbz', 'Mzbcje', 'DT'],
        ['HosRegisterCode', 'CantonCode', 'FamilyCode', 'PersonalCode',
         'Name', 'Sex', 'Age', 'CertificateCode', 'InstitutionCode',
         'DiseaseCode', 'OperationCode', 'InHosDate', 'OutHosDate',
         'RegisterDate', 'DT'],
        ['PersonalCode', 'FamilyCode', 'Name', 'Gender', 'Age',
         'CantonCode', 'PersonalType', 'CertificateCode', 'Brithday', 'Folk',
         'DT'],
        ['ItemCode_Vc',
         'DrugName',
         'ItemBrevityCode',
         'ItemType',
         'CompRatio',
         'CompRatio_Type',
         'ItemCode'],
        ['Type',
         'Code',
         'Desc',
         'DT'],
        ['HosRegisterCode',
         'PrescriptionCode',
         'ItemIndex',
         'ItemCode',
         'ItemName',
         'ItemType',
         'DrugCatalog',
         'Count',
         'FeeSum',
         'AllowedComp',
         'UnallowedComp',
         'CompRatio',
         'DT']]
    add_header(path_txt, path_csv,name_lists)

