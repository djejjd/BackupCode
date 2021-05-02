#! /home/hadoop/.conda/envs/spark/bin python
# -*- encoding: utf-8 -*-
"""
@File    :   getData.py 
@License :   (C)Copyright 2020-2021, 毕业设计

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/4/20 下午4:02   zhl      1.0         None
"""
import time
import pymysql
import numpy as np
import pandas as pd
from fbprophet import Prophet
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType, StructType, StructField


def getConn():
    db = pymysql.connect(host='localhost', user='warren', password='123456', db='spark',
                         port=3306, charset='utf8')
    return db


def cleanData(path):
    """
    :param path: 大表文件路径 
    :return: dataframe
    """
    data = spark.read.format('parquet').load(path) \
        .select("HosRegisterCode", "RegisterDate", "TotalFee") \
        .dropDuplicates(subset=['HosRegisterCode']) \
        .drop("HosRegisterCode")

    data = data.withColumn("RegisterDate", F.to_date(data.RegisterDate.substr(1, 10)))
    data = data.orderBy(data['RegisterDate'].asc())
    data = data.groupby("RegisterDate").agg(F.sum("TotalFee").alias("TotalFee"))
    data.repartition(1).write.format('csv').option("header", "true").mode("overwrite") \
        .save("/home/hadoop/data/sparkMlib/TotalFee")


# 定义获取函数
def get_df_from_db(sql):
    conn = getConn()
    cursor = conn.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    """
    使用fetchall函数以元组形式返回所有查询结果并打印出来
    fetchone()返回第一行，fetchmany(n)返回前n行
    游标执行一次后则定位在当前操作行，下一次操作从当前操作行开始
    """
    data = cursor.fetchall()

    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df

    """
    使用完成之后需关闭游标和数据库连接，减少资源占用,cursor.close(),db.close()
    db.commit()若对数据库进行了修改，需进行提交之后再关闭
    """
    cursor.close()
    conn.close()

    return df


def getDataMysql():
    sql = "SELECT * FROM predictFee"  # SQL语句
    dff = get_df_from_db(sql)
    return dff


def createHolidays():
    # 加入部分holidays
    guoqing = pd.DataFrame({
        'holiday': 'guoqing',
        #   'ds': pd.to_datetime(['2017-10-01', '2018-10-01', '2019-10-01']),
        'ds': pd.to_datetime(['2017-10-01', '2018-10-01']),
        'lower_window': 0,
        'upper_window': 7,
    })
    yuandan = pd.DataFrame({
        'holiday': 'yuandan',
        'ds': pd.to_datetime(['2017-01-01', '2018-01-01', '2019-01-01']),
        'lower_window': 0,
        'upper_window': 1,
    })
    cunjie01 = pd.DataFrame({
        'holiday': 'cunjie',
        'ds': pd.to_datetime(['2017-01-27']),
        'lower_window': 0,
        'upper_window': 7,
    })
    cunjie02 = pd.DataFrame({
        'holiday': 'cunjie',
        'ds': pd.to_datetime(['2018-02-15']),
        'lower_window': 0,
        'upper_window': 7,
    })
    cunjie03 = pd.DataFrame({
        'holiday': 'cunjie',
        'ds': pd.to_datetime(['2019-02-04']),
        'lower_window': 0,
        'upper_window': 7,
    })
    qingming01 = pd.DataFrame({
        'holiday': 'qingming',
        'ds': pd.to_datetime(['2017-04-02']),
        'lower_window': 0,
        'upper_window': 3,
    })
    qingming02 = pd.DataFrame({
        'holiday': 'qingming',
        'ds': pd.to_datetime(['2018-04-05', '2019-04-05']),
        'lower_window': 0,
        'upper_window': 3,
    })
    laodong01 = pd.DataFrame({
        'holiday': 'laodong',
        'ds': pd.to_datetime(['2017-05-01', '2019-05-01']),
        'lower_window': 0,
        'upper_window': 3,
    })
    laodong02 = pd.DataFrame({
        'holiday': 'cunjie',
        'ds': pd.to_datetime(['2018-04-29']),
        'lower_window': 0,
        'upper_window': 3,
    })
    holiday = [guoqing, yuandan, cunjie01, cunjie02, cunjie03, qingming01, qingming02, laodong01, laodong02]
    holidays = pd.concat(holiday)
    return holidays


def predictFuture():
    df = getDataMysql()
    pd.set_option('display.float_format', lambda x: '%.3f' % x)
    # df['y'] = np.log(df['y'])
    fdays = 10
    # find_params(df, 10)
    # 划分数据集和训练集
    # df['ds'] = pd.to_datetime(df['ds'], format="%Y/%m/%d")
    # df_train = df.loc[df.ds < "2019-06-01"]
    # df_test = df.loc[df.ds >= "2019-06-01"]
    train_size = len(df) - fdays
    df_train, df_test = df[0:train_size], df[train_size:len(df)]

    # 获得节假日
    holidays = createHolidays()

    # 建立模型
    m = Prophet(holidays=holidays, seasonality_mode='multiplicative', holidays_prior_scale=20, seasonality_prior_scale=0.3)
    m.fit(df_train)

    # 预测
    testData = pd.DataFrame(df_test, columns=["ds"])
    forecast = m.predict(df=testData)
    print(df_test)
    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])


# 根据参数进行一次预测，并算出预测的误差
def predict_one(train_df, test_df, params):
    m = Prophet(**params)
    m.fit(train_df)
    testData = pd.DataFrame(test_df, columns=['ds'])
    forecast = m.predict(df=testData)
    #     forecast = m.predict(future).tail(fdays)
    forecast.index = forecast['ds'].map(lambda x: x.strftime('%Y-%m-%d'))
    temp_test_df = test_df
    temp_test_df.index = test_df['ds'].map(lambda x: x.strftime('%Y-%m-%d'))

    return mean_forecast_err(test_df['y'], forecast['yhat'])


# 寻参:为简化for次数，这里只列出两个参数，实际需要迭代的会更多
def find_params(df, fdays):
    import sys
    # 划分数据集和训练集
    df['ds'] = pd.to_datetime(df['ds'], format="%Y/%m/%d")
    df_train = df.loc[df.ds < "2019-06-01"]
    df_test = df.loc[df.ds >= "2019-06-01"]
    train_size = len(df) - fdays
    # df_train, df_test = df[0:train_size], df[train_size:len(df)]
    # df_train = df.sample(frac=0.6, random_state=0, axis=0)
    # df_test = df[~df.index.isin(df_train.index)]
    seasonality_modes = ['additive', 'multiplicative']
    seasonality_prior_scales = [0.01,0.03, 0.1, 0.3, 1, 3, 10,30]
    holidays_prior_scales = [0.01, 0.05, 0.2, 1, 5, 10, 20]  # 事实上可以加入更多参数，这里略去
    min_err = sys.maxsize
    best_params = None
    for mode in seasonality_modes:
        for sp_scale in seasonality_prior_scales:
            for hp_scale in holidays_prior_scales:
                params = {
                    "seasonality_mode": mode,
                    "seasonality_prior_scale": sp_scale,
                    "holidays_prior_scale": hp_scale
                }
                avg_err = predict_one(df_train, df_test, params)

                if avg_err < min_err:
                    min_err = avg_err
                    best_params = params

                print(f'{params}:{avg_err}')

    print(f'最小平均误差:{min_err},最优参数:{best_params}')


# 计算平均误差
def mean_forecast_err(y, yhat):
    return y.sub(yhat).abs().mean()


def test():
    import pystan
    model_code = 'parameters {real y;} model {y ~ normal(0,1);}'
    model = pystan.StanModel(model_code=model_code)
    y = model.sampling(n_jobs=1).extract()['y']
    print(y.mean())  # 结果应当在 0 附近


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
    # tableFile = 'hdfs://localhost:9000/result/form_par_all'
    # cleanData(tableFile)
    # test()
    predictFuture()
