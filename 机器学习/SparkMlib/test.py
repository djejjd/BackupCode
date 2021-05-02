#! /home/hadoop/.conda/envs/spark/bin python
# -*- encoding: utf-8 -*-
"""
@File    :   test.py 
@License :   (C)Copyright 2020-2021, 毕业设计

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/4/20 下午4:06   zhl      1.0         None
"""
import time
import math
import pymysql
import numpy as np
import pandas as pd
from fbprophet import Prophet
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import DecimalType, IntegerType, StructType, StructField, FloatType


def fp(x):
    rel = {'feature': Vectors.dense(float(x[0]), float(x[1]), float(x[2]))}
    return rel


def getDistance(listPoint, listCenter):
    length = []
    for i in listPoint:
        p = i[2]
        # x --> age, y--> TotalFee
        x = i[0] - listCenter[p][0]
        y = i[1] - listCenter[p][1]
        ll = math.sqrt((x ** 2) + (y ** 2))
        length.append(ll)

    med = np.median(np.array(length))
    lengthPoint = []
    y = []

    # 获取相对距离
    t = 1
    for i in length:
        p = i / med
        y.append(p)
        lengthPoint.append([t, p])
        t = t + 1

    y_min = np.array(y).mean() - 2 * np.array(y).std()
    y_max = np.array(y).mean() + 2 * np.array(y).std()
    yMin = np.array(y).mean() - 3 * np.array(y).std()
    yMax = np.array(y).mean() + 3 * np.array(y).std()

    t = 0
    new_lengthPoint = []
    new_diseaseInfo = []
    for i in lengthPoint:
        x = i[0]
        y = i[1]
        if y_min <= y <= y_max:
            t += 1
            if t == 10:
                print(lengthPoint[x - 1])
                print(listPoint[x - 1])
                new_lengthPoint.append(lengthPoint[x - 1])
                new_diseaseInfo.append(listPoint[x - 1])
                t = 0


def train(k, path):
    # fileSave = "/home/hadoop/data_school/sparkMlib/KMeans"
    # # 男: 1, 女: 2
    # df = spark.read.format('csv').option('header', 'true').load(fileSave).fillna('0')
    df = createDataframeKMeans(path).fillna('0')
    df = df.where(df.TotalFee != '0').where(df.DiseaseCode == '13104')
    df = df.withColumn("Age", df.Age.cast(IntegerType())) \
        .withColumn("TotalFee", df.TotalFee.cast(FloatType()))

    # vecAss = VectorAssembler(inputCols=df.columns[2:], outputCol='feature')
    # data = vecAss.transform(df).select("feature")
    # data.show()
    data = df.drop("DiseaseCode")
    data.show()

    # 转换数据
    featureCreator = VectorAssembler(inputCols=data.columns[1:], outputCol='feature')
    data = featureCreator.transform(data)

    # 评估器
    kmeans = KMeans(k=k, featuresCol='feature')

    # 模型拟合
    model = kmeans.fit(data)
    # 聚合
    test = model.transform(data)
    test.show()
    points = []
    for i in test.select("Age", "TotalFee", "prediction", "HosRegisterCode").collect():
        temp = [float(i['Age']), float(i['TotalFee']), int(i['prediction']), i['HosRegisterCode']]
        points.append(temp)

    centers = model.clusterCenters()
    model.save("/home/hadoop/PycharmProjects/SparkMlib/model/kmeans")

    # centerPoints = []
    # for i in centers:
    #     temp = [float(i[0]), float(i[1])]
    #     centerPoints.append(temp)
    # temp = getDistance(points, centerPoints)
    # # lengthList = temp[0]
    # # med = temp[1]
    #
    # kMeansChartShow(temp[0], temp[1])


def kMeansChartShow(lengthList, med):
    x = []  # 编号
    y = []  # 距离
    y1 = []
    for i in lengthList:
        x.append(i[0])
        y.append(i[1])
        y1.append(i[1] / med)

    y_min = np.array(y).mean() - 2 * np.array(y).std()
    y_max = np.array(y).mean() + 2 * np.array(y).std()
    yMin = np.array(y).mean() - 3 * np.array(y).std()
    yMax = np.array(y).mean() + 3 * np.array(y).std()

    import matplotlib.pyplot as plt
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.axhline(y=y_min, linestyle='--', color='red')
    ax.axhline(y=y_max, linestyle='--', color='red')
    ax.axhline(y=yMin, linestyle='--', color='red')
    ax.axhline(y=yMax, linestyle='--', color='red')
    ax.set_xlabel("编号")
    ax.set_ylabel("距离")
    ax.scatter(x, y, c='k', marker='.')

    y_min = np.array(y1).mean() - 2 * np.array(y1).std()
    y_max = np.array(y1).mean() + 2 * np.array(y1).std()
    yMin = np.array(y1).mean() - 3 * np.array(y1).std()
    yMax = np.array(y1).mean() + 3 * np.array(y1).std()
    f = plt.figure()
    ax = f.add_subplot(1, 1, 1)
    ax.axhline(y=y_min, linestyle='--', color='red')
    ax.axhline(y=y_max, linestyle='--', color='red')
    ax.axhline(y=yMin, linestyle='--', color='red')
    ax.axhline(y=yMax, linestyle='--', color='red')
    ax.set_xlabel("编号")
    ax.set_ylabel("距离")
    ax.scatter(x, y1, c='k', marker='.')

    plt.show()


def chartShow():
    data = train()
    # ----Kmeans聚类----
    print("------------------Kmeans聚类--------------------")
    print("------------设定不同的K值，进行分类,计算平方误差之和------------")

    errors = []
    results = []
    centers = []

    for k in range(2, 10):
        # 获得模型
        kmeansmodel = KMeans().setK(k).setFeaturesCol('feature').setPredictionCol('prediction').fit(data)
        print("With K={}".format(k))

        # 带有预测簇标签的数据集
        kmeans_results = kmeansmodel.transform(data).collect()
        results.append(kmeans_results)
        #     for item in kmeans_results:
        #         print(item)
        #         print(str(item[0]) + ' is predcted as cluster' + str(item[1]))

        # 获取到模型的所有聚类中心情况
        kmeans_centers = kmeansmodel.clusterCenters()
        centers.append(kmeans_centers)
        center_seq = 0
        print(len(kmeans_centers))
        for item in kmeans_centers:
            print(item)
            #         print("Cluster" + str(center_seq) + "  Center" + str(item))
            center_seq = center_seq + 1

        # 计算集合内误差平方和（Within Set Sum of Squared Error, WSSSE)
        WSSSE = kmeansmodel.computeCost(data)
        errors.append(WSSSE)
        print("Within Set Sum of Squared Error = " + str(WSSSE))

        print('--' * 30 + '\n')

    # ----WSSSE可视化----
    plt.figure()
    k_number = range(2, 10)
    plt.plot(k_number, errors)
    plt.xlabel('Number of K')
    plt.ylabel('WSSSE')
    plt.title('K-WSSSE')

    # ----聚类结果可视化----
    print("---------将数据转换为panda结构，并查看空间3d图心-----------")
    # 通过K-WSSSE图，k=6时聚类效果较好
    k = 4

    cluster_vis = plt.figure(figsize=(10, 10)).gca(projection='3d')

    for item in results[k - 2]:
        if item[1] == 0:
            cluster_vis.scatter(item[0][0], item[0][1], item[0][2], c='b')  # blue
        if item[1] == 1:
            cluster_vis.scatter(item[0][0], item[0][1], item[0][2], c='y')  # yellow
        if item[1] == 2:
            cluster_vis.scatter(item[0][0], item[0][1], item[0][2], c='m')  # magenta
        if item[1] == 3:
            cluster_vis.scatter(item[0][0], item[0][1], item[0][2], c='k')  # black
        if item[1] == 4:
            cluster_vis.scatter(item[0][0], item[0][1], item[0][2], c='g')  # green
        if item[1] == 5:
            cluster_vis.scatter(item[0][0], item[0][1], item[0][2], c='c')  # cyan

    for item in centers[k - 2]:
        cluster_vis.scatter(item[0], item[1], item[2], c='r', marker='p')  # red,五角

    plt.show()


def newTrain():
    fileSave = "/home/hadoop/data_school/sparkMlib/KMeans"
    # 男: 1, 女: 2
    df = spark.read.format('csv').option('header', 'true').load(fileSave).fillna('0')
    df = df.where(df.TotalFee != '0').where(df.DiseaseCode == '13104')
    df = df.withColumn("Sex", df.Sex.cast(IntegerType())) \
        .withColumn("Age", df.Age.cast(IntegerType())) \
        .withColumn("TotalFee", df.TotalFee.cast(FloatType()))

    # vecAss = VectorAssembler(inputCols=df.columns[2:], outputCol='feature')
    # data = vecAss.transform(df).select("feature")
    # data.show()
    data = df.drop("Sex", "DiseaseCode")
    data.show()

    # 转换数据
    featureCreator = VectorAssembler(inputCols=data.columns, outputCol='features')
    data = featureCreator.transform(data)

    distance = []
    for k in range(2, 10):
        # 评估器
        kmeans = KMeans(featuresCol='features').setK(k)
        # 模型拟合
        model = kmeans.fit(data)
        # 聚合
        test = model.transform(data).select('features', 'prediction')

        evaluator = ClusteringEvaluator()

        evaResult = evaluator.evaluate(test)
        print("the distance = " + str(evaResult))
        distance.append(evaResult)

    import matplotlib.pyplot as plt
    plt.figure()
    x = [i for i in range(2, 10)]
    plt.plot(x, distance)
    plt.xlabel("K")
    plt.ylabel("Distance")
    plt.show()


def createDataframeKMeans(path):
    data = spark.read.format('parquet').load(path).select("DiseaseCode", "HosRegisterCode", "Age", "TotalFee").fillna('0')
    return data


def useModel(path):
    model_path = '/home/hadoop/PycharmProjects/SparkMlib/model/kmeans'
    df = createDataframeKMeans(path)
    df = df.where(df.TotalFee != '0').where(df.DiseaseCode == '24495')
    df = df.withColumn("Age", df.Age.cast(IntegerType())) \
        .withColumn("TotalFee", df.TotalFee.cast(FloatType()))

    data = df.drop("DiseaseCode")
    data.show()

    # 转换数据
    featureCreator = VectorAssembler(inputCols=data.columns[1:], outputCol='feature')
    data = featureCreator.transform(data)

    model = KMeansModel.load(model_path)

    # 聚合
    test = model.transform(data)
    test.show()
    points = []
    for i in test.select("Age", "TotalFee", "prediction", "HosRegisterCode").collect():
        temp = [float(i['Age']), float(i['TotalFee']), int(i['prediction']), i['HosRegisterCode']]
        points.append(temp)

    centers = model.clusterCenters()

    centerPoints = []
    for i in centers:
        temp = [float(i[0]), float(i[1])]
        centerPoints.append(temp)
    getDistance(points, centerPoints)
    # lengthList = temp[0]
    # med = temp[1]
    #
    # kMeansChartShow(temp[0], temp[1])


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
    # createDataframeKMeans(inputFile)
    # chartShow()
    # train(4, inputFile)
    # train(6)
    # newTrain()
    useModel(inputFile)
