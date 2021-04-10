import time
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row


def convert_float(v):
    return float(v)


# 处理预测目标值
def process_label(line):
    return float(line[-1])


def process_features(line):
    """处理特征,line为字段行"""
    # 处理季节特征
    SeasonFeature = [convert_float(value) for value in line[2]]
    # 处理余下的特征
    Features = [convert_float(value) for value in line[4: 14]]
    return Features


# 定义模型评估函数
def RMSE(model, validationData):
    # 计算模型准确率
    predict = model.predict(validationData.map(lambda p: p.features))

    # 拼接预测值和实际值
    predict_real = predict.zip(validationData.map(lambda p: p.label))

    rmse = np.sqrt(predict_real.map(lambda p: (p[0] - p[1]) ** 2).sum() / predict_real.count())

    return rmse


# 创建trainEvaluateModel函数包含训练评估功能，并计算训练评估的时间
def trainEvaluateModel(trainData, validationData, maxDepthParm, maxBinsParm, minInstancesPerNodeParm, minInfoGainParm):
    startTime = time.time()
    # 创建并训练模型
    model = DecisionTree.trainRegressor(trainData, categoricalFeaturesInfo={}, impurity="variance",
                                        maxDepth=maxDepthParm,
                                        maxBins=maxBinsParm, minInstancesPerNode=minInstancesPerNodeParm,
                                        minInfoGain=minInfoGainParm)
    # 计算RMSE
    rmse = RMSE(model, validationData)
    duration = time.time() - startTime  # 持续时间
    print("训练评估：参数" + ",  maxDepth=" + str(maxDepthParm) + ",  maxBins=" + str(maxBinsParm) +
          ", minInstancesPerNode=" + str(minInstancesPerNodeParm) + ", minInfoGainParm=" + str(minInfoGainParm) + "\n"
                                                                                                                  "===>消耗时间=" + str(
        duration) + ",  均方误差RMSE=" + str(rmse))
    return rmse, duration, maxDepthParm, maxBinsParm, minInstancesPerNodeParm, minInfoGainParm, model


def dealData(path):
    rawData = sc.textFile(path + 'hour.csv')
    header = rawData.first()
    rData = rawData.filter(lambda x: x != header)

    lines = rData.map(lambda x: x.split(","))
    labelpointRDD = lines.map(lambda r: LabeledPoint(process_label(r),
                                                     process_features(r)))
    print(labelpointRDD.first())
    # 划分训练集、验证集和测试集
    (trainData, validationData, testData) = labelpointRDD.randomSplit([7, 1, 2])
    print("训练集样本个数：" + str(trainData.count()) + " 验证集样本个数：" + str(validationData.count()) + " 测试集样本个数："
          + str(testData.count()))

    # 将数据暂存在内存中，加快后续运算效率
    trainData.persist()
    validationData.persist()
    testData.persist()

    model = DecisionTree.trainRegressor(trainData, categoricalFeaturesInfo={}, impurity="variance", maxDepth=5,
                                        maxBins=32, minInstancesPerNode=1, minInfoGain=0.0)

    rmse = RMSE(model, validationData)
    print("均方误差RMSE=" + str(rmse))

    ## 评估参数 maxDepth
    maxDepthList = [3, 5, 10, 15, 20, 25]
    maxBinsList = [10]
    minInstancesPerNodeList = [1]
    minInfoGainList = [0.0]

    ## 返回结果存放至metries中
    metrics = [trainEvaluateModel(trainData, validationData, maxDepth, maxBins, minInstancesPerNode, minInfoGain)
               for maxDepth in maxDepthList
               for maxBins in maxBinsList
               for minInstancesPerNode in minInstancesPerNodeList
               for minInfoGain in minInfoGainList]


if __name__ == '__main__':
    conf = SparkConf().setAppName("spark_example")
    sc = SparkContext(conf=conf)
    file = 'file:/home/hadoop/PycharmProjects/pythonProject/data/'
    dealData(file)
