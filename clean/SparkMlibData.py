#! /home/hadoop/.conda/envs/spark/bin python
# -*- encoding: utf-8 -*-
"""
@File    :   SparkMlibData.py 
@License :   (C)Copyright 2020-2021, 毕业设计

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/5/31 下午4:50   zhl      1.0         None
"""
import time
import math
import pymysql
import numpy as np
import pandas as pd
from fbprophet import Prophet
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
import CreateData as CD
from pyspark import SparkContext
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import DecimalType, IntegerType, StructType, StructField, FloatType


def getDataDecisionTree(path):
    data = spark.read.format("parquet").load(path).select("HosRegisterCode", "DiseaseName")
    print(data.count())
    print(data.columns)

    temp_diseases = data.select("DiseaseName").toPandas().values.tolist()
    temp_diseases = sum(temp_diseases, [])

    diseaseNames = list(set(temp_diseases))

    df = data.withColumn("Count", F.lit(1))

    df = df.groupby("HosRegisterCode") \
        .pivot("DiseaseName", diseaseNames) \
        .agg(F.sum("Count")) \
        .fillna(0)

    df.show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("test") \
        .config("spark.debug.maxToStringFields", "100") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.default.parallelism", "600") \
        .config("spark.sql.auto.repartition", "true") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    inputFile = 'hdfs://localhost:9000/result/form_par_new'
    allFile = 'hdfs://localhost:9000/result/form_par_all'
    tree = "hdfs://localhost:9000/data/data_tree"
    getDataDecisionTree(inputFile)
