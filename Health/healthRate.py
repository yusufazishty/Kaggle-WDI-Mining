# -*- coding: utf-8 -*-
"""
Created on Thu May 26 22:02:34 2016

@author: yusufazishty
"""
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
import math
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
from pyspark.ml.clustering import KMeans
import pandas as pd

conf = SparkConf()
conf.set("spark.executor.memory", "2g")
conf.set("spark.cores.max","4")
conf.setAppName("health_rate_clustering")
# Initialize only once
sc = SparkContext('local', conf=conf)
SQLContext = SQLContext(sc)

def println(x):
    print(x)

def transform_data(line):   
    Code = line.CountryCode
    Value = float(line.Value)
    result = (Code, Value)
    return result

def transform_data1(line):   
    Code = line.CountryCode
    IndicatorCode = line.IndicatorCode
    Value = float(line.Value)
    result = (Code, Value, IndicatorCode)
    return result


raw_lines = SQLContext.read.format('json').load("death_rate.json")
rate = raw_lines.map(transform_data)
deathRate = rate.mapValues(lambda x: (x,1))
deathRateCollated = deathRate.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+ y[1]))
deathRateCollated = deathRateCollated.mapValues(lambda x: x[0]/ x[1])
flipped = deathRateCollated.map(lambda (x,y):(y,x))
deathRateCollated = flipped.sortByKey()
deathRateCollated.cache()
deathRateCollated.collect()
print("RDD Data collected")
deathRateCollated.foreach(println)


