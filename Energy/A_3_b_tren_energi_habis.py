# -*- coding: utf-8 -*-
"""
Created on Mon May 30 13:43:00 2016

@author: yusufazishty
"""
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.regression import LinearRegression

conf = SparkConf()
conf.set("spark.executor.memory", "2g")
conf.set("spark.cores.max","4")
conf.setAppName("akses_bbm_clustering")
# Initialize only once
sc = SparkContext('local', conf=conf)
ssc = StreamingContext(sc, 1)
SQLContext = SQLContext(sc)

def println(x):
    print(x)
    
def parse_train(line):
    data = line.split(";")
    label = data[0]
    vector = Vectors.dense(data[1:])    
    return LabeledPoint(label, vector)

def parse_test(line):
    data = line.split(";")
    label = None
    vector = Vectors.dense(data[1:])    
    return LabeledPoint(label, vector)

    
# Read Data, SQLContext.read.format is reading to RDD BRUH!
# Get CountryCode and Value

#VECTORIZE TRAIN DATA
energi_habis_train = ssc.textFileStream("train_habis.txt")
energi_habis_train_labeled = energi_habis_train.map(parse_train)
energi_habis_train_labeled_DF = SQLContext.createDataFrame(energi_habis_train_labeled["label", "features"])
print(energi_habis_train_labeled_DF)

#VECTORIZE TEST DATA
energi_habis_test = ssc.textFileStream("test_habis.txt")
energi_habis_test_labeled = energi_habis_test.map(parse_test)
energi_habis_test_labeled_DF = SQLContext.createDataFrame(energi_habis_test_labeled["label", "features"])
print(energi_habis_test_labeled_DF)

#Create Model
numFeatures = 3
lr = LinearRegression(maxIter=50)
lrModel = lr.fit(energi_habis_train_labeled_DF)

#see what the model do
print("Coefficients: "+str(lrModel.coefficients))
print("Intercept: "+str(lrModel.intercept))

#Predict On the tested data
predictions = lrModel.transform(energi_habis_test_labeled_DF)
predictions.select("prediction","label", "features").show()

#Evaluate the predictions
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="r2")
evaluator.evaluate(predictions)