# -*- coding: utf-8 -*-
"""
Created on Thu May 26 23:42:33 2016

@author: yusufazishty
"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def println(x):
    print(x)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("fetched_data/fakefriends.csv")
rdd = lines.map(parseLine)
rdd.foreach(println)
totalsByAge = rdd.mapValues(lambda x: (x, 1))
totalsByAge.foreach(println)
totalAgeReduced = totalsByAge.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
totalAgeReduced.foreach(println)
#averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
#results = averagesByAge.collect()
#for result in results:
#    print(result)
