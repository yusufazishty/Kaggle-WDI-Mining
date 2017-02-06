# -*- coding: utf-8 -*-
"""
Created on Thu May 26 22:02:34 2016

@author: ihsanp
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
conf.setAppName("current_gni_clustering")
# Initialize only once
sc = SparkContext('local', conf=conf)
SQLContext = SQLContext(sc)

def println(x):
    print(x)
 
def transform_data(line):   
    Code = line.CountryCode
    Value = line.Value
    result = (Code, Value)
    return result
    
def transformToVector(inputLine):
    Values = Vectors.dense(inputLine[1])
    return Values
    
def centerAndScale(inVector):
    global bcMeans
    global bcStdDev
    meanArray = bcMeans.value
    stdArray= bcStdDev.value
    
    valueArray=inVector.toArray()    
    retArray=[]
    for i in range(valueArray.size):
        retArray.append((valueArray[i]-meanArray[i]/stdArray[i]))
    return Vectors.dense(retArray)
    
def unstripData(instr):
    return(instr["prediction"], instr["features"][0])
    
def save_txt(dataToSave,fileName):
    head="CountryCode;CountryName;Hubungan;Cluster\n"
    with open(fileName, 'w') as txtfile:
        txtfile.write(head)
        for i in range(len(dataToSave)):
            try :
                line=str(dataToSave[i][0])+";"+str(dataToSave[i][1])+";"+str(dataToSave[i][2])+";"+str(dataToSave[i][3])+"\n"
            except IndexError as detail:
                    print(detail)
                    print(i)      
            txtfile.write(line)  
    txtfile.close()

# Read Data, SQLContext.read.format is reading to RDD BRUH!
# Get CountryCode and Value
current_gni_lines = SQLContext.read.format('json').load("fetched_data/current_gni.json")
current_gni_rdd = current_gni_lines.map(transform_data)
current_gni_rdd.cache()
current_gni_rdd.collect()
print("RDD Data collected")
current_gni_rdd.foreach(println)

#Map the values
current_gni_maped = current_gni_rdd.mapValues(lambda x: (x,1))
current_gni_maped.cache()
current_gni_maped.collect()
print("Mapped RDD Data")
current_gni_maped.foreach(println)

#Reduced By Key
current_gni_reduced = current_gni_maped.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
current_gni_reduced.cache()
current_gni_reduced.collect()
print("Reduced RDD Data")
current_gni_reduced.foreach(println)

#Get The Average
current_gni_final_maped = current_gni_reduced.mapValues(lambda x: x[0] / x[1])
current_gni_final_maped.cache()
current_gni_final_maped.collect()
print("Average per Country collected")
#current_gni_final_maped.foreach(println)

# Make Vector the data
autoVector = current_gni_final_maped.map(transformToVector)
autoVector.persist()
autoVector.collect()
print("Vectorized Average")
autoVector.foreach(println)

# Centering and scaling, substract every colom with that colomn means, and divided by its std. deviation
autoStats = Statistics.colStats(autoVector)
colMeans=autoStats.mean() #resulting numpy array
print("Means:")
print(colMeans)

colVariance=autoStats.variance()
print("Variances:")
print(colVariance)

colStdDev=map(lambda x: math.sqrt(x), colVariance)
#colStdDev.collect()
print("StdDev:")
#colStdDev.foreach(println)
print(colStdDev)

#Place the means and std.dev values in a broadcast variable
bcMeans = sc.broadcast(colMeans)
bcStdDev = sc.broadcast(colStdDev)
csAuto = autoVector.map(centerAndScale)
#csAuto.collect()
#csAuto.foreach(println)
print(csAuto)

#Create Spark Data Frame
autoRows = csAuto.map(lambda f:Row(features=f))
autoDf = SQLContext.createDataFrame(autoRows)
autoDf.select("features").show(10)

kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(autoDf)
predictions = model.transform(autoDf)
predictions.collect()
predictions.foreach(println)

#Plot the results in a scatter plot
unstripped = predictions.map(unstripData)
predList=unstripped.collect()
predPd = pd.DataFrame(predList)

# preparing to save the clustered data
list_current_gni_final_maped = current_gni_final_maped.collect()
list_current_gni_rdd = current_gni_rdd.collect()
list_predictions_pandas=predictions.toPandas()
list_predictions_temp=list_predictions_pandas.as_matrix()

list_predictions=[]
for i in range(len(list_predictions_temp)):
    line=[]
    line.append(float(list_predictions_temp[i][0][0]))
    line.append(float(list_predictions_temp[i][1]))
    #print(line)
    list_predictions.append(line)
#print(list_predictions)

listrik_lines=current_gni_lines.collect()
CountryCode=[]
Country=[]
for i in range(len(listrik_lines)):
    if listrik_lines[i].CountryCode not in CountryCode:
        CountryCode.append(listrik_lines[i].CountryCode)
    if listrik_lines[i].Country not in Country:
        Country.append(listrik_lines[i].Country)    
    
data_result_predicted=[]
for i in range(current_gni_final_maped.count()):
    NameIdx = CountryCode.index(list_current_gni_final_maped[i][0])
    CountryName = Country[NameIdx]
    data=[str(list_current_gni_final_maped[i][0]), CountryName, float(list_predictions[i][0]), int(list_predictions[i][1])]
    data_result_predicted.append(data)
"""
bcMeans_list_temp=[bcMeans.value]
bcMeans_list=[]
for i in range(len(bcMeans_list_temp[0])):
    bcMeans_list.append(float(bcMeans_list_temp[0][i]))
bcMeans_list=[bcMeans_list]"""

#save the clustered data
save_txt(data_result_predicted, "current_gni_cluster.txt")
print("current_gni_cluster.txt saved")

#plot the clustered dataimport matplotlib.pylab as plt
import matplotlib.pylab as plt
plt.cla()
plt.scatter(predPd[1], predPd[0])
plt.show()
