# -*- coding: utf-8 -*-
"""
Created on Thu May 26 11:19:05 2016

@author: yusufazishty
"""
#For converting the sqlite to the json files, the dataframe in spark need json inputs
import sqlite3 as lite
import sys
import json
import copy
from pprint import pprint


# fetch from sqlite data
def fetch_db(query, data_path):
    con = None    
    try:
        con = lite.connect(data_path)
        
        cur = con.cursor()    
        cur.execute(query)
        data = cur.fetchall()
        return data
    except lite.Error as e:
        print("Error %s:" % e.args[0])
        sys.exit(1)        
    finally:        
        if con:
            con.close()

# save the fetched data from sql to csv            
def save_txt(dataToSave,fileName):
    #python 3
    #csvfile = open(fileName, 'w', newline='')
    #python 2
    #csvfile = open(fileName, 'wb')
    #Writer = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_ALL)
    #Writer.writerow([ str("CountryName"), str("CountryCode"), str("Year"), str("Value") ])    
    with open(fileName, 'w') as txtfile:   
        for i in range(len(dataToSave)):
            #print(locale.format('%.2f', float(dataToSave[i][3]), True))
            #Writer.writerow([ str(dataToSave[i][0]), str(dataToSave[i][1]), int(dataToSave[i][2]), locale.format('%.2f', float(dataToSave[i][3]), True) ])
            #Writer.writerow([ dataToSave[i][0], dataToSave[i][1], dataToSave[i][2], locale.format('%.2f', dataToSave[i][3], True) ])
            try :
                line=str(dataToSave[i][0])+";"+str(dataToSave[i][1])+";"+str(dataToSave[i][2])+";"+str(dataToSave[i][3])+"\n"
            except IndexError as detail:
                    print(detail)
                    print(i)
            
            txtfile.write(line)  
    txtfile.close()

def get_dict(sql_data, Fields):
    sql_data_list=[]
    for i in range(len(sql_data)):
        dicti = {Fields[0]:sql_data[i][1],
                 Fields[1]:sql_data[i][0],
                 Fields[2]:sql_data[i][2],
                 Fields[3]:sql_data[i][3]
                }
        sql_data_list.append(dicti)    
    sql_data_dict = json.JSONEncoder().encode(sql_data_list)
    return sql_data_dict
    
def get_dict1(sql_data, Fields):
    sql_data_list=[]
    for i in range(len(sql_data)):
        dicti = {Fields[0]:sql_data[i][1],
                 Fields[1]:sql_data[i][0],
                 Fields[2]:sql_data[i][2],
                 Fields[3]:sql_data[i][3],
                 Fields[4]:sql_data[i][4]
                }
        sql_data_list.append(dicti)    
    sql_data_dict = json.JSONEncoder().encode(sql_data_list)
    return sql_data_dict

#Start fetching from the sqlite
SQL_PATH = "database.sqlite"    
Fields1=["CountryCode", "Country", "Year", "Value", "IndicatorCode"]
Fields=["CountryCode", "Country", "Year", "Value"]
#1 Ambil data indikator %populasi terakses listrik     
#query_1="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'SP.DYN.CDRT.IN' ORDER BY CountryName"
#akses_listrik = fetch_db(query_1,SQL_PATH)
#akses_listrik_dict = get_dict(akses_listrik, Fields)
#with open('death_rate.json', 'w') as fp:
#    fp.write(akses_listrik_dict)
#    fp.close()
    
query_2="SELECT CountryName,CountryCode,Year,Value,IndicatorCode FROM Indicators WHERE lower(IndicatorName) like '%poverty%' ORDER BY CountryName"
akses_listrik = fetch_db(query_2,SQL_PATH)
akses_listrik_dict = get_dict1(akses_listrik, Fields1)
with open('poverty_rate.json', 'w') as fp:
    fp.write(akses_listrik_dict)
    fp.close()
    
#query_3="SELECT CountryName,CountryCode,Year,Value,IndicatorCode FROM Indicators WHERE lower(IndicatorName) like '%nutrition%' ORDER BY CountryName"
#akses_listrik = fetch_db(query_3,SQL_PATH)
#akses_listrik_dict = get_dict1(akses_listrik, Fields1)
#with open('poverty_rate.json', 'w') as fp:
#    fp.write(akses_listrik_dict)
#    fp.close()
    
