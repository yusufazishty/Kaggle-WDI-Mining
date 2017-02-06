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
    

#Start fetching from the sqlite
SQL_PATH = "database.sqlite"    
Fields=["CountryCode", "Country", "Year", "Value"]

#1 Ambil data indikator %populasi terakses listrik     
query_1="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'EG.ELC.ACCS.ZS' ORDER BY CountryName"
akses_listrik = fetch_db(query_1,SQL_PATH)
akses_listrik_dict = get_dict(akses_listrik, Fields)
with open('fetched_data/akses_listrik.json', 'w') as fp:
    fp.write(akses_listrik_dict)
    fp.close()
    
#2 Ambil data indikator %populasi terakses bbm non padat     
query_2="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'EG.NSF.ACCS.ZS' ORDER BY CountryName"
akses_bbm = fetch_db(query_2,SQL_PATH)
akses_bbm_dict = get_dict(akses_bbm, Fields) 
#Edit the order with http://www.jsoneditoronline.org/
with open('fetched_data/akses_bbm.json', 'w') as fp:
    fp.write(akses_bbm_dict)
    fp.close()
    
#3 Indikator energi terbarukan 
#a Ambil data indikator hydro electricity     
query_3a="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'EG.ELC.HYRO.ZS' ORDER BY CountryName"
hydro = fetch_db(query_3a,SQL_PATH)
hydro_dict = get_dict(hydro, Fields) 
#Edit the order with http://www.jsoneditoronline.org/
with open('fetched_data/hydro.json', 'w') as fp:
    fp.write(hydro_dict)
    fp.close()

#b Ambil data natural gas electricityquery_3a="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'EG.ELC.HYRO.ZS' ORDER BY CountryName"
query_3b="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'EG.ELC.NGAS.ZS' ORDER BY CountryName"
gas=fetch_db(query_3b,SQL_PATH)
gas_dict = get_dict(gas, Fields)
#Edit the order with http://www.jsoneditoronline.org/
with open('fetched_data/gas.json', 'w') as fp:
    fp.write(gas_dict)
    fp.close()

# Cari persentase energi terbarukan tiap negara
energi_terbarukan = copy.deepcopy(gas)
for i in range(len(gas)):
    value=((energi_terbarukan[i][3]+hydro[i][3])/2)
    lst = list(energi_terbarukan[i])
    lst[3]=value
    tup=tuple(lst)
    energi_terbarukan[i]=tup
energi_terbarukan_dict = get_dict(energi_terbarukan, Fields)
#Edit the order with http://www.jsoneditoronline.org/
with open('fetched_data/energi_terbarukan.json', 'w') as fp:
    fp.write(energi_terbarukan_dict)
    fp.close()

#c Ambil data  coal electricity
query_3c="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'EG.ELC.COAL.ZS' ORDER BY CountryName"
coal=fetch_db(query_3c,SQL_PATH)
coal_dict = get_dict(coal, Fields)
#Edit the order with http://www.jsoneditoronline.org/
with open('fetched_data/coal.json', 'w') as fp:
    fp.write(coal_dict)
    fp.close()

#d Ambil data  oil electricity
query_3d="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'EG.ELC.PETR.ZS' ORDER BY CountryName"
oil=fetch_db(query_3d,SQL_PATH)
oil_dict = get_dict(oil, Fields)
#Edit the order with http://www.jsoneditoronline.org/
with open('fetched_data/coal.json', 'w') as fp:
    fp.write(coal_dict)
    fp.close()

energi_habis = copy.deepcopy(oil)
for i in range(len(oil)):
    value=((energi_habis[i][3]+coal[i][3])/2)
    lst = list(energi_habis[i])
    lst[3]=value
    tup=tuple(lst)
    energi_habis[i]=tup
energi_habis_dict = get_dict(energi_habis, Fields)
#Edit the order with http://www.jsoneditoronline.org/
with open('fetched_data/energi_habis.json', 'w') as fp:
    fp.write(energi_habis_dict)
    fp.close()

#e Ambil data nuclear electricity
query_3e="SELECT CountryName,CountryCode,Year,Value FROM Indicators WHERE IndicatorCode = 'EG.ELC.NUCL.ZS' ORDER BY CountryName"
nuclear=fetch_db(query_3e,SQL_PATH)
data=[]
for i in range(len(nuclear)):
    for j in range(41):    
       data.append(nuclear)     
        
nuclear_dict = get_dict(nuclear, Fields)


#Edit the order with http://www.jsoneditoronline.org/
with open('fetched_data/nuclear.json', 'w') as fp:
    fp.write(nuclear_dict)
    fp.close()
    
#Opening energi terbarukan, convert to dataset style, save to csv
def train_test(source_file,train_name, test_name):
    with open(source_file) as json_data:    
        data = json.load(json_data)
    #pprint(data)    
    all_CountryCode=[]
    all_Country=[]
    all_Year=[]
    all_Value=[]
    for i in range(len(data)):
        all_CountryCode.append(data[i]["CountryCode"])
        all_Country.append(data[i]["Country"])
        all_Year.append(data[i]["Year"])
        all_Value.append(data[i]["Value"])
    
    dataset=[]
    for i in range(len(all_Country)):
        line=[]
        line.append(all_Value[i]);line.append(all_Year[i]);
        line.append(all_CountryCode[i]);line.append(all_Country[i]);
        dataset.append(line)
    #print(dataset)
    
    train=[]
    test=[]
    for i in range(len(dataset)):
        if dataset[i][1]==2012:
            test.append(dataset[i])
        if dataset[i][1]!=2012:
            train.append(dataset[i])
    #print(train)
    #print(test)
            
    distinct_code=[]
    code_count=0
    distinct_country=[]
    country_count=0
    for i in range(len(dataset)):
        if dataset[i][2] not in distinct_code:
            distinct_code.append(dataset[i][2])
            code_count+=1
        if dataset[i][2] not in distinct_country:
            distinct_country.append(dataset[i][2])
            country_count+=1
    #print(distinct_code)
            
    #for i in range(len(distinct_code)):
    #    distinct_code[i]=(distinct_code[i],i)
    #    distinct_country[i]=(distinct_country[i],i)
    
    #Transform data train ke numeric semua    
    for i in range(len(train)):
        if train[i][0] in distinct_code:
            idx_dist_code = distinct_code.index(train[i][0])
            train[i][0] = idx_dist_code
            train[i][1] = idx_dist_code
    print(train)
    
    #Transform data test ke numeric semua    
    for i in range(len(test)):
        if test[i][0] in distinct_code:
            idx_dist_code = distinct_code.index(test[i][0])
            test[i][0] = idx_dist_code
            test[i][1] = idx_dist_code
    print(test)
    
    save_txt(train, train_name)
    save_txt(test, test_name)    

source=["fetched_data/energi_terbarukan.json","fetched_data/energi_habis.json","fetched_data/nuclear.json"]
train_name=["train_terbarukan.txt", "train_habis.txt", "train_nuclear.txt"]
test_name=["test_terbarukan.txt", "test_habis.txt", "test_nuclear.txt"]

for i in range(len(source)):
    train_test(source[i],train_name[i], test_name[i])
    
del akses_listrik, query_1, akses_bbm, query_2, hydro, query_3a, gas, query_3b, energi_terbarukan, coal, query_3c, oil, query_3d,energi_habis ,nuclear, query_3e
