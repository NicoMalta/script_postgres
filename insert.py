import pandas as pd
import random
import csv

finalList = []


def generateEmail(name, surname):
    return name + "_" + surname + "@gmail.com"

def generateRandom(dataFrame):
    size = dataFrame.size
    randomNumber = random.randint(0, size)
    return dataFrame.iloc[randomNumber]

def insert_data(nameTable, nameData1, iteration):
    insert_query = " INSERT INTO " + nameTable + "VALUES  "

    dataSet1 = pd.read_csv('data/' + nameData1 + '.csv', header=None, usecols=[0])
    
    query = ""
    for x in range(iteration):
        query = query   + "('" + generateRandom(dataSet1)  +  "'),"
    
    query = query[:-1]
    return insert_query + query

def insert_data(nameTable, nameData1, nameData2, iteration):
    insert_query = " INSERT INTO " + nameTable + "VALUES  "

    dataSet1 = pd.read_csv('data/' + nameData1 + '.csv', header=None, usecols=[0])
    dataSet2 = pd.read_csv('data/' + nameData2 + '.csv', header=None, usecols=[0])
    
    query = ""
    for x in range(iteration):
        query = query   + "('" + generateRandom(dataSet1)  + "','" + generateRandom(dataSet2) +  "'),"
    
    query = query[:-1]
    return insert_query + query
    

def insert_data(nameTable, nameData1, nameData2, nameData3, iteration):
    insert_query = " INSERT INTO " + nameTable + "VALUES  "

    dataSet1 = pd.read_csv('data/' + nameData1 + '.csv', header=None, usecols=[0])
    dataSet2 = pd.read_csv('data/' + nameData2 + '.csv', header=None, usecols=[0])
    dataSet3 = pd.read_csv('data/' + nameData3 + '.csv', header=None, usecols=[0])
    
    query = ""
    for x in range(iteration):
        query = query   + "('" + generateRandom(dataSet1)  + "','" + generateRandom(dataSet2) + "','" +  generateRandom(dataSet3)  +   "'),"
    
    query = query[:-1]
    return insert_query + query

def insert_data(nameTable, nameData1, nameData2, nameData3, nameData3, iteration):
    insert_query = " INSERT INTO " + nameTable + "VALUES  "

    dataSet1 = pd.read_csv('data/' + nameData1 + '.csv', header=None, usecols=[0])
    dataSet2 = pd.read_csv('data/' + nameData2 + '.csv', header=None, usecols=[0])
    dataSet3 = pd.read_csv('data/' + nameData3 + '.csv', header=None, usecols=[0])
    dataSet4 = pd.read_csv('data/' + nameData4 + '.csv', header=None, usecols=[0])

    
    query = ""
    for x in range(iteration):
        query = query   + "('" + generateRandom(dataSet1)  + "','" + generateRandom(dataSet2) + "','" +  generateRandom(dataSet3)  + "','" +  generateRandom(dataSet4) + "'),"
    
    query = query[:-1]
    return insert_query + query