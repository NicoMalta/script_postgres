import pandas as pd
import psycopg2
from datetime import datetime
from utilities import random_date
import random


def insert_activities():
    postgres_insert_query = " INSERT INTO actividad (descripcion, fecha_inicio) VALUES  "
    activities = pd.read_csv('data/activities.csv', header=None, usecols=[0])
    aux_string =  ""
    d1 = datetime.strptime('1/1/2008 1:30 PM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('1/1/2020 4:50 AM', '%m/%d/%Y %I:%M %p')
    index = random.randint(0,(activities.count()[0]-1))
    aux_string = aux_string   + "('" + activities[0][index]  + "','" + random_date(d1,d2).strftime("%Y-%m-%d %H:%M:%S") + "'),"
    aux_string = aux_string[:-1]
    return postgres_insert_query + aux_string

def insert_employees():
    postgres_insert_query = " INSERT INTO empleado (nombre,apellido,direccion,sueldo,fecha_ingreso) VALUES  "
    names = pd.read_csv('data/names.csv', header=None, usecols=[0])
    surnames = pd.read_csv('data/surnames.csv', header=None , usecols=[0])
    aux_string = ""
    d1 = datetime.strptime('1/1/2008 1:30 PM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('1/1/2020 4:50 AM', '%m/%d/%Y %I:%M %p')
    index_names = random.randint(0,(names.count()[0]-1))
    index_surnames = random.randint(0,(surnames.count()[0]-1))
    salary = random.randint(10000,40000)
    aux_string = aux_string  + "('" + names[0][index_names]  + "','" + surnames[0][index_surnames] + "','Eduardo oliber 437','" + str(salary) + "','" + random_date(d1,d2).strftime("%Y-%m-%d %H:%M:%S") + "'),"
    aux_string = aux_string[:-1]
    return postgres_insert_query + aux_string

def insert_specialties():
    pass
    
def main(_user,_pw):
    try:
        connection = psycopg2.connect(user=_user,
                                        password=_pw,
                                        host="127.0.0.1",
                                        port="5432",
                                        database="recursos_humanos")
        cursor = connection.cursor()

        query = insert_activities() + "; \n"
        query += insert_employees() + "; \n"
    
        cursor.execute(query)

        connection.commit()
        count = cursor.rowcount
        print (count, "Record inserted successfully")

    except (Exception, psycopg2.Error) as error :
        if(connection):
            print("Failed to insert record", error)

    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


main("malta","malta")