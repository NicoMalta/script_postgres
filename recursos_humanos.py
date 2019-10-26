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
    for i in range(21):
        aux_string = aux_string   + "('" + activities[0][i]  + "','" + random_date(d1,d2).strftime("%Y-%m-%d %H:%M:%S") + "'),"
    aux_string = aux_string[:-1]
    return postgres_insert_query + aux_string

def insert_employees():
    postgres_insert_query = " INSERT INTO empleado (nombre,apellido,direccion,sueldo,fecha_ingreso) VALUES  "
    names = pd.read_csv('data/names.csv', header=None, usecols=[0])
    surnames = pd.read_csv('data/surnames.csv', header=None , usecols=[0])
    addresses = pd.read_csv('data/addresses.csv', header=None)
    aux_string = ""
    d1 = datetime.strptime('1/1/2008 1:30 PM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('1/1/2020 4:50 AM', '%m/%d/%Y %I:%M %p')
    for i in range(1000):
        index_names = random.randint(0,(names.count()[0]-1))
        index_surnames = random.randint(0,(surnames.count()[0]-1))
        index_addresses = random.randint(0,(addresses.count()[0]-1))
        salary = random.randint(25000,98000)
        aux_string = aux_string  + "('" + names[0][index_names]  + "','" + surnames[0][index_surnames] + "','" + addresses[0][index_addresses]  + "','" + str(salary) + "','" + random_date(d1,d2).strftime("%Y-%m-%d %H:%M:%S") + "'),"
    aux_string = aux_string[:-1]
    return postgres_insert_query + aux_string

def insert_employees_activities():
    postgres_insert_query = " INSERT INTO actividad_instructor (id_actividad,legajo,hora_inicio,hora_fin) VALUES  "

    aux_string = ""
    for i in range(1,1000):
        id_actividad = random.randint(1,21)
        employee = random.randint(1,1000)
        hora_inicio = random.randint(7,22)
        hora_fin = random.randint(7,22)
        aux_string = aux_string + "(" + str(id_actividad) + "," + str(employee) + ",'" + str(hora_inicio) + "','" + str(hora_fin) + "'),"
    aux_string = aux_string[:-1]
    return postgres_insert_query + aux_string

def insert_employees_phone():
    postgres_insert_query = " INSERT INTO telefono_empleado (legajo,telefono_empleado) VALUES  "
    
    aux_string = ""
    for i in range(1,1000):
        phone = random.randint(15600000,15699999)
        aux_string = aux_string + "(" + str(i) + ",'" + str(phone) + "'),"
    aux_string = aux_string[:-1]
    return postgres_insert_query + aux_string

dict_specialties = { 
    1: "Aerobico",
    2: "Musculación",
    3: "Crossfit",
    4: "Acuático",
    5: "Yoga",
    6: "Funcional",
    7: "Pilates",
    8: "Rehabilitación"
}

def insert_specialties():
    postgres_insert_query = " INSERT INTO especialidades (cod_especialidad, descripcion) VALUES  "
    aux_string = ""
    for i in range(1,9):
        speciality_code = i
        description = dict_specialties[i]
        aux_string = aux_string + "(" + str(speciality_code) + ",'" + description + "'),"
    aux_string = aux_string[:-1]
    return postgres_insert_query + aux_string


def insert_employees_specialties():
    postgres_insert_query = " INSERT INTO especialidades_empleado (cod_especialidad, legajo, nivel_conocimiento, horas_capacitacion) VALUES  "
    aux_string = ""
    for i in range(1000):
        speciality_code = random.randint(1,8)
        legajo = random.randint(1,1000)
        horas_capacitacion = random.randint(30,2000)
        nivel_conocimiento = get_knowledge_level(horas_capacitacion)
        aux_string = aux_string + "(" + str(speciality_code) + "," + str(legajo) + "," + str(nivel_conocimiento) + "," + str(horas_capacitacion) + "),"
    aux_string = aux_string[:-1]  
    return postgres_insert_query + aux_string

def get_knowledge_level(horas_capacitacion):
    if horas_capacitacion < 300:
        return 1
    elif horas_capacitacion < 1000:
        return 2
    return 3

def main(_user,_pw):
    try:
        connection = psycopg2.connect(user=_user,
                                        password=_pw,
                                        host="127.0.0.1",
                                        port="5432",
                                        database="recursos_humanos")
        cursor = connection.cursor()

       # query = insert_activities() + "; \n"
       # query += insert_employees() + "; \n"
       # query = insert_employees_activities() + "; \n"
        query = insert_employees_phone() + "; \n"
       # query += insert_specialties() + "; \n"
       # query += insert_employees_specialties() + "; \n"
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