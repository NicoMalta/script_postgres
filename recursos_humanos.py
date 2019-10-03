import psycopg2
from datetime import datetime

from utilities import random_date


d1 = datetime.strptime('1/1/2008 1:30 PM', '%m/%d/%Y %I:%M %p')
d2 = datetime.strptime('1/1/2009 4:50 AM', '%m/%d/%Y %I:%M %p')

try:
   connection = psycopg2.connect(user="malta",
                                  password="malta",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="recursos_humanos")
   cursor = connection.cursor()

   postgres_insert_query = """ INSERT INTO actividad (descripcion, fecha_inicio) VALUES (%s,%s)"""
   record_to_insert = ('Zumba', random_date(d1, d2))
   cursor.execute(postgres_insert_query, record_to_insert)

   connection.commit()
   count = cursor.rowcount
   print (count, "Record inserted successfully into mobile table")

except (Exception, psycopg2.Error) as error :
    if(connection):
        print("Failed to insert record into mobile table", error)

finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")