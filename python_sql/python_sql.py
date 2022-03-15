import os 
#try:
#   os.remove('/home/max/LSE/DB_local_only/airlines2.db')
#except OSError:
#    pass

import sqlite3
from sqlite3 import Error
import csv as csv
import pandas as pd

conn = sqlite3.connect('/home/max/LSE/DB_local_only/airlines2.db')

c = conn.cursor()

for cs in pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/2000.csv", chunksize=1000, encoding='latin-1'):
    cs.to_sql("ontime", conn, if_exists="append")
for cs in pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/2001.csv", chunksize=1000, encoding='latin-1'):
    cs.to_sql("ontime", conn, if_exists="append")
for cs in pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/2002.csv", chunksize=1000, encoding='latin-1'):
    cs.to_sql("ontime", conn, if_exists="append")
for cs in pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/2003.csv", chunksize=1000, encoding='latin-1'):
    cs.to_sql("ontime", conn, if_exists="append")
for cs in pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/2004.csv", chunksize=1000, encoding='latin-1'):
    cs.to_sql("ontime", conn, if_exists="append")
for cs in pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/2005.csv", chunksize=1000, encoding='latin-1'):
    cs.to_sql("ontime", conn, if_exists="append")

airports = pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/airports.csv")
carriers = pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/carriers.csv")
planes = pd.read_csv("/home/max/LSE/DB_local_only/dataverse_files/plane-data.csv")

airports.to_sql('airports', con = conn, index = False)
carriers.to_sql('carriers', con = conn, index = False)
planes.to_sql('planes', con = conn, index = False) 

conn.commit()

#QUERIES 
con = sqlite3.connect('/home/max/LSE/DB_local_only/airlines2.db')
#Querry 1
db_df = pd.read_sql_query("""SELECT airports.city AS city, COUNT(*) AS total

FROM airports JOIN ontime ON ontime.dest = airports.iata

WHERE ontime.Cancelled = 0

GROUP BY airports.city

ORDER BY total DESC""", con)
db_df.to_csv('/home/max/LSE/st2195_assignment_3/python_sql/airlines_query_1.csv', index=False)



c.close()

#Querry 2
db_df = pd.read_sql_query("""SELECT carriers.Description AS carrier, COUNT(*) AS total

FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code

WHERE ontime.Cancelled = 1

    AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

GROUP BY carriers.Description

ORDER BY total DESC""", con)
db_df.to_csv('/home/max/LSE/st2195_assignment_3/python_sql/airlines_query_2.csv', index=False)

#Querry 3
db_df = pd.read_sql_query("""SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay

FROM planes JOIN ontime USING(tailnum)

WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0

GROUP BY model

ORDER BY avg_delay""", con)
db_df.to_csv('/home/max/LSE/st2195_assignment_3/python_sql/airlines_query_3.csv', index=False)

#Querry 4
db_df = pd.read_sql_query("""SELECT

    q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) AS ratio

FROM

(

    SELECT carriers.Description AS carrier, COUNT(*) AS numerator

    FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code

    WHERE ontime.Cancelled = 1 AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

    GROUP BY carriers.Description

) AS q1 JOIN

(

    SELECT carriers.Description AS carrier, COUNT(*) AS denominator

    FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code

    WHERE carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

    GROUP BY carriers.Description

) AS q2 USING(carrier)

ORDER BY ratio DESC""", con)
db_df.to_csv('/home/max/LSE/st2195_assignment_3/python_sql/airlines_query_4.csv', index=False)