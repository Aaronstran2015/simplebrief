from urllib.request import urlretrieve as retrieve
import os
import csv
import time
import sqlite3
import pandas as pd
from pathlib import Path
import schedule

url = 'https://www.aviationweather.gov/adds/dataserver_current/current/metars.cache.csv'
filename = 'metars.csv'

conn = sqlite3.connect('avwxdata.db')
c = conn.cursor()


def refresh_data():
    sql_delete_query = ('''DELETE FROM metars;''')
    c.execute(sql_delete_query)
    retrieve(url, filename)
    metars = pd.read_csv(filename, on_bad_lines='skip', skiprows=5)
    metars.to_sql('metars', conn, if_exists='append', index=False)
    os.remove(filename)

while True:
    user_input=(input('Enter ICAO Identifier: ').upper()) #Enter an airport identifier
    refresh_data()
    
    if user_input != '/quit'.upper():
        output = c.execute(f'''SELECT raw_text FROM metars WHERE station_id = '{user_input}' ''').fetchall()
        print(output)
    else:
        break

#deletes downloaded csv after data is read
