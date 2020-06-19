# This programs calls a API to get data of weather and then send to DB.
# Devolment for AWS lambda.
import os
import json
import datetime
from datetime import timedelta
import getpass

import ibm_db
import requests

# conn_str es un string que incluye las credenciales para la conexion a la base de datos.
conn_str =''
# Declaración INSERT para la base de datos.
sql_insert = 'INSERT INTO weather(CITY_ID, CITY_NAME, LON, LAT, WEATHER_MAIN, WEATHER_DESCRIPTION, MAIN_TEMP, MAIN_TEMP_MIN, MAIN_TEMP_MAX, WIND_SPEED, WIND_DEG, WIND_DIRECTION, DT_UNIX, TIME_ZONE, CURRENT_DATE_WLG, CURRENT_TIME_WLG, CURRENT_DATE_MID, CURRENT_TIME_MID, USER_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'
# API
api_str = ''

def AppWeather(event, context): 
    try:
        result = False
        request = requests.get(api_str)
        if request.status_code == 200:
            data = request.json()
            # Se extraen los datos necesarios del json
            city_id = data['id']
            city_name = data['name']
            lon = float("{0:.2f}".format(data['coord']['lon']))
            lat = float("{0:.2f}".format(data['coord']['lat']))
            weather = str(data['weather'])
            w = []
            for word in weather.split(","):
                for word1 in word.split(":"):
                    for word2 in word1.split("'"):
                        w.append(word2)
            weather_main = w[8]
            weather_description = w[14]
            # La temperatura está dada en centrigrados, el json lo trae en kelvin por lo cual se transforma
            main_temp = float("{0:.2f}".format(data['main']['temp']-273))
            main_temp_min = float("{0:.2f}".format(data['main']['temp_min']-273))
            main_temp_max = float("{0:.2f}".format(data['main']['temp_max']-273))
            # La velocidad del viento está dada en m/s
            wind_speed = float("{0:.2f}".format(data['wind']['speed']))
            wind_deg = float("{0:.2f}".format(data['wind']['deg']))
            wind_direction = "ND"
            # dt es la hora en UTC universal
            dt_unix = data['dt']
            # timezone da el delay con respecto al utc.
            timezone = ((data['timezone'])/60)/60
            # preparativos para la hora y la fecha
            date_utc = datetime.datetime.now(datetime.timezone.utc)
            date_wlg = date_utc + timedelta(seconds=0, minutes=0, hours=timezone, days=0)
            date_mid = date_utc - timedelta(seconds=0, minutes=0, hours=5, days=0)
            # variables con los valores correctos
            current_date_wlg = str(date_wlg.date())
            current_time_wlg = str(date_wlg.time().strftime("%H:%M:%S"))
            current_date_mid = str(date_mid.date()) 
            current_time_mid = str(date_mid.time().strftime("%H:%M:%S"))
            user_id = getpass.getuser()
            print('Los datos fueron leidos de manera correcta.')
            print("{}".format(user_id))
            # conexion con la db2
            conn = ibm_db.connect(conn_str,'','')
            if ibm_db.active(conn):
                print('Se establece la conexion con la DB.')
                stmt = ibm_db.prepare(conn, sql_insert)
                ibm_db.bind_param(stmt, 1, city_id)
                ibm_db.bind_param(stmt, 2, city_name)
                ibm_db.bind_param(stmt, 3, lon)
                ibm_db.bind_param(stmt, 4, lat)
                ibm_db.bind_param(stmt, 5, weather_main)
                ibm_db.bind_param(stmt, 6, weather_description)
                ibm_db.bind_param(stmt, 7, main_temp)
                ibm_db.bind_param(stmt, 8, main_temp_min)
                ibm_db.bind_param(stmt, 9, main_temp_max)
                ibm_db.bind_param(stmt, 10, wind_speed)
                ibm_db.bind_param(stmt, 11, wind_deg)
                ibm_db.bind_param(stmt, 12, wind_direction)
                ibm_db.bind_param(stmt, 13, dt_unix)
                ibm_db.bind_param(stmt, 14, timezone)
                ibm_db.bind_param(stmt, 15, current_date_wlg)
                ibm_db.bind_param(stmt, 16, current_time_wlg)
                ibm_db.bind_param(stmt, 17, current_date_mid)
                ibm_db.bind_param(stmt, 18, current_time_mid)
                ibm_db.bind_param(stmt, 19, user_id)
                print('Preparativos listos.')
                try:
                    ibm_db.execute(stmt)
                    result = True
                    print('Los datos fueron agregados a la DB') 
                except:
                    print('Error en execute.')
                    print(ibm_db.stmt_errormsg())
            else:
                print('Error en la conexión a la DB.')
        else:
            print(request.status_code)
    except:
        print(ibm_db.stmt_errormsg())
    finally:
        request.close()
        if ibm_db.active(conn):
            ibm_db.close(conn)
            if ibm_db.active(conn) == False:
                print('La conexion a la base de datos se ha cerrado.')
        print('La conexion con el servidor de datos de Weather se ha cerrado.')
    return result

        



