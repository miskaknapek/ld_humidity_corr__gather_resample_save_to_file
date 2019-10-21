import pandas as pd
import numpy as np
import time
# db
import psycopg2

conn = psycopg2.connect("dbname='met_no_data' user='postgres' password='secret' host='localhost' ")
cur = conn.cursor()
conn.set_isolation_level(0)

colunm_names = [ 'forecast_timestamp', 'fetched_at_timestamp', 'daily_fetching_session_id', 'lat', 'lon', 'altitude', 'temperature', 'winddirection', 'windspeed', 'pressure', 'humidity' ]

cur.execute("select * from met_no_fetched_data__sensor_grid ORDER BY forecast_timestamp  ;")

in_data = pd.DataFrame( cur.fetchall(), columns=colunm_names )


#### HOW TO CONCAENATE LAT+LON string values 
in_data['latlon'] = in_data['lat'].map(str) + in_data['lon'].map(str)
#### AND THEN DO THE UNIQUE 
in_data['latlon'].unique() 


### SPEEDTEST OF FETCHING UNIQUE VALUES ( and their lengths )

out3 = []
unique_latlons = []
def loop_uniques():
	starttime = time.time()
	for unique_id in in_data['latlon'].unique():
		rows_for_this_unique_latlon = in_data[ in_data['latlon'] == unique_id ]
        out3.append( rows_for_this_unique_latlon.shape[0] )
        unique_latlons.append( [ float( rows_for_this_unique_latlon.ix[0, 'lat']), float( rows_for_this_unique_latlon.ix[0, 'lon'])  ]  )
    print(" --- done in "+str( time.time() - starttime )+" seconds " )