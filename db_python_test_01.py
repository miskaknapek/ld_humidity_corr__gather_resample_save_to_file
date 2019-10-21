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
        unique_latlons.append( float( [ rows_for_this_unique_latlon.ix[0, 'lat']), float( [ rows_for_this_unique_latlon.ix[0, 'lon'])  ]  )
    print(" --- done in "+str( time.time() - starttime )+" seconds " )

 
out3 = []
unique_latlons = []
def loop_uniques():
	starttime = time.time()
	for unique_id in in_data['latlon'].unique():
		rows_for_this_unique_latlon = in_data[ in_data['latlon'] == unique_id ]
        out3.append( rows_for_this_unique_latlon.shape[0] )
        unique_latlons.append( [ float( rows_for_this_unique_latlon.ix[0, 'lat']), float( rows_for_this_unique_latlon.ix[0, 'lon'])  ]  )
    print(" --- done in "+str( time.time() - starttime )+" seconds " )



______________ HISTORY : 

run gather_resample_and_save_met_data_to_csv_01.py
run gather_resample_and_save_met_data_to_csv_01.py
run gather_resample_and_save_met_data_to_csv_01.py
pd.Timestamp( 2019, 10, 14, 12, 13, 14 )
then = pd.Timestamp( 2019, 10, 14, 12, 13, 14 )
now = pd.Timestamp.now()
then-now
then-now.total_seconds()
thus= then-now
thus.total_seconds()
conn = psycopg2.connect("dbname='met_no_data' user='postgres' password='secret' host='localhost' ")
cur = conn.cursor()
print("--- got cursor!! ")
# take off the gloves
conn.set_isolation_level(0)
# something
0
conn = psycopg2.connect("dbname='met_no_data' user='postgres' password='secret' host='localhost' ")
cur = conn.cursor()
conn.set_isolation_level(0)
colunm_names = [ 'forecast_timestamp', 'fetched_at_timestamp', 'daily_fetching_session_id', 'lat', 'lon', 'altitude', 'temperature', 'winddirection', 'windspeed', 'pressure', 'humidity' ]
cur.execute("select * from 'met_no_fetched_data__sensor_grid' limit 20000 ORDER BY forecast_timestamp ;")
cur.execute("select * from met_no_fetched_data__sensor_grid limit 20000 ORDER BY forecast_timestamp ;")
cur.execute("select * from met_no_fetched_data__sensor_grid ORDER BY forecast_timestamp limit 20000  ;")
cur.execute("select * from met_no_fetched_data__sensor_grid ORDER BY forecast_timestamp limit 20000  ;")
in_data = pd.DataFrame( cur.fetchAll(), columns=colunm_names 
)
in_data = pd.DataFrame( cur.fetchall(), columns=colunm_names )
in_data
in_data.unique( lat, lon )
in_data['lat'].uniuque()
in_data['lat'].unique()
in_data['lat', 'lon'].unique()
in_data[ ['lat', 'lon'] ].unique()
in_data['latlon'] = in_data['lat']+in_data['lon']
in_data
in_data['latlon'] = pd.concat( in_data['lat'], in_data['lon'] )
out = np.unique( in_data['lat'], in_data['lon'] )
out = np.unique( in_data['lat'], in_data['lon'], axis=0 )
in_data['latlon'] = np.unique( in_data[ ['lat'], ['lon'] ], axis =0 )
in_data['latlon'] = np.unique( in_data[ ['lat'], ['lon'] ] )
in_data['latlon'] =  in_data[ ['lat'], ['lon'] ].apply( lambda x: ''.join(x), axis =1 )
in_data['latlon'] =  in_data[ ['lat'], ['lon'] ].apply( lambda x: ''.join( str(x) ), axis =1 )
out = pd.unique( in_data[ ['lat', 'lon'] ].values.ravel() )
ou
out
out = np.unique( in_data[ ['lat', 'lon'] ] )
out
out = in_data.groupby('lat')['lon'].apply( lambda x: list(np.unique(x)))
out
in_data['latlon'] = in_data[ ['lat', 'lon'] ].apply( lambda x : ''.join(x), axis =1 )
in_data['latlon'] = in_data[ ['lat', 'lon'] ].apply( lambda x : ''.join(str(x) ), axis =1 )
in_data
in_data['latlon']
in_data.dtypes
in_data['latlon'] = in_data['lat'].map( str ) + in_data['lon'].map( str )
in_data
in_data['latlon2'] = in_data['lat']*1000 + in_data['lon']*1000
in_data
out = 679507 +  251263
out
in_data['latlon2'] = in_data['lat']*10000 + in_data['lon']*10000
in_data
in_data['latlon3'] = in_data['lat']*100000.map(str) + in_data['lon']*10000
in_data['latlon3'] = (in_data['lat']*100000).map(str) + (in_data['lon']*10000).map(str)
in_data
out3 = in_data['latlon3'].unique()
out3
out3.size
out3.shape
in_data[ in_data['latlon3'] == '4680440.0000196862.0000' ]
in_data[ in_data['latlon3'] == '4680440.0000196862.0000' ]
cur.execute("select * from met_no_fetched_data__sensor_grid ORDER BY forecast_timestamp  ;")
in_data2 = pd.DataFrame( cur.fetchall(), columns=colunm_names )
in_data2.shape
in_data = in_data2
in_data['latlon3'] = (in_data['lat']*100000).map(str) + (in_data['lon']*10000).map(str)
in_data
uq = in_data.latlon3.unique()
uq.shape
cur.execute("select * from met_no_fetched_data__sensor_grid  ORDER BY forecast_timestamp  ;")
cur.execute("select * from met_no_fetched_data__sensor_grid where timestamp > '2019-10-17' and timestmap < '2019-10-18'  ORDER BY forecast_timestamp  ;")
cur.execute("select * from met_no_fetched_data__sensor_grid where forecast_timestamp > '2019-10-17' and forecast_timestamp < '2019-10-18'  ORDER BY forecast_timestamp  ;")
in_data = pd.DataFrame( cur.fetchall(), columns=colunm_names )
in_data
in_data['latlon'] = in_data['lat'].map(str) + in_data['lon].map(str)
%time in_data['latlon'] = in_data['lat'].map(str) + in_data['lon'].map(str)
def loop_uniques()
def loop_uniques():
    starttime = time.time()
    for unique_id in in_data['latlon'].unique():
        out = in_data[ in_data['latlon']  == unique_id ]
    print(" --- done in "+tsr( time.time() - starttime )+" seconds " )
%time loop_uniques()
def loop_uniques():
    starttime = time.time()
    for unique_id in in_data['latlon'].unique():
        out = in_data[ in_data['latlon']  == unique_id ]
    print(" --- done in "+str( time.time() - starttime )+" seconds " )
%time loop_uniques()
out3 = []
def loop_uniques():
    starttime = time.time()
    for unique_id in in_data['latlon'].unique():
        out = in_data[ in_data['latlon']  == unique_id ]
        out3.append( out.shape[0] )
    print(" --- done in "+str( time.time() - starttime )+" seconds " )
loop_uniques()
out3
np.unique ( out3 )
np.unique ( out3.append(21) )
out3
np.unique ( out3 )
cur.close()
cur
