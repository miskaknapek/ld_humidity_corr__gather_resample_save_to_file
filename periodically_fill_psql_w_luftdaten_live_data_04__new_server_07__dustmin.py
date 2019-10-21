# hellow!
import psycopg2
import json, requests
import time
import sys
 
# ----- DEBUGGING : 

#### just to check that this runs, according to crontab
# fp = open("/home/ubuntu/python_luftdaten_write___log.txt", "w")
# fp.write( "- script finishing at "+str( time.ctime() ) )
# fp.flush()
# fp.close()

# ------------------ parameters

# relevant table name
## UNUSED : UNUSUED : UNUSED : this is hardcoded below…instead :-(
## UNUSED : UNUSUED : UNUSED : this is hardcoded below…instead :-(
## UNUSED : UNUSUED : UNUSED : this is hardcoded below…instead :-(
relev_table_name__for_input = "fill_me_w_ld_daten_06"  ## UNUSED : UNUSUED : UNUSED : this is hardcoded below…instead :-(

# for the latest fetch of luftdaten data
current_luftdaten_data = {}

# luftdaten data url 
# luftdaten_dust_min_url = "http://api.luftdaten.info/static/v2/data.dust.min.json"
luftdaten_dust_min_url = "https://maps.luftdaten.info/data/v2/data.dust.min.json" # new? 20190427

# testdata 
testdata_json_as_string = '[ {"id": 2593830593, "timestamp": "2018-12-30 11:47:14", "location": {"id": 42, "latitude": "48.8000", "longitude": "9.0020", "altitude": "365.6", "country": "DE"}, "sensor": {"id": 92, "sensor_type": {"id": 14, "name": "SDS011", "manufacturer": "Nova Fitness"}}, "sensordatavalues": [{"id": 5514161398, "value": "15.76", "value_type": "P1"}, {"id": 5514161399, "value": "9.54", "value_type": "P2"}]}, {"id": 2593829764, "timestamp": "2018-12-30 11:47:02", "location": {"id": 49, "latitude": "48.5300", "longitude": "9.2000", "altitude": "373.1", "country": "DE"}, "sensor": {"id": 107, "sensor_type": {"id": 1, "name": "PPD42NS", "manufacturer": "Shinyei"}}, "sensordatavalues": [{"id": 5514159630, "value": "564151.50", "value_type": "durP1"}, {"id": 5514159638, "value": "60501.00", "value_type": "durP2"}, {"id": 5514159637, "value": "972.68", "value_type": "P1"}, {"id": 5514159641, "value": "105.34", "value_type": "P2"}, {"id": 5514159634, "value": "1.88", "value_type": "ratioP1"}, {"id": 5514159640, "value": "0.20", "value_type": "ratioP2"}]} ]'

testdata_as_json_as_json = json.loads( testdata_json_as_string )


# ----------------- setup

print(">>>>>>>  Starting soon! filling your psql db with luftdaten data periodically! " ) 

# connect to db
conn = psycopg2.connect("dbname='ld_realtime_data_02' user='postgres' password='secret' host='localhost' ")
# conn = psycopg2.connect("dbname='ld_realtime_data' user='postgres' password='secret' host='localhost' ")

print("--- connected to database! ")

# get cursor
cur = conn.cursor()

print("--- got cursor!! ")

# take off the gloves
conn.set_isolation_level(0)
  


# - -----------------  various code


def fetch_luftDaten_data():
    print(">>> fetching luftdaten data at "+str( time.ctime() ) ) 

    starttime = time.time()

    current_luftdaten_data = json.loads( requests.get( luftdaten_dust_min_url ).text  )

    print(" --- fetched luftdaen data - and it's "+str( len( current_luftdaten_data ) )+" long :) - and that took "+str( time.time() - starttime )+" seconds ")

    return current_luftdaten_data


def write_MULTIPLE_luftdaten_min_data_to_db( current_luftdaten_data_ ):
    print(">>> write_MULTIPLE_luftdaten_min_data_to_db - starting at time "+str( time.ctime() ) ) 

    print( "-- just checking if we've got luftdaten data - eg length : "+str( len( current_luftdaten_data_ ) ) )

    # print( "----- and it looks like this " )
    # for item in current_luftdaten_data_:
    #   print( item )

    # to collect data to write
    data_to_write = []

    # to keep track 
    measurement_index = -1 
    sds011_count = 0
    sds011_VALID_count = 0 # < for when the data is all there .... and not caught by an exception 
    loop_start_time = time.time()

    print("- STARTING write loop : ")
    # loop
    for curr_measurement in current_luftdaten_data_:

        # keeep count 
        measurement_index = measurement_index + 1

        # feedback, but only every 200 lines 
        if measurement_index % 200 == 0:
            print( "---- working on measurement #"+str( measurement_index)+" / "+str( len( current_luftdaten_data_ )) )

        # ---- store relevant sensor values … 

        curr_luftdata_item = curr_measurement

        # -- check if it's a SDS011… 

        # fetch  sensor name 
        sensor_name = curr_luftdata_item['sensor']['sensor_type']['name']

        if( sensor_name == "SDS011" ):
            sds011_count = sds011_count + 1
            # print(" \t -------------- YESSS! it's an SDS011! ")
        
            # -- if it's a SDS011, fetch the other sensor values
            p1 = -1
            p2 = -1 
            
            # -- initialise the variables 
            # sensor id
            sensor_id = -1
            # sensor name 
            sensor_name = -1
            # timestamp
            timestamp = -1
            # lat
            lat = -1
            lon = -1 

            # check if the sensor indicies are there… 

            try: 
                # sensor id
                sensor_id = curr_luftdata_item['sensor']['id']
                # sensor name 
                sensor_name = curr_luftdata_item['sensor']['sensor_type']['name']
                # timestamp
                timestamp = curr_luftdata_item['timestamp'] 
                # lat
                lat = curr_luftdata_item['location']['latitude'] 
                lon = curr_luftdata_item['location']['longitude'] 

                # p1 and p2 values
                for data_value in curr_luftdata_item['sensordatavalues']:
                    if data_value['value_type'] == "P1":
                        p1 = data_value['value']
                    if data_value['value_type'] == "P2":
                        p2 = data_value['value']

            except( KeyError ):
                print("--- KeyError - likely latitude is missing… - passing the rest…  on sensor id "+str( curr_luftdata_item['sensor']['id'] ) )
                pass

            # print("\t ---- got sensor_id, sensor_name, lat, lon, timestamp, p1, p2 : ")
            # print( sensor_id, sensor_name, lat, lon, timestamp, p1, p2 )
            # print("\t ----" )

            data_to_write.append( ( sensor_id, sensor_name, lat, lon, timestamp, p1, p2 ) )

            # addthis as a valid write 
            sds011_VALID_count = sds011_VALID_count + 1

        # the else clause for the sensor name checking … i.e. not an SDS011
        else: 
            # print(" \t OH NOOOOO… it's not an SDS011, but a |"+str( sensor_name)+"|" )
            pass



    # --- write to database? 
    try: 
        # cur.execute( "INSERT INTO fill_me_w_ld_daten_01 ( sensor_id, sensor_name, lat, lon, timestamp, p1, p2 ) VALUES ( %s, %s, %s, %s, %s, %s, %s )", ( 12, 'SDS011', 48.8000, 9.0020, '2018-12-30 11:47:14', -1, -1 ) );
        print("--- now WRITING DATA TO DATABASE! -  "+str( time.time() - loop_start_time)+" - from starttime " )
        #  no write :) 
        cur.executemany( "INSERT INTO fill_me_w_ld_daten_06 ( sensor_id, sensor_name, lat, lon, timestamp, p1, p2 ) VALUES ( %s, %s, %s, %s, %s, %s, %s )", data_to_write );
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print(" \t --------- DONE - writing "+str( sds011_count  )+" SDS011 measurements to psql, at least in theory ;)" ) 
    print('\t ----------------- and all that took '+str( time.time() - loop_start_time  )+" seconds " )






def write_luftdaten_min_data_to_db( current_luftdaten_data_ ):
    print(">>> write_luftdaten_min_data_to_db - starting at time "+str( time.ctime() ) ) 

    print( "-- just checking if we've got luftdaten data - eg length : "+str( len( current_luftdaten_data_ ) ) )

    print( "----- and it looks like this " )
    for item in current_luftdaten_data_:
        print( item )

    # ---- store relevant sensor values … 

    curr_luftdata_item = current_luftdaten_data_[0]

    # sensor if
    sensor_id = curr_luftdata_item['sensor']['id']
    # sensor name 
    sensor_name = curr_luftdata_item['sensor']['sensor_type']['name']
    # timestamp
    timestamp = curr_luftdata_item['timestamp'] 
    # lat
    lat = curr_luftdata_item['location']['latitude']
    # lon
    lon = curr_luftdata_item['location']['longitude']

    p1 = -1 
    p2 = -1

    for data_value in curr_luftdata_item['sensordatavalues']:
        if data_value['value_type'] == "P1":
            pm1_val = data_value['value']
        if data_value['value_type'] == "P2":
            pm2_val = data_value['value']

    print("--- got sensor_id, sensor_name, lat, lon, timestamp, p1, p2 : ")
    print( sensor_id, sensor_name, lat, lon, timestamp, p1, p2 )

    # --- write to database? 
    try: 
        # cur.execute( "INSERT INTO fill_me_w_ld_daten_01 ( sensor_id, sensor_name, lat, lon, timestamp, p1, p2 ) VALUES ( %s, %s, %s, %s, %s, %s, %s )", ( 12, 'SDS011', 48.8000, 9.0020, '2018-12-30 11:47:14', -1, -1 ) );
        cur.execute( "INSERT INTO fill_me_w_ld_daten_06 ( sensor_id, sensor_name, lat, lon, timestamp, p1, p2 ) VALUES ( %s, %s, %s, %s, %s, %s, %s )", ( sensor_id, sensor_name, lat, lon, timestamp, p1, p2 ) );
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    print(" DONE - writing values to psql, at least in theory ;) ")


# ------------------ run 

# current_luftdaten_data = fetch_luftDaten_data()

luftdaten_dust_min_DATA = fetch_luftDaten_data()
write_MULTIPLE_luftdaten_min_data_to_db( luftdaten_dust_min_DATA )

#### just to check that this runs, according to crontab
# fp = open("/home/ubuntu/python_luftdaten_write___log.txt", "w")
# fp.write( "- script finishing at "+str( time.ctime() ) )
# fp.flush()
# fp.close()

# -------------- fin 
sys.exit()


