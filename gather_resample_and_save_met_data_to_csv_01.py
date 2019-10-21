#!/usr/bin/env python
# coding: utf-8

# # Luftdaten data : data cleaning, 

import pandas as pd
import numpy as np
import time
import psycopg2
import json
import sys
import os


# -------- hello ! 
 
print("\n >>>> gather_saved_met_data_and_save_csv___SCRATCHPAD_01.py ()  ;) \n")


# --------- debugging? 

starttime = time.time()

# writing to the system to check that's going on 
# os.system( " echo 'started fetching script at "+str( time.ctime() )+" ' >> /mnt/virtio-bbc6cf3a-042b-4410-9/luftdaten/luftdaten_code/humidity_correction/fetch_met_no_api_coords_weather_data__add_it_to_db/fetch_script_START_log.txt")

# --------------- scripted variables+methods 

# connect to db
conn = psycopg2.connect("dbname='met_no_data' user='postgres' password='secret' host='localhost' ")

print("--- connected to database! ")

# get cursor
cur = conn.cursor()

print("--- got cursor!! ")

# take off the gloves
conn.set_isolation_level(0)


# -------------------- variables/parameters 



# ================= TEST ZONE ====================


# --- --- try making dataframes with varying numbers of columns ( eg depending on what weather data we wan ;)


basic_test_data_frame = pd.DataFrame( data={"p1": np.NaN, "p2" : np.NaN }, index=pd.DatetimeIndex( [start_timestamp] ) )

basic_test_data_frame











# ==================== METHODS ==============================







# ==================== RUN ==============================





# ==================== FINNISAGE ==============================











# ------------- the fin ;) ------------------