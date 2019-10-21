#!/usr/bin/env python
# coding: utf-8

# # Luftdaten data : data cleaning, resampling - mini version - cleaning v5
# 
# # - TRYING TO HEAD FOR FINAL… 
# ## Code builds a continuous time tabular version of the luftdaen data, such that the same time period is present for each sensor in the data, regardless of whether each sensor has data for all the time slots. 
# 

"""
TO DO :  TO  DO :  TO  DO :  TO  DO :  TO  DO
--------------------------

- FIX THE NAN! 

- Run on server 
    - Do psql query! 

-- do the time setting if one's calculating 24 hour of data 

"""


import pandas as pd
import numpy as np
import time
# db
import psycopg2
# json
import json
# for command line arguments 
import sys
import os


## --- --- --- --- --- --- --- --- SAY HELLO! 

print("\n >>> welcome to dustmin->tabular data  ")


## --- --- --- --- --- --- SWITCH ON/OFF PRINTING?

print2 = print

# this takes out the print behaviour 
def print( x ):
   return 1

 

## ===================== FUNCTIONS ==============================

## -- --- --- --- FUNCTION : How many sample periods fit into the time duration examined?


def figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration( kind_of_time_period_we_are_doing, time_length_of_sample_period__in_seconds, if_date_range__start_time=0, if_date_range__end_time=0, ):
    
    print("\n >>> figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration : checking time period : doing time since midnight : |"+str(kind_of_time_period_we_are_doing)+"| single sample time length : |"+str(time_length_of_sample_period__in_seconds)+"| - start/end periods if doing date range :  |"+str(if_date_range__start_time)+"| - |"+str(if_date_range__end_time)+"|" ) 
    
    num_of_sample_time_periods_fit_in_total_sampled_period = 0
    
    # 24 hours period
    if kind_of_time_period_we_are_doing == "kind_of_time_period_we_are_doing__one_day_only":
        num_of_sample_time_periods_fit_in_total_sampled_period = int( ( 24*60*60 / time_length_of_sample_period__in_seconds ) ) 
    
    # since midnight period 
    if kind_of_time_period_we_are_doing == "kind_of_time_period_we_are_doing__since_midnight":
        ## - get time since midnight 
        
        # generate timestamp, to get time since midnight 
        end_timestamp = pd.Timestamp.now()
        hours_seconds_since_midnight = end_timestamp.hour*60*60
        min_seconds_since_midnight = end_timestamp.minute*60
        seconds_since_midnight = hours_seconds_since_midnight + min_seconds_since_midnight + end_timestamp.second
        
        print( "-- the number of seconds since midnight is "+str( seconds_since_midnight ) )
        
        num_of_sample_time_periods_fit_in_total_sampled_period = int( seconds_since_midnight / time_length_of_sample_period__in_seconds ) 

    # since midnight period 
    if kind_of_time_period_we_are_doing == "kind_of_time_period_we_are_doing__date_range":

        print("-- calculating time length in date range, starting end in "+str(if_date_range__start_time)+" - "+str(if_date_range__end_time) )

        total_time = if_date_range__end_time - if_date_range__start_time
        total_time = total_time.total_seconds()
        num_of_sample_time_periods_fit_in_total_sampled_period = int( total_time / time_length_of_sample_period__in_seconds )


    print("- - - - NUMBER OF SAMPLE PERIODS IN WHOLE TIME PERIOD : num_of_sample_time_periods_fit_in_total_sampled_period : "+str( num_of_sample_time_periods_fit_in_total_sampled_period ))
    
    # adding +1 for off-by-one-errors … 
    return num_of_sample_time_periods_fit_in_total_sampled_period+1



## -- --- --- --- FUNCTION : Check date validity and return a valid date if ok


def check_date_string_validity_and_return_pd_timestmp_if_valid( in_date_to_test ):

    print( ">>>> check_date_string_validity_and_return_pd_timestmp_if_valid() - checking in_date : |"+str( in_date_to_test )+"|" )

    # split the indate 
    in_date_raw_split = np.array(  in_date_to_test.split("-") ).astype( int ) # 1st argument 

    if in_date_raw_split.shape[0] == 3:
        print("--- ---- --- checking length : LENGTH OK - got three elements in the date -- trying to see if the input data will make a good date…")

        ## decide the whole date is ok or not … 
        try :
            parsed_date = pd.Timestamp( in_date_raw_split[0], in_date_raw_split[1], in_date_raw_split[2] )
        except ValueError :
            print("--- ---- --- --- DATE NOT VALID :-( - EEEEEEXXXXIIIIITTTTTING ") 
            sys.exit(0)

        # give good news if the date works! 
        print("--- ---- --- --- DATE OK OK OK OK ! - |"+str( parsed_date )+"|" )
        # set the variable to do a data search for 24 hours     

        return parsed_date

    else:
        print("--- ---- command line arguments - GOT THE WRONG NUMBER of arguments - wanted 0 or 1, but got "+str( len( in_date_to_test )-1 ))
        print("--- ---- EXITING - sorry! - EXITING! ")
        sys.exit(0)



## -- --- --- --- FUNCTION : Return today's date in a relevant format


def return_todays_date_as_pd_timestamp():

    print(">>>> return_todays_date_as_pd_timestamp() ");

    midnight_timestamp_from_today = -1;

    now_timestamp = pd.Timestamp.now()

    midnight_timestamp_from_today = pd.Timestamp( now_timestamp.year, now_timestamp.month, now_timestamp.day, 0 )

    print("--- returning : |"+str( midnight_timestamp_from_today )+"| " )

    return midnight_timestamp_from_today



## ========================================================= PARAMETERS & disorganised line-by-line code



## --- --- --- --- --- --- database and table names 

# -- DATABASE NAMES 

database_name = "ld_realtime_data_02"       # REALTIME DATA 
# database_name = "ld_ts_older_data"                   # OLDER DATA 


# -- TABLE NAMES 

# table_name = "fill_me_w_ld_daten_06"        # REALTIME DATA 

## --- table names from the ld_ts_older_data database
# ld_ts_2018_05d_chks_ts_idx                          # OLDER DATA 
# table_name = "ld_ts_2018_05d_chks_ts_n_snid_idx"    # OLDER DATA 
table_name = "fill_me_w_ld_daten_06"                 # realitme data 

# -- COLUMN NAMES

# columns_names =  [ "index_", "sensor_id", "lat", "lon", "timestamp", "p1", "p2", "fixed_p1_value", "fixed_p2_value", "fixed_latlon_values" ]   # OLDER DATA 
columns_names =  ['sensor_id', 'sensor_name', 'lat', 'lon', 'timestamp', 'p1', 'p2']    # REALTIME DATA 




## --- --- --- --- --- --- KEY SWITCHES


do_sql_data_fetch = True

do_resampling_loop = True 

saving_data = True 

save_file_to_remote_server = True

## --- --- --- --- --- --- Timing

# for TIMING the code 
total_start_time = time.time()


 
## --- --- --- --- --- --- Filenames 


# which directory is the file in? 
##s# basic_file_path_to_final_file = "/mnt/virtio-bbc6cf3a-042b-4410-9/luftdaten/luftdaten_daten/tabular_data/tabular_ld_data__30x60_s_intervals/" 
basic_file_path_to_final_file = "/mnt/virtio-bbc6cf3a-042b-4410-9/luftdaten/luftdaten_daten/tabular_data/tabular_ld_data_TEST_AREA/"


# filename beginning for files about the laest data

file_name__for__generate_data_since_midnight = "latest_data_since_midnight"

file_name__for__generate_given_24_hours = "24_hrs_pm_data__starting_from__"

file_name_suffix = ".json"


## -- --- --- ---  DATA URLS 


nordic_midnight_24_hrs_data__url = "/Users/miska/Documents/open_something/luftdaten/luftdaten_code/luftdaten__make_tabular_data__from_db_data/ld_NYE_midnight_24hrs_nordics_all_data_01.csv"
## nordic_midnight_24_hrs_data__url = "/home/miska/documents/opensomething/luftdaten/dustmin_to_csv__various_code/ld_NYE_midnight_24hrs_nordics_all_data_01.csv"
# nordic_midnight_24_hrs_data__url = "/Users/miska/temp_temp_temp/sds011_files_wo_microsecond_nulling/2018-09_sds011.csv_cleaned.csv"
nordic_midnight_24_hrs_data__url = "/mnt/virtio-bbc6cf3a-042b-4410-9/luftdaten/luftdaten_daten/2019-01_sds011.csv_cleaned__FIRST_500k.csv"

# DATA URL 
curr_url = nordic_midnight_24_hrs_data__url


# REMOTE DIRECTORY FOR FILES 
remote_server_destination_directory_SCP_url = "sourisr@kapsi.fi:sites/sourisr.kapsi.fi/www/luftdaten/luftDaten_data_explorations/ld_daten_various/tabular_ld_data__480_s_intervals/"


# where the BIG DATA goes! 
in_data = 0


## -- --- --- ---  BASIC VARIABLES ( set up )


# 
list_of_unique_sensor_ids = []

# time length of sample period
time_length_of_sample_period__in_seconds = 60*8

time_length_of_sample_period__in_seconds__as_pandas_resampleing_time = str(time_length_of_sample_period__in_seconds)+"S"
print2("\n--- time_length_of_sample_period__in_seconds__as_pandas_resampleing_time : "+time_length_of_sample_period__in_seconds__as_pandas_resampleing_time )

# how many time sample periods can fit in the current total data time period
num_of_sample_time_periods_in_whole_time_span__for_basic_data = 0 



# for when we're creating 24 hour time segements
default__generate_data_for_24_hour_period_starting_from_starttime = 'default__generate_data_for_24_hour_period_starting_from_starttime'

# for when we're creating data files for the period until now
default__generate_data_from_last_midnight_until_current_time =  "default__generate_data_from_last_midnight_until_current_time"

# set global variable 
current_time_duration_in_data_generation = 0







## --- --- --- --- --- --- general operations - what kind of time length in the data, are we outputting

# ---  set default values 

# Do we generate time periods since midnight (True) or full 24 hour periods ( False ) 
# true == do since midnight until now 
# false == do midnight to midnight from the suggested date 'start_date__if_doing_24_hours_data'
# ( defined according to present or absent commandline input )
do_time_since_midnight = 0 

# value templates 
kind_of_time_period_we_are_doing__since_midnight = "kind_of_time_period_we_are_doing__since_midnight"
kind_of_time_period_we_are_doing__one_day_only = "kind_of_time_period_we_are_doing__one_day_only"
kind_of_time_period_we_are_doing__date_range = "kind_of_time_period_we_are_doing__date_range"
# actual value used 
kind_of_time_period_we_are_doing = kind_of_time_period_we_are_doing__one_day_only


# ( defined according to present or absent commandline input )
start_date__if_doing_24_hours_data = 0


#--  fetch command line arguments : which might affect the above options 
command_line_arguments__via_sys_argv = sys.argv 

# check if the correct length of arguemnts are presented 
# - one + zero arguments means make a file since midnight
# - one + two arguments means make a file from midnight to midnight

print("\n --- getting command line arguments  - got "+str( len( command_line_arguments__via_sys_argv )-1 )+" arguments ")

# check if there's the number of argumetns to do the since-midnight data-generation
"""
if len( command_line_arguments__via_sys_argv ) == 1:
    print("--- --- got no command line arguments - so continuing as normal in make tabular data of data since midnight")    
    kind_of_time_period_we_are_doing = kind_of_time_period_we_are_doing__since_midnight
"""
# - IF DOING A SINGLE DAY, with a given date… 
# check if the input arguments suggsts doing a midnight-midnight data generation run ( if there's ONE arguments)
# - then check if the date is valid and usable 
# elif len( command_line_arguments__via_sys_argv ) == 2 : 
if len( command_line_arguments__via_sys_argv ) == 1 : 
    """ OLD 
    print("--- ---- command line arguments - got ONE argument - making data from a midnight to another midnight  ")
    print("--- ---- got arguments |"+str( command_line_arguments__via_sys_argv[1] )+"|")
    print("--- ---- --- NOW CHECKING IF THE DATE IS VALID  ")
    """

    # start_date__if_doing_24_hours_data = check_date_string_validity_and_return_pd_timestmp_if_valid( command_line_arguments__via_sys_argv[1] )
    start_date__if_doing_24_hours_data = return_todays_date_as_pd_timestamp()
    #
    kind_of_time_period_we_are_doing = kind_of_time_period_we_are_doing__one_day_only

    # NEW printing 
    print("--- ---- command line arguments - got ONE argument - making data from a midnight to another midnight  ")
    print("--- ---- making day date : |"+str( start_date__if_doing_24_hours_data )+"|")


# - IF DOING A DATE RANGE - between first and last date! 
# check if the input arguments suggsts doing a midnight-midnight data generation run ( if there's ONE arguments)
# - then check if the date is valid and usable 
elif len( command_line_arguments__via_sys_argv ) == 3 : 
    print("--- ---- command line arguments - got TWO arguments - making data from the suggested start/end date … i.e. "+str(command_line_arguments__via_sys_argv[1])+" - "+str(command_line_arguments__via_sys_argv[2])+"" )
    print("--- ---- got arguments |"+str( command_line_arguments__via_sys_argv[1] )+"|")
    print("--- ---- --- NOW CHECKING IF THE DATE IS VALID  ")

    start_date_if_doing_date_range_data_gen = check_date_string_validity_and_return_pd_timestmp_if_valid( command_line_arguments__via_sys_argv[1] )
    end_date_if_doing_date_range_data_gen = check_date_string_validity_and_return_pd_timestmp_if_valid( command_line_arguments__via_sys_argv[2] )
    #
    kind_of_time_period_we_are_doing = kind_of_time_period_we_are_doing__date_range

    print("--- --- DOING DATE RANGE - got two dates parsed : |"+str(start_date_if_doing_date_range_data_gen)+"| - |"+str(end_date_if_doing_date_range_data_gen)+"|" )






# DEBUGGING 
print("--- quick start_date__if_doing_24_hours_data check : "+str( start_date__if_doing_24_hours_data )+" and kind_of_time_period_we_are_doing : "+str( kind_of_time_period_we_are_doing )  ) 



# --- Generate relevant timestamps


# make variables
end_timestamp = 0
start_timestamp = 0

print("--- generating start/end timestamps for the pqsl query : kind_of_time_period_we_are_doing is : |"+str( kind_of_time_period_we_are_doing)+"| start_date__if_doing_24_hours_data looks like : |"+str( start_date__if_doing_24_hours_data)+"|" )

# if generating data from last midnight until current time
if kind_of_time_period_we_are_doing == kind_of_time_period_we_are_doing__since_midnight:    
    # the start timestamp would be last midnight 
    timestamp_now = pd.Timestamp.now()
    start_timestamp = pd.Timestamp( timestamp_now.year, timestamp_now.month, timestamp_now.day, 0 )
    # timestamp last midnight 
    end_timestamp = pd.Timestamp.now()
    #
# or, ir generating data from midnight to midnight
elif kind_of_time_period_we_are_doing == kind_of_time_period_we_are_doing__one_day_only:
    # the start timestamp would be last midnight 
    start_timestamp = start_date__if_doing_24_hours_data
    # timestamp last midnight 
    end_timestamp = start_timestamp + pd.DateOffset(1)
    #
elif kind_of_time_period_we_are_doing == kind_of_time_period_we_are_doing__date_range:
    # the start timestamp would be last midnight 
    start_timestamp = start_date_if_doing_date_range_data_gen
    # timestamp last midnight 
    end_timestamp = end_date_if_doing_date_range_data_gen


# time feedback 
print2( "\n-- generating relevant start/end tiemstamps, in mode |"+str(kind_of_time_period_we_are_doing)+"| \n \t got start time "+str( start_timestamp )+" and end time "+str( end_timestamp ) )




# --- set up blank start and end rows

in_data__START_TIME__blank_row = pd.DataFrame( data={ "p1": np.NaN, "p2" : np.NaN }, index=pd.DatetimeIndex( [start_timestamp] ) )

in_data__END_TIME__blank_row = pd.DataFrame( data={ "p1": np.NaN, "p2" : np.NaN }, index=pd.DatetimeIndex( [end_timestamp] ) )



# -- -- -- FETCH DATA! 



if do_sql_data_fetch == True:

    mini_timing = time.time() 

    print2("-- -- PSQL QUERY : attempting to connect to DB, executing query ")

    conn = psycopg2.connect("dbname='"+database_name+"' user='postgres' password='secret' host='localhost' ")

    cur = conn.cursor()

    query = "select * from "+table_name+" where timestamp > '"+str( start_timestamp )+"' and timestamp < '"+str( end_timestamp )+"' "
    print2( "-- -- running psql query |"+query+"|" )

    cur.execute( query )

    print2("-- --- query ready at "+str( time.time() - mini_timing ) ) 

    in_data = pd.DataFrame( cur.fetchall(), columns=columns_names )

    print2("\n --- --- got table of shape "+str( in_data.shape )+" "+str( time.time() - mini_timing ) )    
    print2("--- --- got columns : |"+str( in_data.columns)+"|" )

 
# READ DATA FROM FILE
elif do_sql_data_fetch == False : 
 
    # READ DAtA 
    in_data = pd.read_csv( curr_url, sep=";", header=0 )
    # in_data.shape

    print2("-- loaded data, and got columns : "+str( in_data.columns ) )
    print2("-- loaded data, and it took "+str( time.time() - total_start_time )+" to load data of shape "+str( in_data.shape ) )


# --- POST READ DATA : set up data - timestamps and all! 

# aha - timestamp column not a timestamp column?
# - let's fix 
in_data['timestamp'] = pd.to_datetime( in_data['timestamp'] )

# set the timestamp column as the index 
in_data = in_data.set_index( 'timestamp' )

# sort data chronologically, by the index 
in_data = in_data.sort_index()





## -- --- --- --- set up data holders


# --- test fetching the number of sample periods in 24 hours 
num_of_sample_time_periods_in_whole_time_span__for_basic_data = figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration( kind_of_time_period_we_are_doing, time_length_of_sample_period__in_seconds, start_timestamp, end_timestamp )

"""
# if generating data from last midnight until current time
if do_time_since_midnight == True:
    # test fetching the number of sample periods since last midnight 
    num_of_sample_time_periods_in_whole_time_span__for_basic_data = figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration( do_time_since_midnight, time_length_of_sample_period__in_seconds )
#
# or, ir generating data from midnight to midnight
elif do_time_since_midnight == False:
    num_of_sample_time_periods_in_whole_time_span__for_basic_data = figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration( do_time_since_midnight, time_length_of_sample_period__in_seconds )
"""

# --- Get the list of unique sensor ids
 
list_of_unique_sensor_ids = in_data['sensor_id'].unique()
print2( "-- got "+str( list_of_unique_sensor_ids.shape[0] )+" UNIQUE SENSOR IDS ")

# #### make OUT DATA variables

out_data__resampled_sensor_values_as_rows = np.array( np.zeros( list_of_unique_sensor_ids.shape[0] * num_of_sample_time_periods_in_whole_time_span__for_basic_data ) )

# reshape 
out_data__resampled_sensor_values_as_rows = out_data__resampled_sensor_values_as_rows.reshape( [ list_of_unique_sensor_ids.shape[0], num_of_sample_time_periods_in_whole_time_span__for_basic_data ] )

# make p1 and p2 versions of the out array 
out_data__resampled_sensor_values_as_rows__p1_values = out_data__resampled_sensor_values_as_rows
out_data__resampled_sensor_values_as_rows__p2_values = np.copy( out_data__resampled_sensor_values_as_rows )
print( "\n -- made out arrays of p1/p2 data : out_data__resampled_sensor_values_as_rows__ p1/p2 _values np arrays, of shape : "+str( out_data__resampled_sensor_values_as_rows__p1_values.shape)+", "+ str(out_data__resampled_sensor_values_as_rows__p2_values.shape ) )

# --- TEMPORARY out data variable 
# ( until we run with live data, the size of the outputted data will be a bit wrong )
temp_out_data = []

# --- make variable for LAT LONG coordinates of sensors
# x2 so lat lon can be on one row 
sensors_lat_lon_list = np.array( np.zeros( list_of_unique_sensor_ids.shape[0] *2 ) )
sensors_lat_lon_list = sensors_lat_lon_list.reshape( [ list_of_unique_sensor_ids.shape[0], 2 ] )



# --- ---- --- --- RESAMPLE LOOP


# --- LOOP AND RESAMPLE!
# for timing 
start_time = time.time()

print2("\n -- -- GETTING READY TO LOOP! at time "+str( time.time() - total_start_time ) )


# loop
if do_resampling_loop : 
    # for each index to each sensor id 
    for current_sensor_id_i in range( len( list_of_unique_sensor_ids[:] )):

        if current_sensor_id_i % 100 == 0 : 
            print2("\n--- --- --- working on sensor id "+str(current_sensor_id_i)+" / "+str( list_of_unique_sensor_ids.shape[0] )+" at time "+str( ( time.time() - start_time ) )  ) 

        # get current sensor id 
        current_sensor_id = list_of_unique_sensor_ids[ current_sensor_id_i ]
        
        # get all the values of the current sensor id 
        curr_sensor_id__in_data = in_data[ in_data['sensor_id'] == current_sensor_id ]
        
        print("--- --- --- the first row looks like this "+str( curr_sensor_id__in_data.iloc[0] ) )
        
        # save lat/lon data
        sensors_lat_lon_list[ current_sensor_id_i, 0 ] = curr_sensor_id__in_data.iloc[0]['lat'] 
        sensors_lat_lon_list[ current_sensor_id_i, 1 ] = curr_sensor_id__in_data.iloc[0]['lon'] 
        # test output 
        print("--- --- --- lat lon output test : ")
        print( "--- --- --- "+str( sensors_lat_lon_list[ current_sensor_id_i, 0 ] )+", "+str( sensors_lat_lon_list[ current_sensor_id_i, 1 ] ) )


        
        #     print("- - - current sensor lat/on : "+str( sensors_lat_lon_list[ current_sensor_id_i, 0 ] )+","+str( sensors_lat_lon_list[ current_sensor_id_i, 1 ] ) )
        #     print("- - - - input lat lon : "+str( curr_sensor_id__in_data[0]['lat'] )+","+str( curr_sensor_id__in_data[0]['lon'] ) )    
        
        
        # minimise the in_data, so it's easier to resample the p1 and p2 values
        curr_sensor_id__in_data = curr_sensor_id__in_data[ ['p1', 'p2'] ]
        
        print("\n--- --- ---  working on sensor id "+str( current_sensor_id)+" - - got "+str( curr_sensor_id__in_data.shape[0])+" rows of data" )
        
        print( "--- --- ---  got columns : "+str( curr_sensor_id__in_data.columns ) ) 
        
        ## INSERT bland start and end points 
        
        curr_sensor_id__in_data__with_blank_start_time = in_data__START_TIME__blank_row.append( curr_sensor_id__in_data )
        
        print("--- --- ---  current data length = "+str( curr_sensor_id__in_data__with_blank_start_time.shape ))
        
        curr_sensor_id__in_data__with_blank_start_AND_end_time = curr_sensor_id__in_data__with_blank_start_time.append( in_data__END_TIME__blank_row )
        
        print("--- --- ---  current data length = "+str( curr_sensor_id__in_data__with_blank_start_AND_end_time.shape ))   
        
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time[:10] )
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time[-10:] )    
        
        curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED = curr_sensor_id__in_data__with_blank_start_AND_end_time.resample( time_length_of_sample_period__in_seconds__as_pandas_resampleing_time ).mean().fillna( 0 )

        print("\n--- --- --- resampled data dtypes :")
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED.dtypes )
        
        print("\n--- --- ---  printing resampled data ( shape : "+str( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED.shape)+") ")
        
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED[:10] )
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED[-10:] )

        out_data__resampled_sensor_values_as_rows__p1_values[ current_sensor_id_i ][0:num_of_sample_time_periods_in_whole_time_span__for_basic_data] = curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED['p1']
        out_data__resampled_sensor_values_as_rows__p2_values[ current_sensor_id_i ][0:num_of_sample_time_periods_in_whole_time_span__for_basic_data] = curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED['p2']
        
        ## STORE DATA
        
        temp_out_data.append( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED )
        
    

    

## ----------- FINALIGNS 


## --- convert those data arrays to ints? 
print2("\n ------- final : convert p1/p2 arrays to int")
out_data__resampled_sensor_values_as_rows__p1_values = out_data__resampled_sensor_values_as_rows__p1_values.astype( int )
out_data__resampled_sensor_values_as_rows__p2_values = out_data__resampled_sensor_values_as_rows__p2_values.astype( int )
print2(" ------- final : .... and that toop "+str( time.time() - total_start_time ))

print2( "\n out_data__resampled_sensor_values_as_rows__p1_values, out_data__resampled_sensor_values_as_rows__p2_values head/tail :" )
print2( "p1 values head/tail :" )
print2( out_data__resampled_sensor_values_as_rows__p1_values[1][:10] )
print2( out_data__resampled_sensor_values_as_rows__p1_values[1][-10:] )
print2( "p2 values head/tail :" )
print2( out_data__resampled_sensor_values_as_rows__p2_values[1][:10] )
print2( out_data__resampled_sensor_values_as_rows__p2_values[1][-10:] )


# --- saving data?

print2("\n ------- saving data? |"+str( saving_data )+"|" )

if saving_data == True : 

    ## --- --- prepare data for export 
    print2("------- ------- NOW SAVING DATA - at time "+str( time.time() - total_start_time ))

    # --- --- --- ---  finally prepare for export 
    # json : 
    # - data_time_period_start
    # - data_time_period_end
    # - sample_length
    # - num_of_sample_periods
    # - list_of_sensor_ids
    # - lat_lon # in the same order as sensor_ids
    # - p1_values # in the same order as sensor_ids
    # - p2_values # in the same order as sensor_ids



    exported_data = {}
    exported_data[ 'data_time_period_start' ] = str( start_timestamp )
    exported_data[ 'data_time_period_end' ] = str( end_timestamp )
    exported_data[ 'individual_data_sample_length_in_seconds' ] = time_length_of_sample_period__in_seconds
    exported_data[ 'num_of_sample_periods' ] = num_of_sample_time_periods_in_whole_time_span__for_basic_data
    exported_data[ 'sensor_ids' ] = list_of_unique_sensor_ids.tolist() # convert to list 
    exported_data[ 'lat_lon' ] = sensors_lat_lon_list.tolist() # convert to list 
    exported_data[ 'data__p1_values' ] = out_data__resampled_sensor_values_as_rows__p1_values.tolist() # convert to list 
    exported_data[ 'data__p2_values' ] = out_data__resampled_sensor_values_as_rows__p2_values.tolist() # convert to list 


    ## --- --- save data? 

    # ---  generate text version of data
    exported_data__as_json_string = json.dumps( exported_data )

    # --- make filename 
    # 
    curr_filename = "nada.json"

    ## --  produce a different filename depending on which data generating mode we're in… 

    # if generating data from last midnight until current time
    if kind_of_time_period_we_are_doing == kind_of_time_period_we_are_doing__since_midnight:
        #curr_filename = basic_file_path_to_final_file+file_name__for__generate_data_since_midnight+file_name_suffix
        # GIVE IT A FILENAME AS IF THE FILE WAS A DAY-FILE
        # curr_timedate = pd.Timestamp.now()
        curr_filename = basic_file_path_to_final_file+file_name__for__generate_given_24_hours+str( start_date__if_doing_24_hours_data.year )+"-"+str( start_date__if_doing_24_hours_data.month )+"-"+str( start_date__if_doing_24_hours_data.day)+file_name_suffix        
    #
    # or, ir generating data from midnight to midnight
    elif kind_of_time_period_we_are_doing == kind_of_time_period_we_are_doing__one_day_only:
        # curr_timedate = pd.Timestamp.now()
        curr_filename = basic_file_path_to_final_file+file_name__for__generate_given_24_hours+str( start_date__if_doing_24_hours_data.year )+"-"+str( start_date__if_doing_24_hours_data.month )+"-"+str( start_date__if_doing_24_hours_data.day)+file_name_suffix
    # or, ir generating data from midnight to midnight
    elif kind_of_time_period_we_are_doing == kind_of_time_period_we_are_doing__date_range:
        # curr_timedate = pd.Timestamp.now()
        curr_filename = basic_file_path_to_final_file+file_name__for__generate_given_24_hours+str( start_timestamp.year )+"-"+str( start_timestamp.month )+"-"+str( start_timestamp.day)+"__"+str( end_timestamp.year )+"-"+str( end_timestamp.month )+"-"+str( end_timestamp.day)+file_name_suffix
    # feedback
    print2("\n--- time_length_of_sample_period__in_seconds__as_pandas_resampleing_time : "+time_length_of_sample_period__in_seconds__as_pandas_resampleing_time )
    print2( "\n -- -- -- -- : saving filename |"+curr_filename+"|" )


    # save!
    fp = open( curr_filename, "w" )
    fp.write( exported_data__as_json_string )
    fp.close()

    # save to a remote server too? 
    if save_file_to_remote_server == True : 
        # status : 
        print("--- saving the file |"+str( curr_filename )+"| to remove server address |"+str( remote_server_destination_directory_SCP_url )+"|")
        # ask the os to send the file 
        os.system( "sshpass -vf /home/ubuntu/.ssh2 scp "+str( curr_filename )+" "+str( remote_server_destination_directory_SCP_url ) )
        # feedback
        print("--- DONE sending file to remote server… we hope! ")

    # cleanup - dele local file pointer
    del fp


###### --------- FINISHED for real?

print2("\n ||||||||||||||||||||||||||||| ENDE - finished after "+str( time.time() - total_start_time )+" seconds |||||||||||||||||||||||||||||||||" )
 

# -------------- fin 

"""
cur.close()
conn.close()
"""
sys.exit()



