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


## --- --- --- --- --- --- SWITCH ON/OFF PRINTING?
"""
print2 = print

# this takes out the print behaviour 
def print( x ):
   return 1
"""

# ====================================================== for real :) 

class gather_resample_metMo_data_make_csv:


	# ============================ PARAMETERS ============================
	# ============================ PARAMETERS ============================
	# ============================ PARAMETERS ============================

	# test var 
	test_var = "test_var"

	## --- ---  SWITCH ON/OFF PRINTING?

	
	print2 = print
	"""
	# this takes out the print behaviour 

	def print( self, x ):
	   return 1
	"""

	# --- --- ---  real vars :) 

	# --- --- FLAGS! 

	do_sql_data_fetch = True

	do_resampling_loop = True 

	saving_data = True 

	save_file_to_remote_server = True	


	# --- --- various variables

	command_line_arguments__via_sys_argv = -1


	# --- --- kind of period we're doing 

	# kind or period we're doing, samples 
	kind_of_time_period_we_are_doing__since_midnight = "kind_of_time_period_we_are_doing__since_midnight"
	kind_of_time_period_we_are_doing__one_day_only = "kind_of_time_period_we_are_doing__one_day_only"	
	kind_of_time_period_we_are_doing__date_range = "kind_of_time_period_we_are_doing__date_range"

	# final kind of period we're doing 
	kind_of_time_period_we_are_doing = -1 

	# --- --- timing 

	db_search_starttime = -1 
	db_search_endtime = -2

	# for inserting into dataframe… 
	df_starttime_row = -1 
	df_endtime_row = -2

	# ( defined according to present or absent commandline input )
	start_date__if_doing_24_hours_data = 0

	# sample time length
	time_length_of_sample_period__in_seconds = 60*8
	# in pandas compatible time description format… 
	time_length_of_sample_period__in_seconds__as_pandas_resampleing_time = str(time_length_of_sample_period__in_seconds)+"S"
	print2("\n--- time_length_of_sample_period__in_seconds__as_pandas_resampleing_time : "+time_length_of_sample_period__in_seconds__as_pandas_resampleing_time )

	# how many time sample periods can fit in the current total data time period
	num_of_sample_time_periods_fit_in_total_sampled_period = 0 



	# --- --- sql vars 

	sql_query = 0

	# database name 
	database_name = "met_no_data"
	# database table name 
	database_table_name__sensor_grid = "met_no_fetched_data__sensor_grid"
	database_table_name__low_res_grid = "met_no_fetched_data__low_res_grid"
	database_table_name__current__db_table_name = database_table_name__sensor_grid

	# cursor 
	cur = -1


	# --- ---- data!

	# RELEVANT COLUMN NAMES? 
	# EXAMPLE 
	# EXAMPLE 
	# EXAMPLE 
	desired_data_export_metadata = [ { "filename" : "latLonPlusHumidity", "add_timestamp_to_file_TrueFale" : True, "columns" : [ "lat", "lon", "humdidity"] }, { "filename" : "onlyLatLon", "add_timestamp_to_file_TrueFale": True, "columns" : [ "temperature", "winddirection", "windspeed", "pressure"] } ]
	# EXAMPLE 
	# EXAMPLE 
	# EXAMPLE 


	# MAKE THIS FROM THE DAtA EXPORT META-DATA?
	# EXAMPLE 
	# EXAMPLE 
	# EXAMPLE 
	gathered_desired_data_columns = ["GENERATE", "THIS", "DATA", "FROM", "THE", "desired_data_export_metadata", "metadata!"]
	""" it's like this in the non-object_oriented file : 
	columns_names =  ['sensor_id', 'sensor_name', 'lat', 'lon', 'timestamp', 'p1', 'p2']    # REALTIME DATA 
	"""
	# EXAMPLE 
	# EXAMPLE 
	# EXAMPLE 


	# fetched data goes here :) 
	fetched_sql_data_as_sql_data = -3
	fetched_sql_data_as_a_pd_dataframe = -4 

	# from the sql data
	num_of_unique_sensor_ids = 781 

	# in between fast storage of resampled data
	resampled_data_as_np_array = -1 


	# -- Filenames

	basic_file_path_to_final_file = "/mnt/virtio-bbc6cf3a-042b-4410-9/luftdaten/luftdaten_daten/tabular_data/tabular_ld_data_TEST_AREA/"


	# filename beginning for files about the laest data

	file_name__for__generate_data_since_midnight = "latest_data_since_midnight"

	file_name__for__generate_given_24_hours = "24_hrs_pm_data__starting_from__"

	file_name_suffix = ".json"


	# --   DATA URLS 


	nordic_midnight_24_hrs_data__url = "/Users/miska/Documents/open_something/luftdaten/luftdaten_code/luftdaten__make_tabular_data__from_db_data/ld_NYE_midnight_24hrs_nordics_all_data_01.csv"
	## nordic_midnight_24_hrs_data__url = "/home/miska/documents/opensomething/luftdaten/dustmin_to_csv__various_code/ld_NYE_midnight_24hrs_nordics_all_data_01.csv"
	# nordic_midnight_24_hrs_data__url = "/Users/miska/temp_temp_temp/sds011_files_wo_microsecond_nulling/2018-09_sds011.csv_cleaned.csv"
	nordic_midnight_24_hrs_data__url = "/mnt/virtio-bbc6cf3a-042b-4410-9/luftdaten/luftdaten_daten/2019-01_sds011.csv_cleaned__FIRST_500k.csv"

	# DATA URL 
	curr_url = nordic_midnight_24_hrs_data__url


	# REMOTE DIRECTORY FOR FILES 
	remote_server_destination_directory_SCP_url = "sourisr@kapsi.fi:sites/sourisr.kapsi.fi/www/luftdaten/luftDaten_data_explorations/ld_daten_various/tabular_ld_data__480_s_intervals/"


	# ============================  METHODS ============================
	# ============================  METHODS ============================
	# ============================  METHODS ============================

	# --------------  *test*  methods :)  *test* 

	def test_method(self):
		self.test_var = self.test_var+str(2)
		print( ">>>> test_method : self.test_var : |"+self.test_var+"|")

		self.test_method2()


	def test_method2(self):
		print(">>>> test_method2 : self.test_var : |"+self.test_var+"|")
		starttime = time.time() 

		# --- --- --- code goes here 

		# ---------- finnnisage 
		print("|||| and that took "+str( time.time() - starttime)+" ms ")


	# -------------- **real**  methods :)  **real**


	# --- --- timing 

	def check_which_kind_of_time_period_were_doing_accoding_to_command_line_arguments( self ):
		print("\n >>>> check_which_kind_of_time_period_were_doing_accoding_to_command_line_arguments() ")
		print("\t --- argv looks like his : |"+str( self.command_line_arguments__via_sys_argv )+"| ")

		# --- --- --- code goes here 

		# no input means do a from-midnight run 
		if len( self.command_line_arguments__via_sys_argv ) == 1: 

			self.kind_of_time_period_we_are_doing = self.kind_of_time_period_we_are_doing__since_midnight

			self.db_search_starttime = self.return_todays_date__at_midnight__as_pd_timestamp()
			self.db_search_endtime = self.db_search_starttime + pd.DateOffset(1)

		# one input means do a run from the given time plus 24 hours 
		if len( self.command_line_arguments__via_sys_argv ) == 2: 

			self.kind_of_time_period_we_are_doing = self.kind_of_time_period_we_are_doing__one_day_only

			self.db_search_starttime = self.check_date_string_validity_and_return_pd_timestmp_if_valid( self.command_line_arguments__via_sys_argv[1] )
			self.db_search_endtime = self.db_search_starttime + pd.DateOffset(1)

		# one input means do a run from the given time plus 24 hours 
		if len( self.command_line_arguments__via_sys_argv ) == 3: 

			self.kind_of_time_period_we_are_doing = self.kind_of_time_period_we_are_doing__date_range

			self.db_search_starttime = self.check_date_string_validity_and_return_pd_timestmp_if_valid( self.command_line_arguments__via_sys_argv[1] )
			self.db_search_endtime = self.check_date_string_validity_and_return_pd_timestmp_if_valid( self.command_line_arguments__via_sys_argv[2] )		


		# ----------- finnisage : 
		print("--- finally : \n\t  kind_of_time_period_we_are_doing : "+self.kind_of_time_period_we_are_doing+" | db_search_starttime/endtime : \n\t\t "+str( self.db_search_starttime )+" \n\t\t ---> "+str( self.db_search_endtime) )



	def check_date_string_validity_and_return_pd_timestmp_if_valid( self, in_date_to_test ):
		print( "\n>>>> check_date_string_validity_and_return_pd_timestmp_if_valid() - checking in_date : |"+str( in_date_to_test )+"|" )

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

		# ---------- finnnisage ----------




	def find_num_of_sample_periods_that_will_fit_in_start_to_end_time_period( self ):
		print("\n >>>> find_num_of_sample_periods_that_will_fit_in_start_to_end_time_period() ")

		print("--- got sample lengh period of |"+str( self.time_length_of_sample_period__in_seconds )+"| \n\t and start/end times of  \n\t\t "+str( self.db_search_starttime )+" \n\t\t ---> "+str( self.db_search_endtime) )

		total_time = self.db_search_endtime - self.db_search_starttime
		total_time = total_time.total_seconds()

		print("--- got total seconds count of "+str( total_time ) )

		self.num_of_sample_time_periods_fit_in_total_sampled_period = int( total_time / self.time_length_of_sample_period__in_seconds )

		print("--- got self.num_of_sample_time_periods_fit_in_total_sampled_period ( BEFORE +1 trick ) of |"+str( self.num_of_sample_time_periods_fit_in_total_sampled_period )+"|" )

		# remember the off-by-one-trick… 
		self.num_of_sample_time_periods_fit_in_total_sampled_period = self.num_of_sample_time_periods_fit_in_total_sampled_period + 1 




	def return_todays_date__at_midnight__as_pd_timestamp( self ):

		print(">>>> return_todays_date_as_pd_timestamp() ");

		midnight_timestamp_from_today = -1;

		now_timestamp = pd.Timestamp.now()

		midnight_timestamp_from_today = pd.Timestamp( now_timestamp.year, now_timestamp.month, now_timestamp.day, 0 )

		print("--- returning : |"+str( midnight_timestamp_from_today )+"| " )

		return midnight_timestamp_from_today


	# --- --- data! 



	# setup data holding object(s) in regards to num or samples and columns … 
	def setup_out_data_objects__according_to_sample_length_and_desired_columns( self ):
		print("\n>>>> setup_out_data_objects__according_to_sample_length_and_desired_columns() ")

		# make basic array 
		self.resampled_data_as_np_array = np.array( np.zeros( len( gathered_desired_data_columns ) *  self.num_of_unique_sensor_ids * self.num_of_sample_time_periods_fit_in_total_sampled_period  ) )

		print( "--- got self.num_of_sample_time_periods_fit_in_total_sampled_period = "+str(self.num_of_sample_time_periods_fit_in_total_sampled_period)+" | len( gathered_desired_data_columns ) = "+str( len( gathered_desired_data_columns ) )+" | self.num_of_unique_sensor_ids : "+str( self.num_of_unique_sensor_ids )+" == array length of "+str( self.resampled_data_as_np_array.shape ) )

		# reshape 
		self.resampled_data_as_np_array  = self.resampled_data_as_np_array.reshape( [ len( gathered_desired_data_columns ),self.num_of_unique_sensor_ids, self.num_of_sample_time_periods_fit_in_total_sampled_period ] )

		#
		print("\t --- and this reshaped looks like this : "+str( self.resampled_data_as_np_array.shape ))




	"""
		Idea: 
			- setup object that will hold data in the end 
				- but don't save to this object until the end ( see end of this )

			- to optimise speed, work with numpy arrays while processing data
			- THEN save data to data object 

			- in the end, when making out data files, use the data object
				to pull processed data from

		Don't forget : 
			- list of sensors… 

		OR OR OR : 
			- you could make a widely multidimensional numpy array, 
				for all the columns data, and later separate it out… 


		TRY 2 : 
			- save initial data to 3-demensional numpy array? 
				( dimensions : [ weather parameter ][ time index ][ actual value  ] )
			- the order of the weather parameter dimensions is in the order of desired columns

			- pseudo-pseudo-code : 
				- setup array : 
					- length : ( columns.length * time.length * 1variable )
				- loop : 
					- get all sensor values for given lat/lon combination 

	"""

	# --- --- SQl bits 

	def setup_psql_connection( self ):
		print(">>>> setup_psql_connection() ")

		self.conn = psycopg2.connect("dbname='"+self.database_name+"' user='postgres' password='secret' host='localhost' ")

		self.cur = self.conn.cursor()
		# --- fin! 



	def generate_sql_query_string( self ):
		print(">>>> generate_sql_query_string() ")

		# generate quary string 
		self.sql_query = "SELECT * FROM "+self.database_name+" WHERE timestamp > '"+str( self.db_search_starttime )+"' AND timestamp < '"+str( self.db_search_endtime )+"' ORDER BY forecast_timestamp DESC"
		self.print2( "-- -- generated psql query : |"+self.sql_query+"|" )



	def do_sql_data_data_fetch__convert_to_pd_dataframe( self ):
		print(">>>> do_sql_data_data_fetch__convert_to_pd_dataframe() ")
		starttime = time.time() 

		# --- --- code goes here 

		# generate sql query 
		self.generate_sql_query_string()

		mini_timing = time.time() 
		cur.execute( self.sql_query )

		self.print2("-- --- query ready at "+str( time.time() - mini_timing ) ) 

		self.fetched_sql_data_as_a_pd_dataframe = pd.DataFrame( self.cur.fetchall(), columns=self.gathered_desired_data_columns )

		self.print2("\n --- --- got table of shape "+str( in_data.shape )+" "+str( time.time() - mini_timing ) )    
		self.print2("--- --- got columns : |"+str( in_data.columns)+"|" )
		# --- POST READ DATA : set up data - timestamps and all! 

		# aha - timestamp column not a timestamp column?
		# - let's fix 
		in_data['forecast_timestamp'] = pd.to_datetime( in_data['forecast_timestamp'] )

		# set the timestamp column as the index 
		in_data = in_data.set_index( 'forecast_timestamp' )

		# sort data chronologically, by the index 
		in_data = in_data.sort_index()

		# --- finnisage! 
		print("|||| and that took "+str( time.time() - starttime)+" ms ")



	# for testing wihout net connection! 
	def load_data_from_csv( self ):
		print(">>>> load_data_from_csv() ")
		starttime = time.time() 

		# --- finnisage! 
		print("|||| and that took "+str( time.time() - starttime)+" ms ")


	# make a column with unique lat/lon names 
	# - so you can find the unique lat lon combinations … and save them!
	# 		( not in the least to get the number of sensors… which is then used to format the data )
	#
	# 	- make an int of this number? 
	def create_unique_concatenated_latlon_string_column( self ):
		print("\n>>>> create_unique_concatenated_latlon_string_column() ")

		# speed testing … 
		starttime = time.time() 




		# ----- finnisage! 
		print("|||| and that took "+str( time.time() - starttime)+" ms ")		


	# --- --- columns

	def gather_column_names_from_output_files_metadata( self ):
		print("\n >>>> gather_column_names_from_output_files_metadata() ")
		print("\n --- input looks like this : ")
		print( self.desired_data_export_metadata )

		self.gathered_desired_data_columns = []

		for meta_data_item in self.desired_data_export_metadata:

			for column_name in meta_data_item['columns']:

				if column_name not in self.gathered_desired_data_columns: 

					self.gathered_desired_data_columns.append( column_name )

		print("--- gathered column names look like this : ")
		print( self.gathered_desired_data_columns )





	# ============================  RUN …for fun ============================
	# ============================  RUN …for fun ============================
	# ============================  RUN …for fun ============================


	def __init__(self, one=1, two=2):
		self.one = one
		self.two = two 

		print(" __init__ : one : "+str( self.one )+" two : "+str( self.two ) )

		# self variable scope test :) 
		self.test_method()


		# --- --- --- real things :) 


		# --- get command line arguments 
		self.command_line_arguments__via_sys_argv = sys.argv


		# --- --- --- startup 
		self.run_me()



	# main script? 
	def run_me( self ):
		print( ">>>> run_me :) ")

		starttime = time.time()

		# ----------------- runme … 

		# gather needed column names 
		self.gather_column_names_from_output_files_metadata()

		# figure out whether we're doing since-midnight, or the whole day 
		self.check_which_kind_of_time_period_were_doing_accoding_to_command_line_arguments()

		# find the number of sample length periods in given time period 
		self.find_num_of_sample_periods_that_will_fit_in_start_to_end_time_period()

		# setup out arrays (object) accordingly to sample lengths and number of columns 
		self.setup_out_data_objects__according_to_sample_length_and_desired_columns()


		# ----------------- finnisage! 

		print("|||| runme took "+str( time.time() - starttime)+" ms ")



# ------------------- end of class 



# ---- ---- --- RUN? 

print(">>>> WELCOME! <<<< ")
gather_resample_make_csv = gather_resample_metMo_data_make_csv( 1, 2)



# ====================================================== for real :) 