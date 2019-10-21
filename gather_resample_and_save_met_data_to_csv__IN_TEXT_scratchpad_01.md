

#### 



### Code layout : 

Parameters 
- since-midnight or 24 hours
( check which kind of input is required )
- which columns 
- how to output 
	- one file 
	- one file per column 
	- different arrangments of columns in different files
	- met.no coordinate positions in separate file
- sample period length 

Setup
- Setup sql connection 
- Find number of sample periods that fit in the given time period 
- generate sql query for relevant time period and columns
- setup out numpy arrays ; do it in an object, so you can add an arbitrary number of different data columns 

Run : gather data
- fetch sql data
	- don't forget to sort it by time … 
- add to dataframe 
- find unique combinations of lat/lon coordinates 
	- save this to a file later 
- for each lat-lon combination : 
	- setup dataframe… 
	- add start-date and end-date data rows ( from data above ) to the given data. 
	- resample 
	- save data to relevant numpy data column objects 

Save data 
	- look at the configuration of how things are to be saved… 
	- make json obj with data as to which files should have what columns ( including lat/lon )
		- eg : 
			[
				{ "filename" : "latLonPlusHumidity", "add_timestamp_to_file_TrueFale": true, columns" : [ "latlon", "humdidity"] }, 
				{ "filename" : "onlyLatLon", "add_timestamp_to_file_TrueFale": true, columns" : [ "latlon"] }, 
			]

			- NOTE : maybe make column names into variables? 
						- so they can be used in the code too! 

			- NOTE 2 : figure out which columns we need according to the file saving data? 
							- would not be that clear though :( 