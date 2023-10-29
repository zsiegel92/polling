import pandas as pd
import numpy as np
import os
# import numpy as np
# from datetime import datetime
from config import georgia_pp_2016_fname, georgia_pp_2016_dir
# from shapefile_utilities import get_interested_metro_properties_by_namelsad, get_properties_only
# from geojson_utilities import create_point, list_of_points_to_featureCollection
# from geocode_utilities import geocode_address, bulk_geocode_mapquest
from csv_utilities import get_headers, write_csv, get_max_characters_in_cols,get_number_rows


from db_utilities import generate_report, create_table, append_df_to_table, update_records,get_number_records, sample_where, select_by_empty_columns, count_where,generate_report_query, where_multiple, add_null_column,drop_table,drop_column,add_fkey_column, add_null_columns_fast, modify_table_verbose,add_null_column, execute_raw_sql,select_where


from geocode_utilities import geocode_address
# from metro_county_delineation import  get_ohio_county_numbers_for_counties_with_pps

### SETTING UP DB START
def get_all_headers(fnames,sep=","):
	return list(dict.fromkeys([header for fname in fnames for header in get_headers(fname,sep=sep)]).keys())

def get_max_characters_in_cols_multiple_files(filenames,sep=",",pad=0):
	max_chars_all = {}
	for filename in filenames:
		max_chars = get_max_characters_in_cols(filename,sep=sep,risky=True)
		max_chars_all = {k : max(max_chars_all.get(k,0), max_chars.get(k,0)) for k in {**max_chars_all, **max_chars} }
	return {k : v + pad for k,v in max_chars_all.items() }


# primary_key 'SOS_VOTERID' for ohio
# for georgia, use 'REGISTRATION_NUMBER'
def create_db_with_appropriate_col_sizes(tablename,fnames,primary_key=None,sep=",",pad=10,counter_index="pp_identifier"):
	headers = get_all_headers(fnames)
	header_col_sizes = get_max_characters_in_cols_multiple_files(fnames,pad=pad)
	header_col_sizes = {k : header_col_sizes[k] for k in headers} # reorder to default order
	# for filefeature_key,size in extract_query_features_or_sizes(filename=None).items():
		# header_col_sizes[filefeature_key] = size
	# print(header_col_sizes)
	# header_col_sizes['longitude'] = 30
	create_table(tablename,header_col_sizes,primary_key=None,padstyle=None,counter_index=counter_index)

# if filename is None, returns appropriate column sizes for features
def extract_query_features_or_sizes(filename=None):
	if filename is None:
		return {'filename' : 250,'metro' : 250,'spacing' : 30,'query_version' : 10}
	else:
		basename = filename.rsplit("/",1)[-1].rsplit(".",1)[0]
		if not basename.endswith("km"):
			basename, query_version = basename.rsplit(" ",1)
		else:
			query_version = 1
		basename, spacing = basename.rsplit("_spacing_",1)
		spacing = spacing[:-2] #remove km
		metro = basename.lstrip("civicAPI_points_").replace("_"," ")
		return { 'filename' : filename, 'metro': metro,'spacing' : spacing,'query_version' : query_version}

def ingest_csvs(tablename,csv_filenames,chunksize=200000,sep=",",skip_blocks=0,quotenone=False):
	# create_db_with_appropriate_col_sizes(tablename='voters')
	import math
	import time
	import datetime
	nFiles = len([georgia_pp_2016_fname])
	for i, filename in enumerate([georgia_pp_2016_fname]):
		start_time = time.time()
		nchunks =math.ceil((get_number_rows(filename)-1) / chunksize)
		for j, chunk_df in enumerate(pd.read_csv(filename,chunksize=chunksize,sep=sep)):
			# for filefeature_key,filefeature_val in fname_features.items():
				# chunk_df[filefeature_key] = filefeature_val # constant column
			append_df_to_table(tablename,chunk_df)
			print(f"File {i+1}/{nFiles} CHUNK {j+1}/{nchunks} -- {str(datetime.timedelta(seconds = (delt := (time.time()-start_time))))} [{delt}s]")




def geocode_all_google(df):
	nrows = df.shape[0]
	for i in range(nrows):
		try:
			latlon = geocode_address(df.iloc[i]['address'])
			df.at[i,'latitude'] =  latlon[0]
			df.at[i,'longitude'] =  latlon[1]
		except Exception as e:
			print(e)
			print(f"Error on {df.iloc[i]['address']}")
			print("continuing geocoding...")
	df['geocode_source'] = 'google'
	return df


def geocode_pps(tablename,report=True):
	reference_cols= ['address']
	geocoding_cols = reference_cols + [ "latitude","longitude"]
	add_null_columns_fast(tablename=tablename,col_names=['geocode_source'] + geocoding_cols,column_type=50,pkeys=['pp_identifier'])
	# where_uncoded = where_multiple(emptycols=['latitude','longitude'])
	# it = enumerate(select_where(tablename,chunksize=50,cols=None))
	# i,df = next(it)
	modify_table_verbose(tablename,geocode_all_google,'pp_identifier',chunksize=100,n_cores=1)
	if report:
		generate_report_geocoded(tablename)

### HIGH-LEVEL QUERIES START
# USING:
# where_multiple(emptycols=[],nonemptycols=[],equalscols={},incols={})

def select_by_has_latlon(tablename,want_geocoded=True,chunksize=200000,onlyquery=False):
	if want_geocoded:
		return select_by_empty_columns(tablename,nonempty=['latitude','longitude'],chunksize=chunksize,onlyquery=onlyquery)
	else:
		return select_by_empty_columns(tablename,empty=['latitude','longitude'],chunksize=chunksize,onlyquery=onlyquery)

### HIGH-LEVEL QUERIES END


### WRITING REPORTS START
def report_outfilename(tablename,base="all",extra=""):
	return f'{georgia_pp_2016_dir}/{tablename}_{base}_rows{"_"+ extra if extra else ""}.csv'

def generate_report_geocoded(tablename):
	filename=report_outfilename(tablename=tablename,base="geocoded",extra="")
	query = select_by_has_latlon(tablename,want_geocoded=True,onlyquery=True)
	generate_report_query(tablename,filename,query)

def generate_report_all_pps(tablename):
	filename=report_outfilename(tablename=tablename,base="geocoded",extra="")
	query = select_where(tablename,onlyquery=True)
	generate_report_query(filename,query,chunksize=10000)

def generate_report_all(tablename):
	filename = report_outfilename(tablename=tablename)
	generate_report(tablename,filename)

### WRITING REPORTS END

# from csv_utilities import get_pandas_in_chunks


def add_const_placetype_column(tablename):
	add_null_column(tablename=tablename,col_name='placeType',column_type=50)
	# "placeType" "pollingLocation"
	execute_raw_sql(f'''
	                UPDATE {tablename}
					SET "placeType" = 'pollingLocation';
					''')
# if __name__=="__main__":
# 	filename= "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/Polling/Georgia/Polling Places (Special Election 2021)/civicAPI_points_Albany,_GA_Metro_Area_spacing_0.6km.csv"
# 	sep = ","
# 	print(get_max_characters_in_cols(filename,sep=","))
# 	# headers = get_headers(filename,sep=sep)
# 	# max_chars = [0 for header in headers]
# 	# for chunk in pd.read_csv(filename,chunksize=100000,sep=sep,dtype=str):
# 	# 	pass
# 	# return 5


if __name__=="__main__":
	tablename = "georgia_pps_2016"
	geocode_pps(tablename,report=True)
	generate_report_geocoded(tablename)
	# drop_table(tablename)
	# create_db_with_appropriate_col_sizes(tablename,[georgia_pp_2016_fname])
	# ingest_csvs(tablename,[georgia_pp_2016_fname])
	# add_const_placetype_column(tablename)



	# add_null_column(tablename,'test_column',characters=None)
	# add_fkey_column(tablename,'test_reference2','georgia_voters')
	# drop_column(tablename,'test_column')

print("\a")
