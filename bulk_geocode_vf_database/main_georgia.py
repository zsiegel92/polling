import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
from multiprocessing import  Pool
# import numpy as np
# from datetime import datetime
from config import georgia_vf_dir, georgia_vf_filenames
# from shapefile_utilities import get_interested_metro_properties_by_namelsad, get_properties_only
# from geojson_utilities import create_point, list_of_points_to_featureCollection
# from geocode_utilities import geocode_address, bulk_geocode_mapquest
from grid_utilities import measure_distance
from csv_utilities import get_headers, write_csv, get_max_characters_in_cols, get_pandas_in_chunks, get_number_rows
from db_utilities import generate_report, create_table, append_df_to_table, update_records,get_number_records, sample_where, select_by_multiple, count_where,generate_report_query, where_multiple, add_null_column, add_fkey_column,select_all, drop_column,select_join,copy_table,update_records_fast, table_colnames,add_null_columns,add_null_columns_fast,modify_table, modify_table_verbose,select_where


from geocode_utilities import geocode_address,bulk_geocode_census
from census_utilities import get_census_data,demographic_data_tables_lookup,get_census_data_safe


### SETTING UP DB START
def get_all_headers(fnames,sep=","):
	return list(dict.fromkeys([header for fname in fnames for header in get_headers(fname,sep=sep)]).keys())

def get_max_characters_in_cols_multiple_files(filenames,sep=","):
	max_chars_all = {}
	for filename in filenames:
		max_chars = get_max_characters_in_cols(filename,sep=sep)
		max_chars_all = {k : max(max_chars_all.get(k,0), max_chars.get(k,0)) for k in {**max_chars_all, **max_chars} }
	return max_chars_all

def get_georgia_header_col_sizes():
	# return get_max_characters_in_cols_multiple_files(georgia_vf_filenames,sep="|")
	return {'COUNTY_CODE': 11, 'REGISTRATION_NUMBER': 19, 'VOTER_STATUS': 12, 'LAST_NAME': 30, 'FIRST_NAME': 30, 'MIDDLE_MAIDEN_NAME': 30, 'NAME_SUFFIX': 11, 'NAME_TITLE': 10, 'RESIDENCE_HOUSE_NUMBER': 22, 'RESIDENCE_STREET_NAME': 51, 'RESIDENCE_STREET_SUFFIX': 23, 'RESIDENCE_APT_UNIT_NBR': 23, 'RESIDENCE_CITY': 19, 'RESIDENCE_ZIPCODE': 17, 'BIRTHDATE': 9, 'REGISTRATION_DATE': 17, 'RACE': 4, 'GENDER': 6, 'LAND_DISTRICT': 13, 'LAND_LOT': 8, 'STATUS_REASON': 13, 'COUNTY_PRECINCT_ID': 18, 'CITY_PRECINCT_ID': 16, 'CONGRESSIONAL_DISTRICT': 22, 'SENATE_DISTRICT': 15, 'HOUSE_DISTRICT': 14, 'JUDICIAL_DISTRICT': 17, 'COMMISSION_DISTRICT': 19, 'SCHOOL_DISTRICT': 15, 'COUNTY_DISTRICTA_NAME': 21, 'COUNTY_DISTRICTA_VALUE': 22, 'COUNTY_DISTRICTB_NAME': 21, 'COUNTY_DISTRICTB_VALUE': 22, 'MUNICIPAL_NAME': 19, 'MUNICIPAL_CODE': 14, 'WARD_CITY_COUNCIL_NAME': 22, 'WARD_CITY_COUNCIL_CODE': 22, 'CITY_SCHOOL_DISTRICT_NAME': 25, 'CITY_SCHOOL_DISTRICT_VALUE': 26, 'CITY_DISTA_NAME': 15, 'CITY_DISTA_VALUE': 16, 'CITY_DISTB_NAME': 15, 'CITY_DISTB_VALUE': 16, 'CITY_DISTC_NAME': 15, 'CITY_DISTC_VALUE': 16, 'CITY_DISTD_NAME': 15, 'CITY_DISTD_VALUE': 16, 'DATE_LAST_VOTED': 15, 'PARTY_LAST_VOTED': 16, 'DATE_ADDED': 10, 'DATE_CHANGED': 12, 'DISTRICT_COMBO': 14, 'RACE_DESC': 33, 'LAST_CONTACT_DATE': 17, 'MAIL_HOUSE_NBR': 20, 'MAIL_STREET_NAME': 87, 'MAIL_APT_UNIT_NBR': 25, 'MAIL_CITY': 50, 'MAIL_STATE': 10, 'MAIL_ZIPCODE': 12, 'MAIL_ADDRESS_2': 49, 'MAIL_ADDRESS_3': 30, 'MAIL_COUNTRY': 20}
georgia_header_col_sizes = get_georgia_header_col_sizes()

# primary_key 'SOS_VOTERID' for ohio
# for georgia, use 'REGISTRATION_NUMBER'
def create_db_with_appropriate_col_sizes(tablename,primary_key='REGISTRATION_NUMBER',sep=","):
	headers = get_all_headers(georgia_vf_filenames,sep=sep) # has desired order
	# ohio_header_col_sizes = get_max_characters_in_cols_multiple_files(ohio_vf_filenames) #result dumped into config
	header_col_sizes = {k : georgia_header_col_sizes[k] for k in headers} # reorder to default order
	header_col_sizes['latitude'] = 30
	header_col_sizes['longitude'] = 30
	create_table(tablename,header_col_sizes,primary_key=primary_key)


def ingest_csvs(chunksize=200000,tablename='georgia_voters',sep=",",skip_blocks=0,quotenone=False):
	# create_db_with_appropriate_col_sizes(tablename='voters')
	import math
	import time
	import datetime
	nFiles = len(georgia_vf_filenames)
	for i, filename in enumerate(georgia_vf_filenames):
		start_time = time.time()
		nchunks =math.ceil((get_number_rows(filename)-1) / chunksize)
		for j, chunk_df in enumerate(get_pandas_in_chunks(filename,chunksize,skip_blocks=skip_blocks,sep=sep,quotenone=quotenone)):
			append_df_to_table(tablename,chunk_df)
			print(f"File {i+1}/{nFiles} CHUNK {j+1}/{nchunks} -- {str(datetime.timedelta(seconds = (delt := (time.time()-start_time))))} [{delt}s]")

### SETTING UP DB END



def get_number_rows_in_vfs():
	return sum([get_number_rows(filename) for filename in georgia_vf_filenames])

def select_by_has_latlon(tablename,want_geocoded=True,chunksize=200000,onlyquery=False):
	if want_geocoded:
		return select_by_multiple(tablename,nonempty=['latitude','longitude'],chunksize=chunksize,onlyquery=onlyquery)
	else:
		return select_by_multiple(tablename,empty=['latitude','longitude'],chunksize=chunksize,onlyquery=onlyquery)

def get_all_pps(pp_tablename):
	return select_all(pp_tablename,chunksize=None)

def get_geocoded_without_assigned_pp(tablename,fkey_name='closest_pp',other_null_col='pp_dist',chunksize=None,onlyquery=False):
	return select_by_multiple(tablename,nonempty=['latitude','longitude'],null=[fkey_name,other_null_col],chunksize=chunksize,onlyquery=onlyquery)

def get_vf_assigned_pp(tablename,chunksize=None,onlyquery=False):
	return select_by_multiple(tablename,nonnull=['closest_pp','pp_dist'],chunksize=chunksize,onlyquery=onlyquery)


### WRITING REPORTS START
def report_outfilename(tablename,base="all",extra=""):
	return f'{georgia_vf_dir}/{tablename}_{base}_rows{("_"+ extra) if extra else ""}.csv'

def generate_report_geocoded(tablename):
	filename=report_outfilename(tablename=tablename,base="geocoded",extra="")
	query = select_by_has_latlon(tablename,want_geocoded=True,onlyquery=True)
	generate_report_query(filename,query)

def generate_report_all(tablename):
	filename = report_outfilename(tablename=tablename)
	generate_report(tablename,filename)

# tablename,pp_tablename="georgia_pps",fkey_name='closest_pp',pp_id='pp_identifier'
def generate_report_geocoded_assigned(vf_tablename,pp_tablename="georgia_pps",jointuple=('closest_pp','pp_identifier')):
	filename=report_outfilename(tablename=vf_tablename,base="geocoded_assigned",extra="")
	where1 = where_multiple(nonemptycols=['latitude','longitude'],nonnullcols=[jointuple[0]],tablename=vf_tablename)
	query = select_join(vf_tablename,pp_tablename,jointuple,where1=where1,onlyquery=True)
	# query = select_by_has_latlon(tablename,want_geocoded=True,onlyquery=True)
	generate_report_query(filename,query)
### WRITING REPORTS END





## CENSUS
def extract_address(row):
	# address_keys= ('RESIDENTIAL_ADDRESS1', 'RESIDENTIAL_SECONDARY_ADDR','RESIDENTIAL_CITY','RESIDENTIAL_STATE','RESIDENTIAL_ZIP','RESIDENTIAL_ZIP_PLUS4') #ohio
	address_keys1 = ("RESIDENCE_HOUSE_NUMBER", "RESIDENCE_STREET_NAME", "RESIDENCE_STREET_SUFFIX", "RESIDENCE_CITY")
	address_keys2 = ("RESIDENCE_ZIPCODE",)
	return " ".join([v for k in address_keys1 if (v:=row[k])] + ["Georgia"] + [v for k in address_keys2 if (v:=row[k])]) #join nonempty elements, avoid re-indexing


def geocode_all(df):
	nrows = df.shape[0]
	for i in range(nrows):
		try:
			latlon = geocode_address(extract_address(df.iloc[i]))
			df.iloc[i]['latitude'] =  latlon[0]
			df.iloc[i]['longitude'] =  latlon[1]
		except Exception as e:
			print(e)
			print(f"Error on {extract_address(df.iloc[i])}")
			print("continuing geocoding...")
	df['geocode_source'] = 'google'

def sanitize_col(col):
	return col.astype(str).apply(lambda row: row.replace(","," "))

# replace commas with spaces
# collapse all whitespace to a single space
def sanitize_rowdict(rowdict):
	return { k : " ".join(v.replace(","," ").split()) for k,v in rowdict.items()}

#headers of df should have: 'latitude','longitude', 'REGISTRATION_NUMBER', 'RESIDENCE_HOUSE_NUMBER', 'RESIDENCE_STREET_NAME', 'RESIDENCE_STREET_SUFFIX', 'RESIDENCE_CITY', 'RESIDENCE_ZIPCODE'
# for geocode_utilities.bulk_geocode_census headers should be ['id', 'street', 'city', 'state', 'zip']
def geocode_all_census(df):
	geocoding_columns = ['id','street','city','state','zip']
	new_df = pd.DataFrame(columns=geocoding_columns)
	new_df['id'] = sanitize_col(df['REGISTRATION_NUMBER']).values
	new_df['street'] = sanitize_col(df['RESIDENCE_HOUSE_NUMBER'] + " " +  df['RESIDENCE_STREET_NAME'] + " " + df['RESIDENCE_STREET_SUFFIX']).values
	new_df['city'] = sanitize_col(df['RESIDENCE_CITY']).values
	new_df['zip'] = sanitize_col(df['RESIDENCE_ZIPCODE']).values
	new_df['state'] = 'Georgia'
	new_df.set_index('id',inplace=True)
	geocoded = bulk_geocode_census(new_df)
	# geocoded.rename(columns={'id': 'REGISTRATION_NUMBER'},inplace=True)
	for feature in ['latitude','longitude']:
		d = geocoded.set_index('id')[feature].to_dict()
		df[feature] = df['REGISTRATION_NUMBER'].map(d)
	df['geocode_source'] = 'census'
	# return df


def geocode_sample(tablename,fraction=.0001,report=False):
	geocoding_cols = ("REGISTRATION_NUMBER", "RESIDENCE_HOUSE_NUMBER", "RESIDENCE_STREET_NAME", "RESIDENCE_STREET_SUFFIX", "RESIDENCE_CITY", "RESIDENCE_ZIPCODE", "latitude","longitude")
	add_null_columns_fast(tablename=tablename,col_names=['geocode_source'],column_type=50,pkeys=['REGISTRATION_NUMBER'],fkeys=[('closest_pp','pp_identifier','georgia_pps')])
	where_uncoded = where_multiple(emptycols=['latitude','longitude']) #just get all counties for Georgia
	n_total = get_number_records(tablename)
	n_uncoded=count_where(tablename,where_uncoded)
	# n_coded = count_nonempty_rows(tablename,['latitude','longitude'])
	n_to_geocode = int(fraction*n_total)
	fraction_uncoded = n_to_geocode/n_uncoded
	# modify_table_verbose(tablename,geocode_all,fraction=fraction_uncoded,where=where_uncoded,max_applications=n_to_geocode,cols=geocoding_cols)
	modify_table(tablename,geocode_all_census,fraction=fraction_uncoded,where=where_uncoded,max_applications=n_to_geocode,cols=geocoding_cols)
	if report:
		generate_report_geocoded(tablename)

def ensure_census_cols(vf_tablename):
	census_headers = list(demographic_data_tables_lookup.values()) + ['queried_census']
	cols = table_colnames(vf_tablename)
	add_null_columns_fast(tablename=vf_tablename,col_names=[header for header in census_headers if header not in cols] + ['queried_census'],column_type=50,pkeys=['REGISTRATION_NUMBER'],fkeys=[('closest_pp','pp_identifier','georgia_pps')])
	return census_headers


def append_census_df(vf_df):
	census_data = vf_df.apply(lambda row: get_census_data_safe(row['latitude'],row['longitude']), axis=1)
	census_df = pd.DataFrame(list(census_data.values))
	vf_df.update(census_df)
	vf_df['queried_census'] = 'YES'
	return vf_df
	# for i, row in vf_df.iterrows():
	# 	census_data = get_census_data_safe(row['latitude'],row['longitude']) # returns {} if error or no data
	# 	for k,v in census_data.items():
	# 		vf_df.at[i,k] = v

# https://towardsdatascience.com/make-your-own-super-pandas-using-multiproc-1c04f41944a1
def parallelize_dataframe(df, func, n_cores=4):
	if n_cores==1:
		return func(df)
	df_split = np.array_split(df, n_cores)
	pool = Pool(n_cores)
	df = pd.concat(pool.map(func, df_split))
	pool.close()
	pool.join()
	return df

def append_census(vf_tablename,chunksize=100,nParallel=1):
	census_headers = ensure_census_cols(vf_tablename)
	where = where_multiple(nonemptycols=['latitude','longitude'],allnullcols=['queried_census'])
	n_queries = count_where(vf_tablename,where)
	print(f"Making {n_queries} CENSUS QUERIES TOTAL!")
	print(where)
	vf_chunks = select_where(vf_tablename,where=where,chunksize=chunksize,onlyquery=False,cols=None)
	for vf_chunk in vf_chunks:
		vf_chunk = parallelize_dataframe(vf_chunk,append_census_df,n_cores=nParallel)
		print(f"Got census data for {chunksize} individuals")
		update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=census_headers)





def find_closest(vf_tablename,pp_tablename):
	cols = table_colnames(vf_tablename)
	if 'closest_pp' not in cols:
		add_fkey_column(vf_tablename,'closest_pp',"georgia_pps",'pp_identifier')
	if 'pp_dist' not in cols:
		add_null_column(tablename=vf_tablename,col_name="pp_dist",column_type=32)
	pps = get_all_pps(pp_tablename)
	vf_chunk = get_geocoded_without_assigned_pp(vf_tablename,fkey_name='closest_pp',other_null_col='pp_dist')
	vf_coords = vf_chunk[['latitude','longitude']].values.astype(np.float) # list(zip(vf_chunk[['latitude','longitude']].values))
	pp_coords = pps[['latitude','longitude']].values.astype(np.float) #list(zip(pps[['latitude','longitude']].values))
	print("Getting closest pps...")
	closest = pd.DataFrame([pps.iloc[cdist([point], pp_coords).argmin()] for point in vf_coords])
	print("Got closest pps...")
	vf_chunk['closest_pp'] = closest['pp_identifier'].values
	vf_chunk['pp_lat'] = closest['latitude'].values #temp
	vf_chunk['pp_lon'] = closest['longitude'].values #temp
	vf_chunk['pp_dist'] = vf_chunk.apply(lambda row: measure_distance(np.float(row['longitude']), np.float(row['latitude']), np.float(row['pp_lon']), np.float(row['pp_lat'])),axis=1)
	update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=['closest_pp','pp_dist'])
	# update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=['pp_dist'])




# from db_utilities import drop_table
# drop_table('tmp_df')

import time


if __name__=="__main__":
	# copy_table('georgia_voters','voterstesting_georgia')
	# tablename = 'georgia_voters'
	vf_tablename = "voterstesting_georgia"
	pp_tablename = 'georgia_pps'
	start = time.time()
	geocode_sample(vf_tablename,fraction=.00001,report=False)
	end = time.time()
	print(f"geocoding took {end-start}s")
	start = time.time()
	find_closest(vf_tablename,pp_tablename)
	end = time.time()
	print(f"finding closest took {end-start}s")
	start = time.time()
	append_census(vf_tablename,nParallel=8)
	end = time.time()
	print(f"querying census took {end-start}s")
	start = time.time()
	generate_report_geocoded_assigned(vf_tablename,pp_tablename=pp_tablename,jointuple=('closest_pp','pp_identifier'))
	end = time.time()
	print(f"writing report took {end-start}s")
	start = time.time()
print("\a")
