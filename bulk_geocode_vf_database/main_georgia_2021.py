import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
from multiprocessing import  Pool
# from pathos.multiprocessing import ProcessingPool as Pool
# import numpy as np
# from datetime import datetime
from config import  georgia_special2021_dir as georgia_vf_dir, georgia_special2021_fnames as georgia_vf_filenames
# from shapefile_utilities import get_interested_metro_properties_by_namelsad, get_properties_only
# from geojson_utilities import create_point, list_of_points_to_featureCollection
# from geocode_utilities import geocode_address, bulk_geocode_mapquest
from grid_utilities import measure_distance
from csv_utilities import get_headers, write_csv, get_max_characters_in_cols, get_pandas_in_chunks, get_number_rows
from db_utilities import generate_report, create_table, append_df_to_table, update_records,get_number_records, sample_where, select_by_multiple, count_where,generate_report_query, where_multiple, add_null_column, add_fkey_column,select_all, drop_column,select_join,copy_table,update_records_fast, table_colnames,add_null_columns,add_null_columns_fast,modify_table, modify_table_verbose,select_where,select_join_3,select_join_n,add_rows_from_table_to_other_table_where_not_existing,add_pkey_constraint,execute_raw_sql, rename_table,dump_report_sql, table_exists


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
	# return {'COUNTY_CODE': 11, 'REGISTRATION_NUMBER': 19, 'VOTER_STATUS': 12, 'LAST_NAME': 30, 'FIRST_NAME': 30, 'MIDDLE_MAIDEN_NAME': 30, 'NAME_SUFFIX': 11, 'NAME_TITLE': 10, 'RESIDENCE_HOUSE_NUMBER': 22, 'RESIDENCE_STREET_NAME': 51, 'RESIDENCE_STREET_SUFFIX': 23, 'RESIDENCE_APT_UNIT_NBR': 23, 'RESIDENCE_CITY': 19, 'RESIDENCE_ZIPCODE': 17, 'BIRTHDATE': 9, 'REGISTRATION_DATE': 17, 'RACE': 4, 'GENDER': 6, 'LAND_DISTRICT': 13, 'LAND_LOT': 8, 'STATUS_REASON': 13, 'COUNTY_PRECINCT_ID': 18, 'CITY_PRECINCT_ID': 16, 'CONGRESSIONAL_DISTRICT': 22, 'SENATE_DISTRICT': 15, 'HOUSE_DISTRICT': 14, 'JUDICIAL_DISTRICT': 17, 'COMMISSION_DISTRICT': 19, 'SCHOOL_DISTRICT': 15, 'COUNTY_DISTRICTA_NAME': 21, 'COUNTY_DISTRICTA_VALUE': 22, 'COUNTY_DISTRICTB_NAME': 21, 'COUNTY_DISTRICTB_VALUE': 22, 'MUNICIPAL_NAME': 19, 'MUNICIPAL_CODE': 14, 'WARD_CITY_COUNCIL_NAME': 22, 'WARD_CITY_COUNCIL_CODE': 22, 'CITY_SCHOOL_DISTRICT_NAME': 25, 'CITY_SCHOOL_DISTRICT_VALUE': 26, 'CITY_DISTA_NAME': 15, 'CITY_DISTA_VALUE': 16, 'CITY_DISTB_NAME': 15, 'CITY_DISTB_VALUE': 16, 'CITY_DISTC_NAME': 15, 'CITY_DISTC_VALUE': 16, 'CITY_DISTD_NAME': 15, 'CITY_DISTD_VALUE': 16, 'DATE_LAST_VOTED': 15, 'PARTY_LAST_VOTED': 16, 'DATE_ADDED': 10, 'DATE_CHANGED': 12, 'DISTRICT_COMBO': 14, 'RACE_DESC': 33, 'LAST_CONTACT_DATE': 17, 'MAIL_HOUSE_NBR': 20, 'MAIL_STREET_NAME': 87, 'MAIL_APT_UNIT_NBR': 25, 'MAIL_CITY': 50, 'MAIL_STATE': 10, 'MAIL_ZIPCODE': 12, 'MAIL_ADDRESS_2': 49, 'MAIL_ADDRESS_3': 30, 'MAIL_COUNTRY': 20}
	return {'COUNTY_CODE': 3, 'REGISTRATION_NUMBER': 8, 'VOTER_STATUS': 1, 'LAST_NAME': 23, 'FIRST_NAME': 20, 'MIDDLE_MAIDEN_NAME': 22, 'NAME_SUFFIX': 3, 'NAME_TITLE': 3, 'RESIDENCE_HOUSE_NUMBER': 16, 'RESIDENCE_STREET_NAME': 51, 'RESIDENCE_STREET_SUFFIX': 3, 'RESIDENCE_APT_UNIT_NBR': 16, 'RESIDENCE_CITY': 19, 'RESIDENCE_ZIPCODE': 9, 'BIRTHDATE': 4, 'REGISTRATION_DATE': 8, 'RACE': 2, 'GENDER': 1, 'LAND_DISTRICT': 4, 'LAND_LOT': 5, 'STATUS_REASON': 13, 'COUNTY_PRECINCT_ID': 5, 'CITY_PRECINCT_ID': 5, 'CONGRESSIONAL_DISTRICT': 5, 'SENATE_DISTRICT': 5, 'HOUSE_DISTRICT': 5, 'JUDICIAL_DISTRICT': 5, 'COMMISSION_DISTRICT': 5, 'SCHOOL_DISTRICT': 3, 'COUNTY_DISTRICTA_NAME': 5, 'COUNTY_DISTRICTA_VALUE': 3, 'COUNTY_DISTRICTB_NAME': 5, 'COUNTY_DISTRICTB_VALUE': 3, 'MUNICIPAL_NAME': 19, 'MUNICIPAL_CODE': 3, 'WARD_CITY_COUNCIL_NAME': 5, 'WARD_CITY_COUNCIL_CODE': 3, 'CITY_SCHOOL_DISTRICT_NAME': 5, 'CITY_SCHOOL_DISTRICT_VALUE': 3, 'CITY_DISTA_NAME': 3, 'CITY_DISTA_VALUE': 3, 'CITY_DISTB_NAME': 3, 'CITY_DISTB_VALUE': 3, 'CITY_DISTC_NAME': 3, 'CITY_DISTC_VALUE': 3, 'CITY_DISTD_NAME': 3, 'CITY_DISTD_VALUE': 3, 'DATE_LAST_VOTED': 8, 'PARTY_LAST_VOTED': 3, 'DATE_ADDED': 8, 'DATE_CHANGED': 8, 'DISTRICT_COMBO': 3, 'RACE_DESC': 33, 'LAST_CONTACT_DATE': 8, 'MAIL_HOUSE_NBR': 7, 'MAIL_STREET_NAME': 47, 'MAIL_APT_UNIT_NBR': 20, 'MAIL_CITY': 43, 'MAIL_STATE': 3, 'MAIL_ZIPCODE': 9, 'MAIL_ADDRESS_2': 30, 'MAIL_ADDRESS_3': 25, 'MAIL_COUNTRY': 20}
georgia_header_col_sizes = get_georgia_header_col_sizes()

# primary_key 'SOS_VOTERID' for ohio
# for georgia, use 'REGISTRATION_NUMBER'
def create_db_with_appropriate_col_sizes(tablename,primary_key='REGISTRATION_NUMBER',sep=","):
	headers = get_all_headers(georgia_vf_filenames,sep=sep) # has desired order
	# ohio_header_col_sizes = get_max_characters_in_cols_multiple_files(ohio_vf_filenames) #result dumped into config
	print(f"{headers=}")
	header_col_sizes = {k : georgia_header_col_sizes[k] for k in headers} # reorder to default order
	header_col_sizes['latitude'] = 30
	header_col_sizes['longitude'] = 30
	create_table(tablename,header_col_sizes,primary_key=primary_key)
	execute_raw_sql(f'ALTER TABLE {tablename} ALTER COLUMN "MAIL_STREET_NAME" TYPE text;')
	execute_raw_sql(f'ALTER TABLE {tablename} ALTER COLUMN "MAIL_HOUSE_NBR" TYPE text;')
	execute_raw_sql(f'ALTER TABLE {tablename} ALTER COLUMN "MAIL_ADDRESS_2" TYPE text;')

def drop_non_ingest_columns(vf_tablename):
	columns = table_colnames(vf_tablename)
	from db_utilities import drop_column
	for col in columns:
		if col not in georgia_header_col_sizes:
			drop_column(vf_tablename,col)

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
			append_df_to_table(tablename,chunk_df,if_exists='append')
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
	print(query)
	generate_report_query(filename,query)



def generate_full_report_multiple(vf_tablename,pp_tablename="georgia_pps",acs_tablename='acs5_2019',sql_report=True,csv_report=True,dotchar="$"):
	placeTypes, pp_fkeys, distnames = get_all_placeTypes(placeTypes=('pollingLocation','dropOffLocation','earlyVoteSite'))
	filename=report_outfilename(tablename=vf_tablename,base="full_report",extra="")
	joinTuples = [(acs_tablename,'tract_geoid','GEO_ID')] + [(pp_tablename, pp_fkey,'pp_identifier') for pp_fkey in pp_fkeys]
	where1 = where_multiple(nonnullcols=['latitude','longitude','tract_geoid']+pp_fkeys + distnames,tablename=vf_tablename)
	query = select_join_n(vf_tablename,joinTuples,where1=where1,dotchar=dotchar,onlyquery=True)
	print(query)
	if csv_report:
		generate_report_query(filename,query)
	if sql_report:
		sql_report_tablename = filename.rsplit("/",1)[-1].rsplit(".",1)[0]
		if sql_report_tablename != vf_tablename:
			overwrite = (input(f"\aAre you sure you wish to overwrite {sql_report_tablename}? y/n\a").lower() == "y") if table_exists(sql_report_tablename) else True
			if overwrite:
				print(f"GENERATING SQL REPORT TABLE \"{sql_report_tablename}\"")
				dump_report_sql(sql_report_tablename,query,chunksize=20000)


def generate_full_report_individual_pollingTypes(vf_tablename,pp_tablename="georgia_pps",acs_tablename='acs5_2019'):
	for placeType in ('pollingLocation','dropOffLocation','earlyVoteSite'):
		placeTypes, pp_fkeys, distnames = get_all_placeTypes(placeTypes=(placeType,))
		filename=report_outfilename(tablename=vf_tablename,base="full_report",extra=f"{placeType}_only")
		joinTuples = [(acs_tablename,'tract_geoid','GEO_ID')] + [(pp_tablename, pp_fkey,'pp_identifier') for pp_fkey in pp_fkeys]
		where1 = where_multiple(nonnullcols=['latitude','longitude','tract_geoid']+pp_fkeys + distnames,tablename=vf_tablename)
		query = select_join_n(vf_tablename,joinTuples,where1=where1,onlyquery=True) #TODO - dotted_prefix for different pp type
		print(query)
		generate_report_query(filename,query)


## CENSUS
def extract_address(row):
	# address_keys= ('RESIDENTIAL_ADDRESS1', 'RESIDENTIAL_SECONDARY_ADDR','RESIDENTIAL_CITY','RESIDENTIAL_STATE','RESIDENTIAL_ZIP','RESIDENTIAL_ZIP_PLUS4') #ohio
	address_keys1 = ("RESIDENCE_HOUSE_NUMBER", "RESIDENCE_STREET_NAME", "RESIDENCE_STREET_SUFFIX", "RESIDENCE_CITY")
	address_keys2 = ("RESIDENCE_ZIPCODE",)
	return " ".join([v for k in address_keys1 if (v:=row[k])] + ["Georgia"] + [v for k in address_keys2 if (v:=row[k])]) #join nonempty elements, avoid re-indexing


def geocode_all_google(df):
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
	return df

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
	try:
		geocoded.set_index('id',inplace=True)
		# geocoded.rename(columns={'id': 'REGISTRATION_NUMBER'},inplace=True)
		for feature in ['latitude','longitude','census_state','census_county','census_tract','census_block','geocode_source']:
			d = geocoded[feature].to_dict()
			df[feature] = df['REGISTRATION_NUMBER'].map(d)
		return df
	except:
		return pd.DataFrame()






def geocode_sample(tablename,fraction=.0001,report=False,chunksize=1000,n_cores=1):
	geocoding_cols = ["REGISTRATION_NUMBER", "RESIDENCE_HOUSE_NUMBER", "RESIDENCE_STREET_NAME", "RESIDENCE_STREET_SUFFIX", "RESIDENCE_CITY", "RESIDENCE_ZIPCODE", "latitude","longitude",'census_state','census_county','census_tract','census_block']
	add_null_columns_fast(tablename=tablename,col_names=['geocode_source'] + geocoding_cols,column_type=50,pkeys=['REGISTRATION_NUMBER'],fkeys=[('closest_pp','pp_identifier','georgia_pps')])
	where_uncoded = where_multiple(emptycols=['latitude','longitude']) #just get all counties for Georgia
	n_total = get_number_records(tablename)
	n_uncoded=count_where(tablename,where_uncoded)
	# n_coded = count_nonempty_rows(tablename,['latitude','longitude'])
	n_to_geocode = int(fraction*n_total)
	fraction_uncoded = n_to_geocode/n_uncoded
	# modify_table_verbose(tablename,geocode_all_google,fraction=fraction_uncoded,where=where_uncoded,max_applications=n_to_geocode,cols=geocoding_cols)
	modify_table_verbose(tablename,geocode_all_census,fraction=fraction_uncoded,where=where_uncoded,max_applications=n_to_geocode,cols=geocoding_cols,chunksize=chunksize,n_cores=n_cores)
	if report:
		generate_report_geocoded(tablename)

def ensure_census_cols(vf_tablename):
	census_headers = list(demographic_data_tables_lookup.values()) + ['queried_census','GEO_ID']
	cols = table_colnames(vf_tablename)
	add_null_columns_fast(tablename=vf_tablename,col_names=[header for header in census_headers if header not in cols],column_type=50,pkeys=['REGISTRATION_NUMBER'],fkeys=[('closest_pp','pp_identifier','georgia_pps')])
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
# from multiprocessing import  Pool
def parallelize_dataframe(df, func, n_cores=4):
	if n_cores==1:
		return func(df)
	df_split = np.array_split(df, n_cores)
	pool = Pool(n_cores)
	df = pd.concat(pool.map(func, df_split))
	pool.close()
	pool.join()
	# pool.terminate()
	# pool.restart()
	return df

def append_census(vf_tablename,chunksize=100,nParallel=1):
	census_headers = ensure_census_cols(vf_tablename)
	where = where_multiple(nonemptycols=['latitude','longitude'],allnullcols=['queried_census'])
	n_queries = count_where(vf_tablename,where)
	print(f"Making {n_queries} CENSUS QUERIES TOTAL!")
	print(where)
	vf_chunks = select_where(vf_tablename,where=where,chunksize=chunksize,onlyquery=False,cols=None)
	for vf_chunk in vf_chunks:
		# vf_chunk = parallelize_dataframe(vf_chunk,append_census_df,n_cores=nParallel)
		vf_chunk = append_census_df(vf_chunk)
		print(f"Got census data for {vf_chunk.shape[0]} individuals")
		update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=census_headers)
	return vf_chunk



# def find_closest(vf_tablename,pp_tablename):
# 	cols = table_colnames(vf_tablename)
# 	if 'closest_pp' not in cols:
# 		add_fkey_column(vf_tablename,'closest_pp',"georgia_pps",'pp_identifier')
# 	if 'pp_dist' not in cols:
# 		add_null_column(tablename=vf_tablename,col_name="pp_dist",column_type=32)
# 	pps = get_all_pps(pp_tablename)
# 	vf_chunk = select_by_multiple(vf_tablename,nonempty=['latitude','longitude'],null=['closest_pp','pp_dist'],chunksize=None,onlyquery=False)
# 	vf_coords = vf_chunk[['latitude','longitude']].values.astype(float) # list(zip(vf_chunk[['latitude','longitude']].values))
# 	pp_coords = pps[['latitude','longitude']].values.astype(float) #list(zip(pps[['latitude','longitude']].values))
# 	print("Getting closest pps...")
# 	closest = pd.DataFrame([pps.iloc[cdist([point], pp_coords).argmin()] for point in vf_coords])
# 	print("Got closest pps...")
# 	vf_chunk['closest_pp'] = closest['pp_identifier'].values
# 	vf_chunk['pp_lat'] = closest['latitude'].values #temp
# 	vf_chunk['pp_lon'] = closest['longitude'].values #temp
# 	vf_chunk['pp_dist'] = vf_chunk.apply(lambda row: measure_distance(float(row['longitude']), float(row['latitude']), float(row['pp_lon']), float(row['pp_lat'])),axis=1)
# 	update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=['closest_pp','pp_dist'])
# 	# update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=['pp_dist'])

def get_all_placeTypes(placeTypes=('pollingLocation','dropOffLocation','earlyVoteSite')):
	pp_fkeys = [f'closest_{placetype}' for placetype in placeTypes]
	distnames = [f'dist_to_{placetype}' for placetype in placeTypes]
	return placeTypes, pp_fkeys, distnames

def find_closest_multiple(vf_tablename,pp_tablename,ensure_fkeys=False):
	print("Getting closest pps...")
	# cols = table_colnames(vf_tablename)
	placeTypes, pp_fkeys, distnames = get_all_placeTypes()# ('pollingLocation','dropOffLocation','earlyVoteSite')
	if ensure_fkeys:
		for fkey in pp_fkeys:
			add_fkey_column(vf_tablename,fkey,"georgia_pps",'pp_identifier')
	for distname in distnames:
		add_null_column(tablename=vf_tablename,col_name=distname,column_type=32)
	all_pps = get_all_pps(pp_tablename)
	vf_chunks = select_by_multiple(vf_tablename,nonnull=['latitude','longitude'],null=pp_fkeys + distnames,chunksize=10000,onlyquery=False)
	for vf_chunk in vf_chunks:
		if vf_chunk.shape[0] == 0:
			return
		vf_coords = vf_chunk[['latitude','longitude']].values.astype(float) # list(zip(vf_chunk[['latitude','longitude']].values))
		# placeType,fkey, distname = next(zip(placeTypes,pp_fkeys,distnames))
		for placeType,fkey, distname in zip(placeTypes,pp_fkeys,distnames):
			pps = all_pps.loc[all_pps['placeType']==placeType]
			pp_coords = pps[['latitude','longitude']].values.astype(float) #list(zip(pps[['latitude','longitude']].values))
			closest = pd.DataFrame([pps.iloc[cdist([point], pp_coords).argmin()] for point in vf_coords])
			vf_chunk[fkey] = closest['pp_identifier'].values
			vf_chunk['pp_lat'] = closest['latitude'].values #temp
			vf_chunk['pp_lon'] = closest['longitude'].values #temp
			vf_chunk[distname] = vf_chunk.apply(lambda row: measure_distance(float(row['longitude']), float(row['latitude']), float(row['pp_lon']), float(row['pp_lat'])),axis=1) #measure_distance is by default in km.
		print("Got closest pps...")
		update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=pp_fkeys + distnames)
	# update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=['pp_dist'])


def find_tract(vf_tablename,acs_tablename):
	cols = table_colnames(vf_tablename)
	if 'tract_geoid' not in cols:
		add_fkey_column(vf_tablename,'tract_geoid',acs_tablename,'GEO_ID',foreign_col_type=50) # varchar50
	tract_data = select_all(acs_tablename,chunksize=None)
	# tract_data = tract_data.rename(columns = {"GEO_ID": 'tract_geoid'})
	tract_data = tract_data.rename(columns={"tract": 'census_tract','county': 'census_county'})
	vf_chunks = select_by_multiple(vf_tablename,nonempty=['latitude','longitude'],null=['tract_geoid'],chunksize=10000,onlyquery=False)
	for vf_chunk in vf_chunks:
		vf_chunk['census_tract'] = pd.to_numeric(vf_chunk['census_tract'])
		vf_chunk['census_county'] = pd.to_numeric(vf_chunk['census_county'])
		# joined = vf_chunk.merge(tract_data,how='left',on=['census_county','census_tract'])# left includes rows with no lat/lon - no point updating those
		joined = vf_chunk.merge(tract_data,how='inner',on=['census_county','census_tract'])
		joined['tract_geoid'] = joined['GEO_ID']
		update_records_fast(vf_tablename,joined,pkey='REGISTRATION_NUMBER',columns=['tract_geoid'])


# import db_utilities
# db_utilities.drop_table('tmp_df')
# import time



# WANT TO COPY ALL ROWS FROM georgia_vf_2021_testing2 into georgia_vf_2021 where row doesn't exist in georgia_vf_2021
if __name__=="__main__":
	vf_tablename = "georgia_vf_2021"
	pp_tablename = 'georgia_pps'
	acs_tablename = 'acs5_2019'
	# rename_table('georgia_vf_2021_testing2','georgia_vf_2021_ingest')
	# create_db_with_appropriate_col_sizes(vf_tablename,primary_key='REGISTRATION_NUMBER',sep="|")
	# ingest_csvs(chunksize=10000,tablename=vf_tablename,sep="|",skip_blocks=0,quotenone=False)
	geocode_sample(vf_tablename,fraction=.01,report=False,n_cores=8,chunksize=4000)
	find_closest_multiple(vf_tablename,pp_tablename)
	find_tract(vf_tablename,acs_tablename)
	generate_full_report_multiple(vf_tablename,sql_report=False,csv_report=False,dotchar="$")
	# generate_full_report_individual_pollingTypes(vf_tablename)


print("\a")
