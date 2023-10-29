import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
from multiprocessing import  Pool
# from pathos.multiprocessing import ProcessingPool as Pool
# import numpy as np
# from datetime import datetime
from config import  georgia_special2021_dir as georgia_vf_dir, georgia_special2021_fnames as georgia_vf_filenames, nationbuilder_vh_filename, nationbuilder_vf_filename, nationbuilder_vfvh_filenames, nationbuilder_vh_dir
# from shapefile_utilities import get_interested_metro_properties_by_namelsad, get_properties_only
# from geojson_utilities import create_point, list_of_points_to_featureCollection
# from geocode_utilities import geocode_address, bulk_geocode_mapquest
from grid_utilities import measure_distance
from csv_utilities import get_headers, write_csv, get_max_characters_in_cols, get_pandas_in_chunks, get_pandas_chunks_basic,get_number_rows
from db_utilities import generate_report, create_table, append_df_to_table, update_records,get_number_records, sample_where, select_by_multiple, count_where,generate_report_query, where_multiple, add_null_column, add_fkey_column,select_all, drop_column,select_join,copy_table,update_records_fast, table_colnames,add_null_columns,add_null_columns_fast,modify_table, modify_table_verbose,select_where,select_join_3,select_join_n,add_rows_from_table_to_other_table_where_not_existing,add_pkey_constraint,execute_raw_sql, rename_table,dump_report_sql, table_exists, get_engine, create_index, get_distinct_values,query_chunks,generate_report_query_pg


from geocode_utilities import geocode_address,bulk_geocode_census
from census_utilities import get_census_data,demographic_data_tables_lookup,get_census_data_safe


### SETTING UP DB START
def get_all_headers(fnames,sep=","):
	return list(dict.fromkeys([header for fname in fnames for header in get_headers(fname,sep=sep)]).keys())

def get_max_characters_in_cols_multiple_files(filenames,sep=","):
	max_chars_all = {}
	for filename in filenames:
		max_chars = get_max_characters_in_cols(filename,sep=sep)
		max_chars_all = {k : int(1.25*max(max_chars_all.get(k,0), max_chars.get(k,0))) for k in {**max_chars_all, **max_chars} }
	return max_chars_all

def get_header_col_sizes(filenames,sep=",",colmin=10):
	return { k : max(v,colmin) for k,v in get_max_characters_in_cols_multiple_files(filenames,sep=sep).items()}

# primary_key 'SOS_VOTERID' for ohio
# for georgia, use 'REGISTRATION_NUMBER'
def create_db_with_appropriate_col_sizes(tablename,filenames,primary_key=None,sep=",",textcols=[]):
	headers = get_all_headers(filenames,sep=sep) # has desired order
	print(f"{headers=}")
	header_col_sizes = get_header_col_sizes(filenames,sep=sep,colmin=10)
	header_col_sizes = {k : header_col_sizes[k] for k in headers} # reorder to default order
	header_col_sizes['latitude'] = 30
	header_col_sizes['longitude'] = 30
	create_table(tablename,header_col_sizes,primary_key=primary_key)
	for col in textcols:
		execute_raw_sql(f'ALTER TABLE {tablename} ALTER COLUMN "{col}" TYPE text;')
	# execute_raw_sql(f'ALTER TABLE {tablename} ALTER COLUMN "MAIL_HOUSE_NBR" TYPE text;')
	# execute_raw_sql(f'ALTER TABLE {tablename} ALTER COLUMN "MAIL_ADDRESS_2" TYPE text;')

def drop_non_ingest_columns(vf_tablename):
	columns = table_colnames(vf_tablename)
	from db_utilities import drop_column
	for col in columns:
		if col not in georgia_header_col_sizes:
			drop_column(vf_tablename,col)

def ingest_csvs(tablename, filenames,chunksize=200000,sep=",",skip_blocks=0,quotenone=False):
	# create_db_with_appropriate_col_sizes(tablename='voters')
	import math
	import time
	import datetime
	nFiles = len(filenames)
	# i, filename,chunksize,sep,skip_blocks,quotenone = 0, filenames[0] ,200000 ,"," ,0 ,False
	for i, filename in enumerate(filenames):
		start_time = time.time()
		nchunks =math.ceil((get_number_rows(filename)-1) / chunksize)
		# j, chunk_df = 0, next(pd.read_csv(filename,chunksize=chunksize,sep=sep,quoting=1))
		for j, chunk_df in enumerate(get_pandas_chunks_basic(filename,chunksize,sep=sep)):
			append_df_to_table(tablename,chunk_df)
			print(f"File {i+1}/{nFiles} CHUNK {j+1}/{nchunks} -- {str(datetime.timedelta(seconds = (delt := (time.time()-start_time))))} [{delt}s]")

### SETTING UP DB END



def get_number_rows_in_vfs(filenames):
	return sum([get_number_rows(filename) for filename in filenames])

def select_by_has_latlon(tablename,want_geocoded=True,chunksize=200000,onlyquery=False):
	if want_geocoded:
		return select_by_multiple(tablename,nonempty=['latitude','longitude'],chunksize=chunksize,onlyquery=onlyquery)
	else:
		return select_by_multiple(tablename,empty=['latitude','longitude'],chunksize=chunksize,onlyquery=onlyquery)

def get_all_pps(pp_tablename):
	df = select_all(pp_tablename,chunksize=None)
	df = df.loc[~df['latitude'].isna() & ~df['longitude'].isna()]
	return df


### WRITING REPORTS START
def report_outfilename(tablename,folder=nationbuilder_vh_dir,base="all",extra=""):
	return f'{folder}/{tablename}_{base}_rows{("_"+ extra) if extra else ""}.csv'




def generate_full_report_multiple(vf_tablename,pp_tablename,acs_tablename='acs5_2019',sql_report=True,csv_report=True,dotchar="$",indices=[]):
	placeTypes, pp_fkeys, distnames = get_all_placeTypes(placeTypes=('pollingLocation',))
	filename=report_outfilename(tablename=vf_tablename,base="full_report",extra="")
	joinTuples = [(acs_tablename,'tract_geoid','GEO_ID')] + [(pp_tablename, pp_fkey,'pp_identifier') for pp_fkey in pp_fkeys]
	where1 = where_multiple(nonnullcols=['latitude','longitude','tract_geoid']+pp_fkeys + distnames,tablename=vf_tablename)
	query = select_join_n(vf_tablename,joinTuples,where1=where1,dotchar=dotchar,onlyquery=True)
	# print(query)
	generate_report_query_pg(filename,query,saveCSV=csv_report,save_table=sql_report,indices=indices)
	# if csv_report:
	# 	generate_report_query(filename,query)
	# if sql_report:
	# 	sql_report_tablename = filename.rsplit("/",1)[-1].rsplit(".",1)[0]
	# 	if sql_report_tablename != vf_tablename:
	# 		# overwrite = (input(f"\aAre you sure you wish to overwrite {sql_report_tablename}? y/n\a").lower() == "y") if table_exists(sql_report_tablename) else True
	# 		overwrite=True
	# 		if overwrite:
	# 			print(f"GENERATING SQL REPORT TABLE \"{sql_report_tablename}\"")
	# 			dump_report_sql(sql_report_tablename,query,chunksize=20000,indices=indices)



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
	new_df['id'] = sanitize_col(df['state_file_id']).values
	new_df['street'] = sanitize_col(df['address1__registered_address']).values
	new_df['city'] = sanitize_col(df['city__registered_address']).values
	new_df['zip'] = sanitize_col(df['zip__registered_address']).values
	new_df['state'] = sanitize_col(df['state__registered_address']).values
	new_df.set_index('id',inplace=True)
	geocoded = bulk_geocode_census(new_df)
	try:
		geocoded.set_index('id',inplace=True)
		# geocoded.rename(columns={'id': 'REGISTRATION_NUMBER'},inplace=True)
		for feature in ['latitude','longitude','census_state','census_county','census_tract','census_block','geocode_source']:
			d = geocoded[feature].to_dict()
			df[feature] = df['state_file_id'].map(d)
		return df
	except:
		return pd.DataFrame()






def geocode_sample(tablename,fraction=.0001,report=False,chunksize=1000,n_cores=1):
	reference_cols= [
		'state_file_id',
		'address1__registered_address',
		# 'address2__registered_address',
		'city__registered_address',
		# 'county__registered_address',
		'zip__registered_address',
		'state__registered_address',
		# 'address1__mailing_address',
		# 'address2__mailing_address',
		# 'address3__mailing_address',
		# 'city__mailing_address',
		# 'zip__mailing_address',
		# 'state__mailing_address',
		# 'country_code__mailing_address',
	]
	geocoding_cols = reference_cols + [ "latitude","longitude",'census_state','census_county','census_tract','census_block']
	add_null_columns_fast(tablename=tablename,col_names=['geocode_source'] + geocoding_cols,column_type=50,pkeys=['state_file_id'],fkeys=[('closest_pp','pp_identifier','georgia_pps')])
	where_uncoded = where_multiple(emptycols=['latitude','longitude']) #just get all counties for Georgia
	n_total = get_number_records(tablename)
	n_uncoded=count_where(tablename,where_uncoded)
	# n_coded = count_nonempty_rows(tablename,['latitude','longitude'])
	n_to_geocode = int(fraction*n_total)
	fraction_uncoded = n_to_geocode/n_uncoded
	# modify_table_verbose(tablename,geocode_all_google,fraction=fraction_uncoded,where=where_uncoded,max_applications=n_to_geocode,cols=geocoding_cols)
	modify_table_verbose(tablename,geocode_all_census,'state_file_id',fraction=fraction_uncoded,where=where_uncoded,max_applications=n_to_geocode,cols=geocoding_cols,chunksize=chunksize,n_cores=n_cores)
	if report:
		generate_report_geocoded(tablename)

def ensure_census_cols(vf_tablename,pkey='REGISTRATION_NUMBER'):
	census_headers = list(demographic_data_tables_lookup.values()) + ['queried_census','GEO_ID']
	cols = table_colnames(vf_tablename)
	add_null_columns_fast(tablename=vf_tablename,col_names=[header for header in census_headers if header not in cols],column_type=50,pkeys=[pkey],fkeys=[('closest_pp','pp_identifier','georgia_pps')])
	return census_headers


def append_census_df(vf_df):
	census_data = vf_df.apply(lambda row: get_census_data_safe(row['latitude'],row['longitude']), axis=1)
	census_df = pd.DataFrame(list(census_data.values))
	vf_df.update(census_df)
	vf_df['queried_census'] = 'YES'
	return vf_df
	# for i, row in vf_df.iterrows():
	#   census_data = get_census_data_safe(row['latitude'],row['longitude']) # returns {} if error or no data
	#   for k,v in census_data.items():
	#       vf_df.at[i,k] = v

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
#   cols = table_colnames(vf_tablename)
#   if 'closest_pp' not in cols:
#       add_fkey_column(vf_tablename,'closest_pp',"georgia_pps",'pp_identifier')
#   if 'pp_dist' not in cols:
#       add_null_column(tablename=vf_tablename,col_name="pp_dist",column_type=32)
#   pps = get_all_pps(pp_tablename)
#   vf_chunk = select_by_multiple(vf_tablename,nonempty=['latitude','longitude'],null=['closest_pp','pp_dist'],chunksize=None,onlyquery=False)
#   vf_coords = vf_chunk[['latitude','longitude']].values.astype(float) # list(zip(vf_chunk[['latitude','longitude']].values))
#   pp_coords = pps[['latitude','longitude']].values.astype(float) #list(zip(pps[['latitude','longitude']].values))
#   print("Getting closest pps...")
#   closest = pd.DataFrame([pps.iloc[cdist([point], pp_coords).argmin()] for point in vf_coords])
#   print("Got closest pps...")
#   vf_chunk['closest_pp'] = closest['pp_identifier'].values
#   vf_chunk['pp_lat'] = closest['latitude'].values #temp
#   vf_chunk['pp_lon'] = closest['longitude'].values #temp
#   vf_chunk['pp_dist'] = vf_chunk.apply(lambda row: measure_distance(float(row['longitude']), float(row['latitude']), float(row['pp_lon']), float(row['pp_lat'])),axis=1)
#   update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=['closest_pp','pp_dist'])
#   # update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=['pp_dist'])

def get_all_placeTypes(placeTypes=('pollingLocation','dropOffLocation','earlyVoteSite')):
	pp_fkeys = [f'closest_{placetype}' for placetype in placeTypes]
	distnames = [f'dist_to_{placetype}' for placetype in placeTypes]
	return placeTypes, pp_fkeys, distnames

def find_closest_multiple(vf_tablename,pp_tablename,pkey,ensure_fkeys=False):
	print("Getting closest pps...")
	# cols = table_colnames(vf_tablename)
	placeTypes, pp_fkeys, distnames = get_all_placeTypes()# ('pollingLocation','dropOffLocation','earlyVoteSite')
	if ensure_fkeys:
		for fkey in pp_fkeys:
			add_fkey_column(vf_tablename,fkey,pp_tablename,'pp_identifier')
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
			# print(f"NUMBER OF ROWS: {len(pps)}")
			if len(pps)==0:
				continue
			pp_coords = pps[['latitude','longitude']].values.astype(float) #list(zip(pps[['latitude','longitude']].values))
			closest = pd.DataFrame([pps.iloc[cdist([point], pp_coords).argmin()] for point in vf_coords])
			vf_chunk[fkey] = closest['pp_identifier'].values
			vf_chunk['pp_lat'] = closest['latitude'].values #temp
			vf_chunk['pp_lon'] = closest['longitude'].values #temp
			vf_chunk[distname] = vf_chunk.apply(lambda row: measure_distance(float(row['longitude']), float(row['latitude']), float(row['pp_lon']), float(row['pp_lat'])),axis=1) #measure_distance is by default in km.
		print("Got closest pps...")
		for pp_fkey in pp_fkeys:
			# https://stackoverflow.com/questions/15891038/change-column-type-in-pandas
			vf_chunk[[pp_fkey]] = vf_chunk[[pp_fkey]].apply(lambda x: pd.to_numeric(x,downcast='integer')) #change to int type
		update_records_fast(vf_tablename,vf_chunk,pkey=pkey,columns=pp_fkeys + distnames)
	# update_records_fast(vf_tablename,vf_chunk,pkey='REGISTRATION_NUMBER',columns=['pp_dist'])


def find_tract(vf_tablename,acs_tablename,pkey):
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
		update_records_fast(vf_tablename,joined,pkey=pkey,columns=['tract_geoid'])


# import db_utilities
# db_utilities.drop_table(vh_tablename)
# db_utilities.drop_table(vf_tablename)
# import time

def create_and_ingest(tablename,csv_filenames,primary_key=None,chunksize=100000,textcols=[]):
	print(f"Ingesting to {tablename} from {', '.join(csv_filenames)}")
	create_db_with_appropriate_col_sizes(tablename,csv_filenames,primary_key=primary_key,textcols=textcols)
	ingest_csvs(tablename,csv_filenames,chunksize=chunksize)




def transform_date_to_voted_colname(date,prepend="",append=""):
	return f'{prepend}voted_{date.replace("-","_")}{append}'


def add_election_indicator_cols(vf_tablename,vh_tablename):
	distinct_election_dates = get_distinct_values(vh_tablename,'election_at')
	col_names = [transform_date_to_voted_colname(election_date) for election_date in  distinct_election_dates]
	# for col_name in col_names:
		# drop_column(vf_tablename,col_name)
	# add_null_columns_fast(vf_tablename,col_names=col_names,column_type=bool,pkeys=['state_file_id'],fkeys=[])
	add_null_columns_fast(vf_tablename,col_names=col_names,column_type=15,pkeys=['state_file_id'],fkeys=[])

# https://stackoverflow.com/questions/6256610/updating-table-rows-in-postgres-using-subquery
# https://stackoverflow.com/questions/25284986/select-column-as-true-false-if-id-is-exists-in-another-table
def populate_election_indicator_cols(vf_tablename,vh_tablename):
	distinct_election_dates = get_distinct_values(vh_tablename,'election_at')
	distinct_vote_methods = get_distinct_values(vh_tablename,'vote_method')
	print("Populating vote indicators")
	for date in distinct_election_dates:
		for vote_method in distinct_vote_methods:
			voted_colname = transform_date_to_voted_colname(date)
			for i, vh_chunk in enumerate(select_by_multiple(vh_tablename,equals={'election_at' : date,'vote_method': vote_method},cols=['state_file_id','election_at','vote_method'],chunksize=200000)):
				print(f"Chunk {i} for '{vote_method}' in '{date}'")
				vh_chunk.rename(columns = {"vote_method": voted_colname},inplace=True)
				update_records_fast(vf_tablename,vh_chunk,'state_file_id',columns=[voted_colname])

		# f'''SELECT "state_file_id", "election_at", "vote_method" from {vh_tablename}
		#   WHERE "election_at" == '{date}' AND "vote_method" == '{vote_method}'
		# '''

# https://stackoverflow.com/questions/14618703/update-query-using-subquery-in-sql-server
def populate_election_indicator_cols_indb(vf_tablename,vh_tablename):
	distinct_election_dates = get_distinct_values(vh_tablename,'election_at')
	distinct_vote_methods = get_distinct_values(vh_tablename,'vote_method')
	print("Populating vote indicators")
	for date in distinct_election_dates:
		for vote_method in distinct_vote_methods:
			voted_colname = transform_date_to_voted_colname(date)
			print(f"Updating '{vote_method}' in '{date}'")
			execute_raw_sql(f'''
							UPDATE {vf_tablename}
							SET "{voted_colname}" = '{vote_method}'
							FROM
								(
									SELECT "state_file_id", "election_at", "vote_method" from {vh_tablename}
									WHERE "election_at"='{date}' AND "vote_method"='{vote_method}'
								) vh
							WHERE
								{vf_tablename}."state_file_id" = vh."state_file_id"
							''')

			# for i, vh_chunk in enumerate(select_by_multiple(vh_tablename,equals={'election_at' : date,'vote_method': vote_method},cols=['state_file_id','election_at','vote_method'],chunksize=200000)):
				# print(f"Chunk {i} for '{vote_method}' in '{date}'")
				# vh_chunk.rename(columns = {"vote_method": voted_colname},inplace=True)
				# update_records_fast(vf_tablename,vh_chunk,'state_file_id',columns=[voted_colname])

def test_indicator_columns(vf_tablename,limit=1000):
	distinct_election_dates = get_distinct_values(vh_tablename,'election_at')
	col_names = [transform_date_to_voted_colname(election_date) for election_date in  distinct_election_dates]
	col_names_testing = [transform_date_to_voted_colname(election_date,prepend="testing_") for election_date in  distinct_election_dates]
	col_pairs = list(zip(sorted(col_names),sorted(col_names_testing)))
	cols_string = ",\n".join(', '.join(pair) for pair in col_pairs)
	df = query_chunks(f"SELECT {cols_string} FROM {vf_tablename} limit {limit};", chunksize=None)
	for col1,col2 in col_pairs:
		assert  all(df[col1].values == df[col2].values)

def create_and_ingest_nationbuilder():
	vh_tablename = "georgia_vh_nationbuilder"
	vf_tablename = "georgia_vf_nationbuilder"
	joined_tablename = "georgia_vfvh_nationbuilder"
	pp_tablename = 'georgia_pps'
	acs_tablename = 'acs5_2019'# rename_table('georgia_vf_2021_testing2','georgia_vf_2021_ingest')
	create_and_ingest(vf_tablename,[nationbuilder_vf_filename],primary_key='state_file_id',textcols=['address2__mailing_address'])
	create_and_ingest(vh_tablename,[nationbuilder_vh_filename])
	add_fkey_column(vh_tablename,'state_file_id',vf_tablename,'state_file_id')
	create_and_ingest(joined_tablename,nationbuilder_vfvh_filenames)
	create_index(vh_tablename,'state_file_id')
	create_index(vh_tablename,'election_at')
	create_index(vh_tablename,'vote_method')
	create_index(joined_tablename,'state_file_id')
	add_election_indicator_cols(vf_tablename,vh_tablename)
	populate_election_indicator_cols(vf_tablename,vh_tablename)
	populate_election_indicator_cols_indb(vf_tablename,vh_tablename)

def calc_true_dist():
	pass

if __name__=="__main__":
	vh_tablename = "georgia_vh_nationbuilder"
	vf_tablename = "georgia_vf_nationbuilder"
	joined_tablename = "georgia_vfvh_nationbuilder"
	pp_tablename = 'georgia_pps_2016'
	acs_tablename = 'acs5_2019'
	# create_and_ingest_nationbuilder()
	# df = query_chunks(f"SELECT * FROM {vf_tablename} limit 1000;", chunksize=None)
	# distinct_election_dates = get_distinct_values(vh_tablename,'election_at')
	# col_names = [transform_date_to_voted_colname(election_date) for election_date in  distinct_election_dates]
	# print(col_names)

	geocode_sample(vf_tablename,fraction=.015,report=False,n_cores=8,chunksize=4000)
	find_closest_multiple(vf_tablename,pp_tablename,'state_file_id',ensure_fkeys=False)
	find_tract(vf_tablename,acs_tablename,'state_file_id')

	calc_true_dist(vf_tablename)
	generate_full_report_multiple(vf_tablename,pp_tablename,sql_report=True,csv_report=True,dotchar="$",indices=['state_file_id'])
print("\a")


# from db_utilities import drop_column
# for column in ['dist_to_pollingLocation','dist_to_dropOffLocation','dist_to_earlyVoteSite','closest_pollingLocation','closest_dropOffLocation','closest_earlyVoteSite','tract_geoid']:
#   drop_column(vf_tablename,column)
