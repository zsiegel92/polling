import math
import pandas as pd
import numpy as np
import os
# from datetime import datetime
from config import ohio_vf_dir, ohio_vf_filenames, ohio_fake_vf_filenames, ohio_fake_geocoded_vf_dir
# from shapefile_utilities import get_interested_metro_properties_by_namelsad, get_properties_only
# from geojson_utilities import create_point, list_of_points_to_featureCollection
# from geocode_utilities import geocode_address, bulk_geocode_mapquest
from csv_utilities import get_headers, write_csv, get_max_characters_in_cols, get_pandas_in_chunks, get_number_rows


from db_utilities import get_engine, generate_report, create_table, append_df_to_table, generate_report, update_records,select_all,delete_table,get_number_records, sample_where,drop_table, select_where, count_empty_rows, count_nonempty_rows, select_by_empty_columns, get_table, table_colnames, where_multiple,generate_report_query, count_where

from metro_county_delineation import  get_ohio_county_numbers_for_counties_with_pps
from geocode_utilities import geocode_address


def get_all_headers(fnames):
	return list(dict.fromkeys([header for fname in fnames for header in get_headers(fname)]).keys())

def get_max_characters_in_cols_multiple_files(filenames):
	max_chars_all = {}
	for filename in filenames:
		max_chars = get_max_characters_in_cols(filename)
		max_chars_all = {k : max(max_chars_all.get(k,0), max_chars.get(k,0)) for k in {**max_chars_all, **max_chars} }
	return max_chars_all

def create_db_with_appropriate_col_sizes(tablename='voters'):
	headers = get_all_headers(ohio_vf_filenames) # has desired order
	# header_col_sizes = get_max_characters_in_cols_multiple_files(ohio_vf_filenames)
	header_col_sizes = {'MAILING_STATE': 13, 'PRIMARY-09/11/2007': 18, 'RESIDENTIAL_SECONDARY_ADDR': 26, 'MUNICIPAL_COURT_DISTRICT': 24, 'PRIMARY-03/17/2020': 18, 'GENERAL-11/07/2017': 18, 'PRIMARY-03/07/2000': 18, 'RESIDENTIAL_POSTALCODE': 22, 'CITY_SCHOOL_DISTRICT': 33, 'GENERAL-11/02/2010': 18, 'GENERAL-11/07/2006': 18, 'SPECIAL-05/06/2003': 18, 'GENERAL-11/02/2004': 18, 'PRIMARY-09/15/2009': 18, 'PRIMARY-03/04/2008': 18, 'RESIDENTIAL_ZIP_PLUS4': 21, 'GENERAL-11/08/2011': 18, 'SPECIAL-05/08/2001': 18, 'SOS_VOTERID': 12, 'PRIMARY-09/13/2005': 18, 'PRIMARY-09/29/2009': 18, 'GENERAL-11/06/2007': 18, 'CONGRESSIONAL_DISTRICT': 22, 'COURT_OF_APPEALS': 16, 'DATE_OF_BIRTH': 13, 'GENERAL-11/04/2008': 18, 'GENERAL-11/05/2013': 18, 'PRIMARY-05/04/2010': 18, 'LIBRARY': 7, 'PRIMARY-05/02/2006': 18, 'MAILING_ZIP': 11, 'PRIMARY-09/07/2010': 18, 'GENERAL-11/04/2014': 18, 'GENERAL-11/08/2005': 18, 'PRIMARY-10/14/2008': 18, 'PRIMARY-05/07/2002': 18, 'RESIDENTIAL_ZIP': 15, 'EDU_SERVICE_CENTER_DISTRICT': 27, 'PRIMARY-09/10/2013': 18, 'GENERAL-11/05/2019': 18, 'PARTY_AFFILIATION': 17, 'PRIMARY-09/15/2015': 18, 'PRIMARY-09/10/2019': 18, 'PRIMARY-09/13/2016': 18, 'PRIMARY-05/03/2011': 18, 'PRIMARY-05/05/2015': 18, 'RESIDENTIAL_STATE': 17, 'GENERAL-11/08/2016': 18, 'PRIMARY-07/13/2010': 18, 'PRIMARY-05/08/2018': 18, 'COUNTY_NUMBER': 13, 'LOCAL_SCHOOL_DISTRICT': 43, 'PRIMARY-03/15/2016': 18, 'GENERAL-11/03/2009': 18, 'GENERAL-11/05/2002': 18, 'MAILING_ZIP_PLUS4': 17, 'SPECIAL-02/08/2005': 18, 'GENERAL-11/06/2018': 18, 'FIRST_NAME': 24, 'PRIMARY-05/08/2007': 18, 'MAILING_CITY': 40, 'COUNTY_COURT_DISTRICT': 21, 'GENERAL-11/03/2015': 18, 'SUFFIX': 6, 'SPECIAL-02/07/2006': 18, 'CAREER_CENTER': 35, 'PRIMARY-03/06/2012': 18, 'PRIMARY-05/02/2017': 18, 'COUNTY_ID': 9, 'CITY': 31, 'GENERAL-11/06/2012': 18, 'PRECINCT_CODE': 13, 'PRIMARY-05/07/2019': 18, 'PRIMARY-05/06/2014': 18, 'STATE_SENATE_DISTRICT': 21, 'GENERAL-11/04/2003': 18, 'GENERAL-11/07/2000': 18, 'PRIMARY-09/12/2017': 18, 'VOTER_STATUS': 12, 'PRIMARY-05/07/2013': 18, 'PRIMARY-09/13/2011': 18, 'MAILING_SECONDARY_ADDRESS': 31, 'MAILING_ADDRESS1': 49, 'STATE_REPRESENTATIVE_DISTRICT': 29, 'GENERAL-11/18/2008': 18, 'GENERAL-11/06/2001': 18, 'EXEMPTED_VILL_SCHOOL_DISTRICT': 44, 'GENERAL-12/11/2007': 18, 'RESIDENTIAL_ADDRESS1': 37, 'PRIMARY-05/03/2005': 18, 'PRIMARY-09/08/2009': 18, 'PRIMARY-10/01/2013': 18, 'REGISTRATION_DATE': 17, 'RESIDENTIAL_COUNTRY': 19, 'PRECINCT_NAME': 40, 'MAILING_POSTAL_CODE': 19, 'PRIMARY-03/02/2004': 18, 'VILLAGE': 26, 'GENERAL-06/07/2016': 18, 'GENERAL-08/07/2018': 18, 'MIDDLE_NAME': 25, 'PRIMARY-11/06/2007': 18, 'PRIMARY-05/05/2009': 18, 'RESIDENTIAL_CITY': 20, 'STATE_BOARD_OF_EDUCATION': 24, 'WARD': 27, 'LAST_NAME': 27, 'TOWNSHIP': 24, 'MAILING_COUNTRY': 25}
	header_col_sizes = {k : header_col_sizes[k] for k in headers} # reorder to default order
	header_col_sizes['latitude'] = 30
	header_col_sizes['longitude'] = 30
	create_table(tablename,header_col_sizes,primary_key='SOS_VOTERID')


def ingest_csvs(chunk_size=10000,max_chunks=None,max_files = None,tablename='voters'):
	for i, filename in enumerate(ohio_vf_filenames):
		if max_files and i + 1 > max_files:
			break
		for j, chunk_df in enumerate(get_pandas_in_chunks(filename,chunk_size,skip_blocks=0)):
			if max_chunks and j+ 1 > max_chunks:
				break
			print(f"File {i} CHUNK {j}")
			append_df_to_table(tablename,chunk_df)

def get_number_rows_in_vfs():
	return sum([get_number_rows(filename) for filename in ohio_vf_filenames])



def generate_test_report():
	tablename = get_test_tablename()
	generate_report_all(tablename)
	generate_report_geocoded(tablename)



def report_outfilename(tablename,base="all",extra=""):
	return f'{ohio_vf_dir}/{tablename}_{base}_rows{"_"+ extra if extra else ""}.csv'

def generate_report_geocoded(tablename):
	filename=report_outfilename(tablename=tablename,base="geocoded",extra="")
	query = select_by_has_latlon(tablename,want_geocoded=True,onlyquery=True)
	generate_report_query(tablename,filename,query)
	if os.path.isfile(filename):
		print(f"{get_number_rows(filename)} in get_number_rows({filename})")
	else:
		print(f"FILE NOT CREATED {filename}")

def generate_report_all(tablename):
	filename = report_outfilename(tablename=tablename)
	generate_report(tablename,filename)
	if os.path.isfile(filename):
		print(f"{get_number_rows(filename)} in get_number_rows({filename})")
	else:
		print(f"FILE NOT CREATED {filename}")
#### TESTING

def get_test_tablename():
	return "voterstesting"

def create_test_table(report=False):
	drop_test_table()
	tablename = get_test_tablename()
	create_db_with_appropriate_col_sizes(tablename=tablename)
	ingest_csvs(chunk_size=1000,max_chunks=2,max_files = 1,tablename=tablename)
	print(f"{get_number_records(tablename)} in get_number_records('{tablename}')")
	if report:
		generate_test_report()


def drop_test_table():
	tablename = get_test_tablename()
	try:
		drop_table(tablename)
	except Exception as e:
		print(e)
	print(f"DROPPED {tablename}")

def testFunction(df):
	for i, row in df.iterrows():
		df.at[i,'LAST_NAME'] = f"000mod000{i}{df.at[i,'LAST_NAME']}{i}"
		df.at[i,'latitude'] = f"{i}"
		df.at[i,'longitude'] = f"{i}"
	return


def get_test_chunker():
	tablename = get_test_tablename()
	return select_all(tablename,chunksize=50)

def get_test_query():
	tablename = get_test_tablename()
	# return f"select * from {tablename}"
	return get_empty_laton_query(tablename)

def get_empty_laton_query(tablename):
	return f"SELECT * FROM {tablename} WHERE  (latitude = '' OR latitude IS NULL) AND (longitude = '' OR longitude IS NULL)"

def get_has_latlon_query(tablename):
	return f"SELECT * FROM {tablename} WHERE  latitude <> '' AND longitude <> ''"

def iterate_over_has_latlon():
	pass

def iterate_over_empty_latlon():
	pass


def get_test_query_chunker(query):
	tablename = get_test_tablename()
	return select_where(tablename,query,chunksize=50)



def test_random_sample():
	fraction = .001
	chunksize=10000
	nrows = get_number_records('voters')
	plan_to_see = int(fraction*nrows)
	planned_number_chunks = math.ceil(nrows/ chunksize)
	nseen = 0
	for i,df in enumerate(sample_where('voters',fraction=fraction,chunksize = chunksize)):
		nseen += df.shape[0]
		print(f"Got chunk {i}/{planned_number_chunks}, seen records {nseen}/{plan_to_see}")


def test_counting(tablename):
	print(get_number_records(tablename))
	print(count_empty_rows(tablename,['latitude','longitude']))
	print(count_nonempty_rows(tablename,['latitude','longitude']))
#### TESTING



def sample_df(df,fraction):
	nrows= df.shape[0]
	indices = sorted(np.random.choice(range(nrows),size=int(fraction*nrows),replace=False))
	return df.iloc[indices]

def select_by_has_latlon(tablename,want_geocoded=True,chunksize=200000,onlyquery=False):
	if want_geocoded:
		return select_by_empty_columns(tablename,nonempty=['latitude','longitude'],chunksize=chunksize,onlyquery=onlyquery)
	else:
		return select_by_empty_columns(tablename,empty=['latitude','longitude'],chunksize=chunksize,onlyquery=onlyquery)


def sample_uncoded(tablename,fraction=.01,chunksize=200000):
	q = select_by_has_latlon(tablename,want_geocoded=False,chunksize=chunksize)
	for df in q:
		yield sample_df(df,fraction=fraction)

def test_sample_uncoded():
	import math
	fraction = .001
	chunksize=500000
	nrows = get_number_records('voters')
	plan_to_see = int(fraction*nrows)
	planned_number_chunks = math.ceil(nrows/ chunksize)
	nseen = 0
	for i,df in enumerate(sample_uncoded('voters',fraction=fraction,chunksize=chunksize)):
		nseen += df.shape[0]
		print(f"Got chunk {i+1}/{planned_number_chunks}, seen records {nseen}/{plan_to_see}")


def test_select_where():
	tablename = 'voterstesting'
	table= get_table(tablename)
	cols = table_colnames(tablename)
	where = where_multiple(incols = {"LAST_NAME" : ["CHANTZ", "KOVAL", "VAUGHN", "RESPRESS", "PANCAKE", "BERRY", "WILLIAMS","RAIDA","HOWARD","HEJJAJI","CAMPBELL","TAYLOR","BUSSARD","COYNE","GEORGE","ROHRER","DECKER","DEGARO","MCCARTY","KNULL",]})

	quer = select_where(tablename,where=where,chunksize=200000,onlyquery=True)
	print(quer)
	quer = select_where(tablename,where=where,chunksize=200000,onlyquery=False)
	df=next(quer)
	print(df['LAST_NAME'])


def extract_address(row):
	desired_keys= ('RESIDENTIAL_ADDRESS1', 'RESIDENTIAL_SECONDARY_ADDR','RESIDENTIAL_CITY','RESIDENTIAL_STATE','RESIDENTIAL_ZIP','RESIDENTIAL_ZIP_PLUS4')
	return " ".join([v for k in desired_keys if (v:=row[k])]) #join nonempty elements, avoid re-indexing


def modify_table(tablename,df_function,chunksize=200000,fraction=1,where=None,verbose=False,max_applications=None):
	nApplied = 0
	if verbose:
		total_results = int(count_where(tablename,where=where)*fraction)
	for df in sample_where(tablename,where=where,fraction=fraction,chunksize=chunksize):
		df_function(df)
		update_records(tablename,df)
		if verbose:
			nApplied += df.shape[0]
			print(f"Applied {df_function.__name__} to {nApplied}/{total_results} (desired) rows of {tablename}")
		if max_applications and nApplied > max_applications:
			print(f"Applied {df_function.__name__} to maximum ({max_applications}) rows of {tablename}")
			break

def modify_table_sample(tablename,df_function,fraction,chunksize):
	for df in sample_where(tablename,fraction,chunksize):
		df_function(df)
		update_records(tablename,df)



## TESTING: FAKE PARAMETER
def geocode_all(df,fake=True):
	nrows = df.shape[0]
	for i in range(nrows):
		latlon = geocode_address(extract_address(df.iloc[i]),fake=fake)
		df.iloc[i]['latitude'] =  latlon[0]
		df.iloc[i]['longitude'] =  latlon[1]

def geocode_sample_of_counties(tablename,fraction=.001):
	county_numbers = get_ohio_county_numbers_for_counties_with_pps()
	where_uncoded_in_counties = where_multiple(emptycols=['latitude','longitude'],incols={'COUNTY_NUMBER': county_numbers})
	n_total = get_number_records(tablename)
	n_uncoded=count_where(tablename,where_uncoded_in_counties)
	# n_coded = count_nonempty_rows(tablename,['latitude','longitude'])
	n_to_geocode = int(fraction*n_total)
	fraction_uncoded = n_to_geocode/n_uncoded
	print(f"Geocoding {n_to_geocode} rows. In {tablename}, {n_uncoded}/{n_total} are uncoded and in desired counties")
	modify_table(tablename,geocode_all,fraction=fraction_uncoded,where=where_uncoded_in_counties,max_applications=n_to_geocode,verbose=True)



if __name__=="__main__":
	# test_counting('voterstesting')
	# test_select_where()
	# create_test_table(report=False)
	tablename = 'voterstesting'
	# generate_test_report()
	geocode_sample_of_counties(tablename,fraction=.01)
	generate_test_report()


	# create_db_with_appropriate_col_sizes(tablename=tablename)
	# ingest_csvs(chunk_size=1000,max_chunks=2,max_files = 1,tablename=tablename)
	# generate_report(tablename,outfilename,chunksize=1000)

	# print(f"{get_number_rows_in_vfs()} in get_number_rows_in_vfs()")
	# print(f"{get_number_rows(outfilename)} in get_number_rows({outfilename})")
	# print(f"{get_number_records(tablename)} in get_number_records('{tablename}')")

	# ingest_csvs(chunk_size=1000)
	# generate_report('voters',f'{ohio_vf_dir}/all_rows.csv',chunksize=10000)



	# delete_table('voters')
	# create_db_with_appropriate_col_sizes()
	# delete_table('voters')
	# ingest_csvs(chunk_size=1000,max_chunks=2,max_files = 1)
	# generate_report('voters',f'{ohio_vf_dir}/all_rows.csv',chunksize=1000)
	# modify_table_sample('voters',my_fun,.001,1000)
	# generate_report('voters',f'{ohio_vf_dir}/all_rows.csv',chunksize=1000)



# header_col_sizes = {'MAILING_STATE': 13, 'PRIMARY-09/11/2007': 18, 'RESIDENTIAL_SECONDARY_ADDR': 26, 'MUNICIPAL_COURT_DISTRICT': 24, 'PRIMARY-03/17/2020': 18, 'GENERAL-11/07/2017': 18, 'PRIMARY-03/07/2000': 18, 'RESIDENTIAL_POSTALCODE': 22, 'CITY_SCHOOL_DISTRICT': 33, 'GENERAL-11/02/2010': 18, 'GENERAL-11/07/2006': 18, 'SPECIAL-05/06/2003': 18, 'GENERAL-11/02/2004': 18, 'PRIMARY-09/15/2009': 18, 'PRIMARY-03/04/2008': 18, 'RESIDENTIAL_ZIP_PLUS4': 21, 'GENERAL-11/08/2011': 18, 'SPECIAL-05/08/2001': 18, 'SOS_VOTERID': 12, 'PRIMARY-09/13/2005': 18, 'PRIMARY-09/29/2009': 18, 'GENERAL-11/06/2007': 18, 'CONGRESSIONAL_DISTRICT': 22, 'COURT_OF_APPEALS': 16, 'DATE_OF_BIRTH': 13, 'GENERAL-11/04/2008': 18, 'GENERAL-11/05/2013': 18, 'PRIMARY-05/04/2010': 18, 'LIBRARY': 7, 'PRIMARY-05/02/2006': 18, 'MAILING_ZIP': 11, 'PRIMARY-09/07/2010': 18, 'GENERAL-11/04/2014': 18, 'GENERAL-11/08/2005': 18, 'PRIMARY-10/14/2008': 18, 'PRIMARY-05/07/2002': 18, 'RESIDENTIAL_ZIP': 15, 'EDU_SERVICE_CENTER_DISTRICT': 27, 'PRIMARY-09/10/2013': 18, 'GENERAL-11/05/2019': 18, 'PARTY_AFFILIATION': 17, 'PRIMARY-09/15/2015': 18, 'PRIMARY-09/10/2019': 18, 'PRIMARY-09/13/2016': 18, 'PRIMARY-05/03/2011': 18, 'PRIMARY-05/05/2015': 18, 'RESIDENTIAL_STATE': 17, 'GENERAL-11/08/2016': 18, 'PRIMARY-07/13/2010': 18, 'PRIMARY-05/08/2018': 18, 'COUNTY_NUMBER': 13, 'LOCAL_SCHOOL_DISTRICT': 43, 'PRIMARY-03/15/2016': 18, 'GENERAL-11/03/2009': 18, 'GENERAL-11/05/2002': 18, 'MAILING_ZIP_PLUS4': 17, 'SPECIAL-02/08/2005': 18, 'GENERAL-11/06/2018': 18, 'FIRST_NAME': 24, 'PRIMARY-05/08/2007': 18, 'MAILING_CITY': 40, 'COUNTY_COURT_DISTRICT': 21, 'GENERAL-11/03/2015': 18, 'SUFFIX': 6, 'SPECIAL-02/07/2006': 18, 'CAREER_CENTER': 35, 'PRIMARY-03/06/2012': 18, 'PRIMARY-05/02/2017': 18, 'COUNTY_ID': 9, 'CITY': 31, 'GENERAL-11/06/2012': 18, 'PRECINCT_CODE': 13, 'PRIMARY-05/07/2019': 18, 'PRIMARY-05/06/2014': 18, 'STATE_SENATE_DISTRICT': 21, 'GENERAL-11/04/2003': 18, 'GENERAL-11/07/2000': 18, 'PRIMARY-09/12/2017': 18, 'VOTER_STATUS': 12, 'PRIMARY-05/07/2013': 18, 'PRIMARY-09/13/2011': 18, 'MAILING_SECONDARY_ADDRESS': 31, 'MAILING_ADDRESS1': 49, 'STATE_REPRESENTATIVE_DISTRICT': 29, 'GENERAL-11/18/2008': 18, 'GENERAL-11/06/2001': 18, 'EXEMPTED_VILL_SCHOOL_DISTRICT': 44, 'GENERAL-12/11/2007': 18, 'RESIDENTIAL_ADDRESS1': 37, 'PRIMARY-05/03/2005': 18, 'PRIMARY-09/08/2009': 18, 'PRIMARY-10/01/2013': 18, 'REGISTRATION_DATE': 17, 'RESIDENTIAL_COUNTRY': 19, 'PRECINCT_NAME': 40, 'MAILING_POSTAL_CODE': 19, 'PRIMARY-03/02/2004': 18, 'VILLAGE': 26, 'GENERAL-06/07/2016': 18, 'GENERAL-08/07/2018': 18, 'MIDDLE_NAME': 25, 'PRIMARY-11/06/2007': 18, 'PRIMARY-05/05/2009': 18, 'RESIDENTIAL_CITY': 20, 'STATE_BOARD_OF_EDUCATION': 24, 'WARD': 27, 'LAST_NAME': 27, 'TOWNSHIP': 24, 'MAILING_COUNTRY': 25}
