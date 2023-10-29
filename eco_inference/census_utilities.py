import requests
import sys
import pandas as pd

from config import census_api_key,census_codes_file,census_prefetch_dir,census_acs_file, census_tract_tiger_data

from census_codes import codes as demographic_data_tables_lookup, groups as demographic_data_groups
from db_utilities import create_table_pandas, add_null_column,table_colnames,table_exists, update_records

def get_census_data_through_gcp(lat,lon):
	census_query_url= "https://us-central1-corded-palisade-136523.cloudfunctions.net/add_census_data"
	url = f"{census_query_url}?census_api_key={census_api_key}&lat={lat}&lng={lon}"
	response = requests.get(url)
	data = response.json()
	# if len(k for k in data.keys() if k not in census_codes.values()) > 0:
	# 	raise
	return data


def get_lookup_codes_and_groups():
	get_items = [table_id for table_id in demographic_data_tables_lookup if not any(table_id.startswith(group) for group in demographic_data_groups)]
	groups = demographic_data_groups
	while len(get_items) + len(groups) > 50:
		prefix_occurrences = {(prefix:=table_id.split("_",1)[0]) : len([k for k in get_items if k.startswith(prefix)]) for table_id in get_items}
		max_prefix = max(prefix_occurrences,key=lambda k: prefix_occurrences[k])
		get_items = [table_id for table_id in get_items if not table_id.startswith(max_prefix)]
		groups.append(max_prefix)
	return get_items, groups

def get_acs(tract=None,state=None,county=None,block_group=None):
	out = {}
	codes,groups = get_lookup_codes_and_groups()
	url = build_census_query_url(tract=tract,state=state,county=county,codes=codes,groups=groups,block_group=block_group)
	r = requests.get(url)
	if r.status_code != 200:
		print("'r2.status != 200' (ACS)")
		print(f"Failed to get census data with\n{url=}\n{state=}\n{county=}\n{tract=}\n{block_group=}")
	data = dict(zip(*r.json()))
	for table_id,value in data.items():
		if table_id in demographic_data_tables_lookup:
			table_name = demographic_data_tables_lookup[table_id]
			out[table_name] = value
	out['GEO_ID']=data['GEO_ID']
	return out

# example:
# https://api.census.gov/data/2019/acs/acs5?get=group(B01001),B01003_001E,B19013_001E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B02001_006E,B02001_007E,B03001_002E,B03001_003E,B08014_002E,B08014_003E,B08014_004E,B08014_005E,B08014_006E,B08014_007E,B08015_001E,B08006_002E,B08006_008E,B08006_014E,B08006_015E,B08006_016E,B08006_017E,B15003_017E,B15003_018E,B15003_019E,B15003_020E,B15003_021E,B15003_022E,B15003_023E,B15003_024E,B15003_025E&for=tract:960203&in=state:13+county:015&key=429b0be33020d3327c4d4dd16273ff357509748e
# Block group reference:
# https://api.census.gov/data/2019/acs/acs5/examples.html
# https://api.census.gov/data/2019/acs/acs5?get=NAME,B01001_001E&for=block%20group:*&in=state:01&in=county:*&in=tract:*&key=YOUR_KEY_GOES_HERE
def build_census_query_url(tract=None,state=None,county=None,codes=None,groups=None,block_group=None,for_geography='tract'):
	census_api_query_url = "https://api.census.gov/data/2019/acs/acs5?get="
	if codes is None:
		codes, groups = get_lookup_codes_and_groups()
	get_items = [table_id for table_id in codes if not any(table_id.startswith(group) for group in groups)]
	get_items += [f"group({group})" for group in groups]
	census_api_query_url += ','.join(get_items)
	statestr = f"state:{state}" if state else ""
	countystr = f"county:{county}" if county else ""
	tractstr = f"tract:{tract}" if tract else ""
	blockgpstr = f"block+group:{block_group}" if block_group else ""
	if block_group is not None:
		instr = '+'.join([s for s in (statestr,countystr,tractstr) if s])
		geography=f"for={blockgpstr}&in={instr}"
	else:
		instr = '+'.join([s for s in (statestr,countystr) if s])
		geography=f"for=tract:{tract}&in={instr}"
	return f"{census_api_query_url}&{geography}&key={census_api_key}"
	# return census_api_query_url




def get_census_data_safe(lat,lon):
	# return get_census_data(lat,lon)
	try:
		return get_census_data(lat,lon)
	except Exception as e:
		print(f"Error getting census data:\n{e}")
		return {}


# it looks like the tract you included in the second URL
# (&for=tract:050744&in=state:13+county:135) is a new one for
# 2020.  You won't be able to run a successful query for this tract in the
#  API until this upcoming fall when the 2020 ACS data is released.  Until then, you'll only be able to get data for tracts that exist in 2019.
# The way around this in the geocoding portion is to keep your
# benchmark=Public_AR_Current and change the vintage=ACS2019_Current.
# This will give you the tracts for the 2019 ACS. (edited)
def get_census_data(lat,lon):
	# vintage can be Current_Current, Census2020_Census2020, ACS2019_Current
	# benchmark can be Public_AR_Current, Public_AR_Census2020
	url_geocode = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lon}&y={lat}&benchmark=Public_AR_Current&vintage=ACS2019_Current&layers=Census%20Tracts&format=json"
	r = requests.get(url_geocode)
	if r.status_code != 200:
		print("'r.status != 200' (census geocoding)")
	data = r.json()
	tract = data['result']['geographies']['Census Tracts'][0]
	census_tract_id = tract['TRACT']
	state_id =tract['STATE']
	county_id =tract['COUNTY']
	return get_acs(tract=census_tract_id,state=state_id,county=county_id)

# def ingest_csvs(chunksize=200000,tablename='georgia_voters',sep=",",skip_blocks=0,quotenone=False):
def ingest_acs():
	tablename = "acs5_2019"
	# from db_utilities import drop_table
	# drop_table(tablename)
	# pkeys = ["state","county","tract"]
	# df = pd.read_csv(census_acs_file,sep=",",index_col=pkeys)
	# create_table_pandas(tablename,df,pkeys=pkeys)
	acs_df = pd.read_csv(census_acs_file,sep=",")
	tract_aland_data = pd.read_csv(census_tract_tiger_data,sep='\t',dtype={'GEOID':str})
	ga_tract_info = tract_aland_data.loc[tract_aland_data['USPS']=='GA']
	acs_df['ga_GEO_ID'] = acs_df['GEO_ID'].str.slice(start=-11)
	merged = acs_df.merge(ga_tract_info,how='left',left_on='ga_GEO_ID',right_on='GEOID')
	acs_df['ALAND'] = merged['ALAND']
	acs_df.drop(columns='ga_GEO_ID',inplace=True)
	if not table_exists(tablename):
		create_table_pandas(tablename,acs_df,pkeys='GEO_ID')
		return
	if 'ALAND' not in table_colnames(tablename):
		add_null_column(tablename,'ALAND',column_type=50)
		update_records(tablename,acs_df,pkey='GEO_ID')

if __name__=="__main__":
	# ingest_acs()
	tablename = "acs5_2019"
	acs_df = pd.read_csv(census_acs_file,sep=",")
	tract_aland_data = pd.read_csv(census_tract_tiger_data,sep='\t',dtype={'GEOID':str})
	ga_tract_info = tract_aland_data.loc[tract_aland_data['USPS']=='GA']
	acs_df['ga_GEO_ID'] = acs_df['GEO_ID'].str.slice(start=-11)
	merged = acs_df.merge(ga_tract_info,how='left',left_on='ga_GEO_ID',right_on='GEOID')
	acs_df['ALAND'] = merged['ALAND']
	acs_df.drop(columns='ga_GEO_ID',inplace=True)
	if not table_exists(tablename):
		create_table_pandas(tablename,acs_df,pkeys='GEO_ID')
		assert False
	if 'ALAND' not in table_colnames(tablename):
		add_null_column(tablename,'ALAND',column_type=50)
		update_records(tablename,acs_df,pkey='GEO_ID')
# def trywrap(callable):
# 	try:
# 		return callable()
# 	except Exception as e:
# 		print(e)
# 		raise

