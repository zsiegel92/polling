import os
import pandas as pd
from config import civic_query_results_dir, map_dir, metro_delineation_file, county_shape_full_path, metro_shape_full_path
from shapefile_utilities import get_interested_metro_properties_by_namelsad, get_properties_only
from ohio_county_numbers import county_numbers as ohio_county_numbers
from georgia_county_numbers import county_numbers as georgia_county_numbers
#adapted from ./unused/geocode_vf.py

# filename = "civicAPI_unique_places_Providence-Warwick,_RI-MA_Metro_Area_spacing_1km_02_11_2020_21_43.csv"
def get_nameLSAD_from_filename(filename):
	return filename.rsplit("_unique_places_",1)[-1].split("_spacing_")[0].replace("_", " ")

def get_all_pp_filenames():
	return [filename for filename in os.listdir(civic_query_results_dir) if 'unique_places' in filename]


def read_delineation(filename):
	with open(filename,'rb') as f:
		df = pd.read_excel(f,skiprows=[0,1],header=[0],encoding='utf-8',dtype=str)
	return [dict(df.loc[i,:]) for i in range(df.shape[0])]

## metro_delineation has: ['CBSA Code', 'Metropolitan Division Code', 'CSA Code', 'CBSA Title', 'Metropolitan/Micropolitan Statistical Area', 'Metropolitan Division Title', 'CSA Title', 'County/County Equivalent', 'State Name', 'FIPS State Code', 'FIPS County Code', 'Central/Outlying County']
## metro['properties'] has: ['CSAFP', 'CBSAFP', 'GEOID', 'NAME', 'NAMELSAD', 'LSAD', 'MEMI', 'MTFCC', 'ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON']
# delineation_metro_key_map = {"CSA Code" : 'CSAFP', 'CBSA Code' : 'CBSAFP'}
def get_counties_for_metro(metro,counties,metro_delineation):
	delineation_metro_key_map = {'CBSA Code' : 'CBSAFP'}
	relevant_delineations = [row for row in metro_delineation if all(row[k] == metro['properties'][v] for k,v in delineation_metro_key_map.items()) ]
	county_delineation_key_map = {"STATEFP" : 'FIPS State Code', "COUNTYFP" : 'FIPS County Code' }
	county_shapes = [county for county in counties for row in relevant_delineations if all(county['properties'][k] == row[v] for k, v in county_delineation_key_map.items())]
	return county_shapes

def get_all_counties_for_all_metros_queried(metros=None,counties=None,metro_delineation=None,pp_filenames=None):
	if metro_delineation is None:
		metro_delineation = read_delineation(metro_delineation_file)
	if counties is None:
		counties =  get_properties_only(county_shape_full_path,write=False)
	if metros is None:
		metros = get_properties_only(metro_shape_full_path)
	if pp_filenames is None:
		pp_filenames = get_all_pp_filenames()
	metroname_county_map = {}
	for filename in pp_filenames:
		nameLSAD = get_nameLSAD_from_filename(filename)
		metro = get_interested_metro_properties_by_namelsad(nameLSAD,features=metros)
		counties_in_metro =  get_counties_for_metro(metro,counties,metro_delineation)
		metroname_county_map[nameLSAD] = counties_in_metro
		print(f"Found {len(counties_in_metro)} counties for {nameLSAD}")
	return metroname_county_map

# Not necessary because counties stored in dict hashing county names.
def deduplicate_counties(all_counties):
	return list({tuple(county['properties'].values()) : county for county in all_counties}.values())

def extract_all_counties_with_statefips(all_counties,statefips):
	return [county for county in all_counties if county['properties']['STATEFP']==statefips]


def get_counties_for_state(statefips):
	metroname_county_map = get_all_counties_for_all_metros_queried()
	ohio_counties = [county for k,countylist in metroname_county_map.items() for county in countylist if county['properties']['STATEFP']==str(statefips)]
	return ohio_counties


def transform_counties_to_ohio_alphabetical(ohio_counties):
	return [ str(ohio_county_numbers[county['properties']['NAME']]).zfill(2) for county in ohio_counties ]


def get_ohio_county_numbers_for_counties_with_pps():
	ohiofips = 39
	ohio_counties = get_counties_for_state(statefips=ohiofips)
	ohio_alphabetical_county_numbers =  transform_counties_to_ohio_alphabetical(ohio_counties)
	return ohio_alphabetical_county_numbers

if __name__=="__main__":
	print(get_ohio_county_numbers_for_counties_with_pps())



