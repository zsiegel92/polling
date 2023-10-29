import os
import csv
import pandas as pd
import numpy as np
from datetime import datetime
from config import civic_query_results_dir, map_dir, ohio_vf_filenames, ohio_geocoded_vf_dir, metro_delineation_file, county_shape_full_path, metro_shape_full_path,ohio_fake_vf_filenames, ohio_fake_geocoded_vf_dir,ohio_geocoded_source
from shapefile_utilities import get_interested_metro_properties_by_namelsad, get_properties_only
from geojson_utilities import create_point, list_of_points_to_featureCollection
from geocode_utilities import geocode_address, bulk_geocode_mapquest
from csv_utilities import get_headers, get_rows, write_csv
from ohio_county_numbers import county_numbers

# ohio_vf_filenames = [ ohio_geocoded_source ] # Already GEOCODED
# ohio_vf_filenames = ohio_fake_vf_filenames #SAMPLE
# ohio_geocoded_vf_dir = ohio_fake_geocoded_vf_dir #SAMPLE



# from plotly_utilities import metro_to_dataframe, metro_mapbox,add_scatter_pps, save_plot as save_plotly, show_fig, open_in_chrome,generate_plot, init_fig, add_metro_to_fig, add_all_to_plot, add_multiple_metros_to_fig


# filename = "civicAPI_unique_places_Providence-Warwick,_RI-MA_Metro_Area_spacing_1km_02_11_2020_21_43.csv"
def get_nameLSAD_from_filename(filename):
	return filename.rsplit("_unique_places_",1)[-1].split("_spacing_")[0].replace("_", " ")

def get_all_pp_filenames():
	return [filename for filename in os.listdir(civic_query_results_dir) if 'unique_places' in filename]


def get_pp_metros():
	pass

def is_in_metro(row,metro_counties_list):
	pass


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
	# ohio_county_numbers = [county['properties']['COUNTYFP'] for county in ohio_counties]
	return ohio_counties


def get_rows_from_files(fnames):
	return pd.concat([row for fname in fnames for row in get_rows(fname,pandas_safe=True)]) #get_rows(fname,safe=True)


def has_been_geocoded(row):
	return (row.get('latitude')) and (row.get('longitude'))


def sample_non_geocoded_rows(rows,fraction=.001):
	all_indices = range(len(rows))
	uncoded_indices = [ind for ind in all_indices if not has_been_geocoded(rows[ind]) ]
	indices = sorted(list(np.random.choice( uncoded_indices ,size=int(fraction*len(rows)),replace=False)))
	to_be_geocoded = [rows[ind] for ind in indices]
	return to_be_geocoded


def extract_address(row):
	desired_keys= ('RESIDENTIAL_ADDRESS1', 'RESIDENTIAL_SECONDARY_ADDR','RESIDENTIAL_CITY','RESIDENTIAL_STATE','RESIDENTIAL_ZIP','RESIDENTIAL_ZIP_PLUS4')
	return " ".join([v for k in desired_keys if (v:=row[k])]) #join nonempty elements, avoid re-indexing


def flatten(chunked_list):
	return [item for chunk in chunked_list for item in chunk]

def break_into_chunks(flat_list,chunk_size):
	return [flat_list[i * chunk_size:(i + 1) * chunk_size] for i in range((len(flat_list) + chunk_size - 1) // chunk_size )]


def geocode_rows(rows,fraction=.001):
	to_be_geocoded = sample_non_geocoded_rows(rows,fraction=fraction)
	addresses = [extract_address(row) for row in to_be_geocoded]
	chunk_size = 100
	# row_chunks = break_into_chunks(to_be_geocoded)
	latlons = flatten([bulk_geocode_mapquest(address_chunk) for address_chunk in  break_into_chunks(addresses,chunk_size)])
	for latlon,row in zip(latlons,to_be_geocoded):
		row['latitude'] = latlon[0]
		row['longitude'] = latlon[1]


def get_outfilename(ohio_counties,statefips):
	timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M")
	# county_string = "counties_" + "_".join(set([county['properties']['COUNTYFP'] for county in counties]))
	# state_string = "state_" + "_".join(set([county['properties']['STATEFP'] for county in counties]))
	county_numbers = [county['properties']['COUNTYFP'] for county in ohio_counties]
	county_string = f"counties_{'_'.join(county_numbers)}"
	state_string = f"state_{statefips}"
	return f"{ohio_geocoded_vf_dir}/geocoded_{state_string}_{county_string}_{timestamp}.csv"




def transform_counties_to_ohio_alphabetical(ohio_counties):
	return [ str(county_numbers[county['properties']['NAME']]).zfill(2) for county in ohio_counties ]


def get_number_geocoded(voters):
	return sum(1 if has_been_geocoded(row) else 0 for row in voters)

if __name__=="__main__":
	ohiofips = 39
	ohio_counties = get_counties_for_state(statefips=ohiofips)
	ohio_alphabetical_county_numbers =  transform_counties_to_ohio_alphabetical(ohio_counties)
	# metros = get_properties_only(metro_shape_full_path)
	# all_county_numbers = [metro['properties']['']]
	# voters = get_rows_from_files(ohio_vf_filenames)
	
	# voters_in_counties = [row for row in voters if row['COUNTY_NUMBER'].lstrip('0') in ohio_alphabetical_county_numbers]
	# geocode_rows(voters_in_counties,fraction=.0001)
	# outfilename = get_outfilename(ohio_counties,ohiofips)
	# write_csv(outfilename,voters,quote_all=True)
	# print(f"Number geocoded: {get_number_geocoded(voters)}/{len(voters)}")

# if __name__=="__main__":
	# metro_delineation = read_delineation(metro_delineation_file)
	# counties =  get_properties_only(county_shape_full_path,write=False)
	# metros = get_properties_only(metro_shape_full_path)
	# pp_filenames = get_all_pp_filenames()
	# metroname_county_map = get_all_counties_for_all_metros_queried(metros=metros,counties=counties,metro_delineation=metro_delineation,pp_filenames=pp_filenames)
	# metroname_county_map = get_all_counties_for_all_metros_queried()
	# all_counties = [county for k,countylist in metroname_county_map.items() for county in countylist]
	# ohio_counties = extract_all_counties_with_statefips(all_counties,"39")
	# ohio_county_names = [county['properties']['NAME'] for county in ohio_counties]



