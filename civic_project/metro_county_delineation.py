import pandas as pd
from turfpy.transformation import union, intersect
from config import civic_query_results_dir, metro_delineation_file, county_shape_full_path, metro_shape_full_path
from shapefile_utilities import get_interested_metro_properties_by_namelsad, get_interested_metro_by_namelsad, get_properties_only,load_geojson_features
#adapted from ./unused/geocode_vf.py


def read_delineation(filename):
	with open(filename,'rb') as f:
		df = pd.read_excel(f,skiprows=[0,1],header=[0],encoding='utf-8',dtype=str)
	return [dict(df.loc[i,:]) for i in range(df.shape[0])]



# Not necessary because counties stored in dict hashing county names.
def deduplicate_counties(all_counties):
	return list({tuple(county['properties'].values()) : county for county in all_counties}.values())

def extract_all_counties_with_statefips(all_counties,statefips):
	return [county for county in all_counties if county['properties']['STATEFP']==statefips]



	# return intersect(union_of_counties,metro)
#
# metro_delineation has: ['CBSA Code', 'Metropolitan Division Code', 'CSA Code', 'CBSA Title', 'Metropolitan/Micropolitan Statistical Area', 'Metropolitan Division Title', 'CSA Title', 'County/County Equivalent', 'State Name', 'FIPS State Code', 'FIPS County Code', 'Central/Outlying County']
## metro['properties'] has: ['CSAFP', 'CBSAFP', 'GEOID', 'NAME', 'NAMELSAD', 'LSAD', 'MEMI', 'MTFCC', 'ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON']
# delineation_metro_key_map = {"CSA Code" : 'CSAFP', 'CBSA Code' : 'CBSAFP'}
def get_counties_for_metro(metro):
	counties =  load_geojson_features(county_shape_full_path)
	metro_delineation = read_delineation(metro_delineation_file)
	delineation_metro_key_map = {'CBSA Code' : 'CBSAFP'}
	relevant_delineations = [row for row in metro_delineation if all(row[k] == metro['properties'][v] for k,v in delineation_metro_key_map.items()) ]
	county_delineation_key_map = {"STATEFP" : 'FIPS State Code', "COUNTYFP" : 'FIPS County Code' }
	county_shapes = [county for county in counties for row in relevant_delineations if all(county['properties'][k] == row[v] for k, v in county_delineation_key_map.items())]
	return county_shapes

# POSSIBLY DON'T NEED COUNTIES.
def get_metro_intersect_state(metro,statefp):
	# all_counties =  load_geojson_features(county_shape_full_path)
	counties_in_metro =  get_counties_for_metro(metro)
	counties_in_metro_and_state = [county for county in counties_in_metro if county['properties']['STATEFP'] == statefp]
	union_of_counties = union(counties_in_metro_and_state)
	return union_of_counties

def get_metro_intersect_state_by_nameLSAD(metro_nameLSAD,statefp=None):
	metro = get_interested_metro_by_namelsad(metro_nameLSAD)
	metroname = metro['properties']['NAMELSAD']
	if statefp is not None:
		metro = get_counties_for_metro_with_statefp(metro,statefp)
	metro['properties']['NAMELSAD'] = metroname
	return metro

def get_counties_for_metro_with_statefp(metro,statefp):
	counties_in_metro =  get_counties_for_metro(metro)
	counties_in_metro_and_state = [county for county in counties_in_metro if county['properties']['STATEFP'] == statefp]
	return union(counties_in_metro_and_state)

def test_plot_metro_intersect_state(metro_nameLSAD,statefp):
	import pathlib
	from plotly_utilities import add_multiple_metros_to_fig, open_in_chrome, save_plot,init_fig,save_plot
	metro = get_interested_metro_by_namelsad(metro_nameLSAD)
	metrotitle = metro['properties']['NAMELSAD'].replace(" ","_")
	plot_path = f"{pathlib.Path().absolute()}/figures/metro_intersect_state_{metrotitle}.html"
	counties_in_metro_and_state = get_counties_for_metro_with_statefp(metro,statefp)
	fig = init_fig()
	fig = add_multiple_metros_to_fig(fig,[metro,counties_in_metro_and_state])
	save_plot(fig,plot_path)
	open_in_chrome(plot_path)

if __name__=="__main__":
	metro_nameLSADs = ["Chattanooga, TN-GA Metro Area","Columbus, GA-AL Metro Area","Augusta-Richmond County, GA-SC Metro Area"]
	for metro_nameLSAD in metro_nameLSADs:
		statefp = "13" # "STATEFP" of "Rockdale" county, in Atlanta metro
		test_plot_metro_intersect_state(metro_nameLSAD,statefp)




