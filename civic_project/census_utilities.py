import requests

from config import census_api_key
from census_codes import codes as demographic_data_tables_lookup, groups

def get_census_data_through_gcp(lat,lon):
	census_query_url= "https://us-central1-corded-palisade-136523.cloudfunctions.net/add_census_data"
	url = f"{census_query_url}?census_api_key={census_api_key}&lat={lat}&lng={lon}"
	response = requests.get(url)
	data = response.json()
	# if len(k for k in data.keys() if k not in census_codes.values()) > 0:
	# 	raise
	return data



def build_census_query_url():
	census_api_query_url = "https://api.census.gov/data/2017/acs/acs5?&get="
	get_items = []
	for group in groups:
		get_items.append("group(" + group + ")")

	for table_id in demographic_data_tables_lookup:
		inGroup = False
		for group in groups:
			if table_id.startswith(group):
				inGroup = True
				break
		if not inGroup:
			get_items.append(table_id)

	census_api_query_url += ','.join(get_items)
	return census_api_query_url

def get_census_data(lat,lon):
	# get base census info url
	census_api_query_url = build_census_query_url()
	# get census tract id from lat lng
	url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lon}&y={lat}&benchmark=Public_AR_Census2010&vintage=Census2010_Census2010&layers=Census%20Tracts&format=json"
	r = requests.get(url)
	if r.status_code != 200:
		print("'r.status != 200' (census)")
		raise
	data = r.json()
	if 'result' not in data:
		print("'result' not in data! (census)")
		raise
	result = data['result']
	if 'geographies' in result:
		geographies = result['geographies']
		if 'Census Tracts' in geographies:
			census_tract_id = geographies['Census Tracts'][0]['TRACT']
			state_id = geographies['Census Tracts'][0]['STATE']
			county_id = geographies['Census Tracts'][0]['COUNTY']

			slug = '&for=tract:' + census_tract_id + '&in=state:' + state_id + '+county:' + county_id
			url = census_api_query_url + slug + '&key=' + census_api_key

			r2 = requests.get(url)
			if r2.status_code != 200:
				print("'r2.status != 200' (census)")
				raise
			data = r2.json()
			if len(data) > 1:
				headers = data[0]
				values = data[1]

				i = 0
				out = {}
				for table_id in headers:
					if table_id in demographic_data_tables_lookup:
						table_name = demographic_data_tables_lookup[table_id]
						out[table_name] = values[i]
					i += 1
				return out




