import requests
from pprint import pprint
from config import census_api_key



# Got census data with
url_geocode0 = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x=-81.072784&y=31.99613&benchmark=Public_AR_Current&vintage=Current_Current&layers=Census%20Tracts&format=json'

# Failed to get census data with
url_geocode1 = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x=-82.93347&y=34.393654&benchmark=Public_AR_Current&vintage=Current_Current&layers=Census%20Tracts&format=json'


geocoding_urls = [url_geocode0,url_geocode1]



for url_geocode in geocoding_urls:
	print(f"GEOCODING:\n{url_geocode=}")
	result = requests.get(url_geocode).json()
	tract = result['result']['geographies']['Census Tracts'][0]
	census_tract_id = tract['TRACT']
	state_id =tract['STATE']
	county_id =tract['COUNTY']
	pprint(result)
	print(f"{census_tract_id=}\n{state_id=}\n{county_id=}\n")

	url_acs=f'https://api.census.gov/data/2019/acs/acs5?get=B01003_001E&for=tract:{census_tract_id}&in=state:{state_id}+county:{county_id}&key={census_api_key}'
	result = requests.get(url_acs).text
	print(f"ACS:\n{url_acs=}\n{result = }")
	print("\n\n")

url_acs = 'https://api.census.gov/data/2019/acs/acs5?get=B01003_001E&for=tract:960103&in=state:13+county:147&key=e9cbebf0cdc461ed9e80991e8526774ab568c690'

url_acs = 'https://api.census.gov/data/2019/acs/acs5?get=B01003_001E&for=tract:*&in=state:13&key=e9cbebf0cdc461ed9e80991e8526774ab568c690'
result = requests.get(url_acs)
print(result.json())


url_acs = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x=-84.2337&y=33.662167&benchmark=Public_AR_Current&vintage=Current_Current&layers=Census%20Tracts&format=json'
url_acs='https://api.census.gov/data/2019/acs/acs5?get=group(B01001),B01003_001E,B19013_001E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B02001_006E,B02001_007E,B03001_002E,B03001_003E,B08014_002E,B08014_003E,B08014_004E,B08014_005E,B08014_006E,B08014_007E,B08015_001E,B08006_002E,B08006_008E,B08006_014E,B08006_015E,B08006_016E,B08006_017E,B15003_017E,B15003_018E,B15003_019E,B15003_020E,B15003_021E,B15003_022E,B15003_023E,B15003_024E,B15003_025E&for=tract:*&in=state:13&key=e9cbebf0cdc461ed9e80991e8526774ab568c690'
# print(url)
# print(r.text)
# r=requests.get(url)

# responses = {year: dict(zip(*requests.get(url.format(year=year)).json())) for year in (2017,2019)}
# combined = {demographic_data_tables_lookup.get(k,k) : { y: responses[y][k] for y in responses} for year in responses for k in responses[year].keys()}
# pprint(combined)

# r = requests.get(url)
# print(r.status_code)
# print(r.text)
