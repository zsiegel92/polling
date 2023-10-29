import requests
import json
import pandas as pd
from io import StringIO
from requests.exceptions import RequestException
from requests_toolbelt.multipart.encoder import MultipartEncoder
import csv
import time
from config import api_key, mapquest_api_key, geoapify_api_key


def _parse_row(row):
	row['longitude'], row['latitude'] = None, None
	if row['coordinate']:
		try:
			row['longitude'], row['latitude'] = tuple(
				float(a) for a in row['coordinate'].split(',')
				)
		except:
			pass
	del row['coordinate']
	row['match'] = row.get('match') == 'Match'
	return row


# https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/census-geocoder.html
# INPUT: dataframe with columns ['id','street','city','state','zip']
# OUTPUT: dataframe with columns ['id','latitude','longitude']
# RETURNS SOMETHING LIKE:
#           id                                            address  match matchtype  ...   lat   lon  longitude   latitude
# 0   12057483  3769 LOWER FAYETTEVILLE RD , NEWNAN, Georgia, ...  False      None  ...  None  None        NaN        NaN
# 1   06097302        222 ST CHARLES AVE , WADLEY, Georgia, 30477  False      None  ...  None  None        NaN        NaN
# 2   08710070  1904 MONTEREY PKWY , SANDY SPRINGS, Georgia, 3...   True     Exact  ...  None  None -84.376390  33.962112
# 3   02249794        228 VALLEY RD , COLBERT, Georgia, 306282718   True     Exact  ...  None  None -83.170876  34.059700
# 4   10255890     4963 W RIDGE DR , DOUGLASVILLE, Georgia, 30135   True     Exact  ...  None  None -84.796425  33.670560
def bulk_geocode_census(df):
	returntype = 'geographies'
	fieldnames = [
		'id', 'address', 'match', 'matchtype', 'parsed', 'coordinate',
		'tigerlineid', 'side', 'census_state', 'census_county', 'census_tract',
		'census_block'
		]
	# returntype='locations'
	# fieldnames = ['id', 'address', 'match', 'matchtype', 'parsed', 'coordinate', 'tigerlineid', 'side']
	if len(df) > 10000:
		raise
	f = StringIO()
	print(f"Geocoding {df.shape[0]} rows with US Census")
	df.to_csv(f, header=False, quoting=csv.QUOTE_NONE)
	url = f"https://geocoding.geo.census.gov/geocoder/{returntype}/addressbatch"
	start = time.time()
	try:
		form = MultipartEncoder(
			fields={
				'vintage': 'ACS2019_Current', #'Census2020_Census2020',
				'benchmark': 'Public_AR_Current', #'Public_AR_Census2020',
				'addressFile': ('batch.csv', f, 'text/plain')
				}
			)
		h = {'Content-Type': form.content_type}
		with requests.post(url, data=form, headers=h) as r:
			# return as list of dicts
			data = r.text
	except RequestException as e:
		raise e
	finally:
		f.close()
	print(f"GOT RESPONSE FROM US CENSUS! Took {time.time()-start : .2f}s")
	with StringIO(data) as f:
		reader = csv.DictReader(f, fieldnames=fieldnames)
		outlist = []
		try:
			for row in reader:
				outlist.append(_parse_row(row))
		except:
			return pd.DataFrame()
	outdf = pd.DataFrame(outlist)
	# for non-geocoded rows fix!
	outdf['geocode_source'] = 'census'
	n_googled = 0
	n_to_google = outdf['latitude'].isna().sum()
	print(f"Geocoding {n_to_google} rows with Google....")
	for i, row in outdf.iterrows():
		if isna(outdf.at[i, 'latitude']) or isna(outdf.at[i, 'longitude']):
			n_googled += 1
			try:
				outdf.at[i, 'latitude'], outdf.at[i, 'longitude'], outdf.at[
					i, 'census_state'], outdf.at[i, 'census_county'], outdf.at[
						i, 'census_tract'], outdf.at[
							i, 'census_block'] = get_geographies_google_census(
								outdf.at[i, 'address']
								)
				outdf.at[i, 'geocode_course'] = 'google_after_census_failed'
			except Exception as e:
				print(f"Error Geocoding with Google on row {i}")
			# if n_googled % 10 == 0:
			# 	print(f"Geocoded {n_googled}/{n_to_google} rows with Google so far...")
	print(f"Geocoded {n_googled}/{outdf.shape[0]} rows with Google.")
	# print(outdf[['id','address','parsed','census_state','census_county','census_tract','census_block','latitude','longitude']])
	# outdf.to_csv(open('census_sample.csv','w'),header=True)
	return outdf


def isna(val):
	return val in (None, 'NaN') or pd.isna(val)


def get_geographies_google_census(address):
	# print("QUERYING GOOGLE AFTER CENSUS FAILURE")
	lat, lng = geocode_address(address)
	census_state, census_county, census_tract, census_block = get_census_geographies(
		lat, lng
		)
	return lat, lng, census_state, census_county, census_tract, census_block


def geocode_address_through_gcp(address="", city="", state=""):
	fullAddress = f"{address} {city} {state}"
	url = f"https://us-central1-corded-palisade-136523.cloudfunctions.net/geocode?api_key={api_key}&address={fullAddress}"
	# print("MAKING A GEOCODING REQUEST!")
	response = requests.get(url)
	data = response.json()
	status = data['status']
	if status != "OK":
		print(f"ERROR GEOCODING: {address = } :: {city = } :: {state = }")
		raise
	result = data['result']
	lat = result['lat']
	lon = result['lng']
	return (lat, lon)


def reverse_geocode_latlon_through_gcp(lat, lon):
	url = f"https://us-central1-corded-palisade-136523.cloudfunctions.net/geocode?api_key={api_key}&lat={lat}&lng={lon}"
	# print("MAKING A ~REVERSE~ GEOCODING REQUEST!")
	response = requests.get(url)
	data = response.json()
	status = data['status']
	if status != "OK":
		print(f"ERROR REVERSE GEOCODING: {lat = } :: {lon = }")
		raise
	result = data['result']
	address = result['formatted_address']
	return address


def geocode_address(address="", city="", state="", fake=False):
	if fake:
		return (0, 0)
	fullAddress = " ".join([
		component for component in (address, city, state) if component
		])
	# fullAddress = f"{address} {city} {state}"
	url = f"https://maps.googleapis.com/maps/api/geocode/json?address={fullAddress}&key={api_key}"
	response = requests.get(url)
	data = response.json()
	status = data['status']
	if status in ('ZERO_RESULTS', 'REQUEST_DENIED'):
		print(f"ERROR GEOCODING: {address = } :: {city = } :: {state = }")
		raise
	result = data['results'][0]
	coords = result['geometry']['location']
	lat = coords['lat']
	lng = coords['lng']
	return (lat, lng)


def get_census_geographies(lat, lon):
	url_geocode = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lon}&y={lat}&benchmark=Public_AR_Current&vintage=ACS2019_Current&layers=Census%20Tracts&format=json"
	r = requests.get(url_geocode)
	if r.status_code != 200:
		print("'r.status != 200' (census geocoding)")
	data = r.json()
	tract = data['result']['geographies']['Census Tracts'][0]
	census_tract_id = tract['TRACT']
	state_id = tract['STATE']
	county_id = tract['COUNTY']
	return state_id, county_id, census_tract_id, None


def reverse_geocode_latlon(lat, lon):
	url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={api_key}"
	response = requests.get(url)
	data = response.json()
	status = data['status']
	if status in ('ZERO_RESULTS', 'REQUEST_DENIED'):
		print(f"ERROR REVERSE GEOCODING: {lat = } :: {lon = }")
		raise
	result = data['results'][0]
	address = result['formatted_address']
	return address


### MAPQUEST: up to 100 addresses per request
# https://developer.mapquest.com/documentation/geocoding-api/
# https://developer.mapquest.com/plans
# FREE 15,000 transactions per month
## Example Argument
# locations = {
#   'locations' :
#   [
#       {
#       'street': '1835 1/2 Carmona Ave',
#       'city' : 'Los Angeles',
#       'state': 'CA',
#       'postalCode' : '90019'
#       },
#       {
#       'street': '3103 Livonia Ave',
#       'city' : 'Los Angeles',
#       'state': 'CA'
#       },
#   ]
# }
#
# Returns [(34.040681, -118.361003)]
def bulk_geocode_mapquest(addresses):
	locations = {'locations': addresses}
	base_url = f'https://www.mapquestapi.com/geocoding/v1/batch?&inFormat=json&outFormat=json&key={mapquest_api_key}'
	response = requests.post(base_url, json=locations)
	data = response.json()
	try:
		status = data['info']['statuscode']
		if status != 0:
			print(f"ERROR BULK GEOCODING")
			print(data)
			raise
	except Exception as e:
		print(data)
		print(e)
		raise
	results = data['results']
	return [(
		result['locations'][0]['latLng']['lat'],
		result['locations'][0]['latLng']['lng']
		) for result in results]


### GEOAPIFY
# https://apidocs.geoapify.com/playground/geocoding
# https://www.geoapify.com/pricing
# Returns (34.04027, -118.361281)
def geocode_geoapify(address):
	base_url = f"https://api.geoapify.com/v1/geocode/search?lang=en&limit=1&apiKey={geoapify_api_key}"
	# text=1835%201%2F2%20Carmona%20Ave%2C%20Los%20Angeles%2C%20CA%2090019
	response = requests.get(base_url, params={'text': address})
	data = response.json()
	lng, lat = data['features'][0]['geometry']['coordinates']
	return (lat, lng)
