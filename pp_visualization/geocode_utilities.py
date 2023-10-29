import requests
import json

from config import api_key, mapquest_api_key, geoapify_api_key

def geocode_address_through_gcp(address="",city="",state=""):
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
	return (lat,lon)


def reverse_geocode_latlon_through_gcp(lat,lon):
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


def geocode_address(address="",city="",state=""):
	fullAddress = f"{address} {city} {state}"
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

def reverse_geocode_latlon(lat,lon):
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
# 	'locations' :
# 	[
# 		{
# 		'street': '1835 1/2 Carmona Ave',
# 		'city' : 'Los Angeles',
# 		'state': 'CA',
# 		'postalCode' : '90019'
# 		},
# 		{
# 		'street': '3103 Livonia Ave',
# 		'city' : 'Los Angeles',
# 		'state': 'CA'
# 		},
# 	]
# }
#
# Returns [(34.040681, -118.361003)]
def bulk_geocode_mapquest(addresses):
	locations = {'locations' : addresses}
	base_url = f'https://www.mapquestapi.com/geocoding/v1/batch?&inFormat=json&outFormat=json&key={mapquest_api_key}'
	response = requests.post(base_url,json=locations)
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
	return [(result['locations'][0]['latLng']['lat'],result['locations'][0]['latLng']['lng']) for result in results]


### GEOAPIFY
# https://apidocs.geoapify.com/playground/geocoding
# https://www.geoapify.com/pricing
# Returns (34.04027, -118.361281)
def geocode_geoapify(address):
	base_url=f"https://api.geoapify.com/v1/geocode/search?lang=en&limit=1&apiKey={geoapify_api_key}"
	# text=1835%201%2F2%20Carmona%20Ave%2C%20Los%20Angeles%2C%20CA%2090019
	response = requests.get(base_url,params={'text':address})
	data = response.json()
	lng, lat = data['features'][0]['geometry']['coordinates']
	return (lat, lng)
