import requests

from config import api_key

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

