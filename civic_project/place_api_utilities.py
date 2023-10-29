import requests
from geopy.distance import distance

from config import  api_key


interesting_types = ['aquarium', 'book_store', 'church', 'city_hall', 'courthouse', 'embassy', 'fire_station', 'funeral_home','hospital', 'library', 'light_rail_station','local_government_office', 'mosque', 'museum', 'police', 'post_office', 'primary_school', 'school', 'secondary_school', 'shopping_mall', 'stadium', 'synagogue', 'train_station', 'transit_station','university']

desired_types =  ['school','local_government_office','church','post_office']

desired_types =  ['school','post_office']



# f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=34.068126,-118.444082&radius=1000&type=school&key=AIzaSyBhOqdprFXa_Poilt7blECbOd8FV6EUBlg"
## TODO:
## -uniquify results (eg in case "school" and "university" return places with same 'place_id')
## SWITCH TO "FIND A PLACE SEARCH" - free for first 100k. "NEARBY SEARCH" costs $32/1000 !!!!
def get_nearby_places(lon,lat,radius_m,n=10000):
	keys = ['name','types','vicinity','geometry','place_id']
	results = []
	for qtype in desired_types:
		nearbyquery = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lon}&radius={radius_m}&type={qtype}&key={api_key}"
		print("MAKING A PLACES API REQUEST")
		rdata = requests.get(nearbyquery).json()
		for i,result in enumerate(rdata['results']):
			if i< n:
				info = {k:result[k] for k in keys}
				info['nearby_rank'] = i
				place_lat = float(result['geometry']['location']['lat'])
				place_lon = float(result['geometry']['location']['lng'])
				info['place_lat'] = place_lat
				info['place_lon'] = place_lon
				info['distance'] = distance((lat,lon),(place_lat,place_lon)).meters #geopy uses e.g. (41.49008, -71.312796) = (lat,lon)
				results.append(info)
	return results








#TODO: this function
# "https://maps.googleapis.com/maps/api/place/nearbysearch/output?parameters"
# https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={qtype}&inputtype=textquery&fields=formatted_address,name&locationbias=circle:1000@{coords[0]},{coords[1]}&key=AIzaSyDdciA22lcmtP9OOd3CIquHRs9yoRfm_jU
def get_nearby_places_findplace_api(lon,lat,n=10):
	radius = 500 #meters
	keys = ['name','formatted_address','geometry','id','place_id']
	results = []
	# empirical qtypes (existing): {'point_of_interest': 15, 'establishment': 15, 'school': 12, 'primary_school': 8, 'secondary_school': 3, 'fire_station': 1, 'courthouse': 1, 'local_government_office': 1}
	for qtype in ['school','local_government_office','church','post_office']:
		print(f"Querying qtype {qtype}")
		findplacequery = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={qtype}&inputtype=textquery&fields={','.join(keys)}&locationbias=circle:1000@{lat},{lon}&key={api_key}"
		r = requests.get(findplacequery)
		r=r.json()
		print(r)
		if r.get('status') == 'OK':
			rdata = r['candidates']
			for i,result in enumerate(rdata):
				if i< n:
					info = {k:result[k] for k in keys}
					info['nearby_rank'] = i
					place_lat = float(result['geometry']['location']['lat'])
					place_lon = float(result['geometry']['location']['lng'])
					info['place_lat'] = place_lat
					info['place_lon'] = place_lon
					info['distance'] = distance(coords,(place_lat,place_lon)).ft
					results.append(info)

	return results





def get_place_info(placeName,lat,lon):
	# f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={coords[0]},{coords[1]}&radius={radius}&type={qtype}&key=AIzaSyDdciA22lcmtP9OOd3CIquHRs9yoRfm_jU"
	# all fields: https://developers.google.com/places/web-service/search#Fields
	q = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?inputtype=textquery&input={placeName}&locationbias=point:{lat},{lon}&fields=formatted_address,geometry,name,permanently_closed,place_id,plus_code,types&key={api_key}"
	r = requests.get(q)
	rdata = r.json()
	return rdata
