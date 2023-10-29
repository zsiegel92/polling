import requests
import xmltodict
import pprint
import json
import os
from utilities import get_headers,get_rows,write_csv
from geopy.distance import distance
from collections import Counter

csvdirectory="/Users/zach/Documents/UCLA_classes/research/Polling/results"
csv_combined_directory=f"{csvdirectory}/combined"
cities =["lafayette","baton_rouge","shreveport","new_orleans"]
apikey=os.environ.get("GOOGLE_PLACE_API_KEY")


pp = pprint.PrettyPrinter(indent=4)


# https://developers.google.com/places/supported_types
# https://developers.google.com/places/web-service/search


def get_combined_results_filenames():
	filenames = [filename for filename in os.listdir(csv_combined_directory) if filename.endswith(".csv") and not ("_with_closest_info" in filename) and not ("only_polling_locations" in filename) and not ("with_candidate_locations" in filename) and not ("FINDPLACE_API" in filename) and not ("nearbyto" in filename)]
	return filenames
def get_placetypes_filenames():
	filenames = [filename for filename in os.listdir(csv_combined_directory) if filename.endswith(".csv") and 'with_place_types_' in filename]
	return filenames


	return filenames
def get_poll_locations():
	filenames = get_combined_results_filenames()
	print(filenames)
	os.chdir(csv_combined_directory)
	polling_location_info = ['pollingLocationLatitude', 'pollingLocationLongitude', 'pollingLocationName', 'pollingLocationAddress', 'pollingLocationCity', 'pollingLocationState', 'pollingLocationZip', 'pollingLocationNotes', 'pollingLocationHours']
	for filename in filenames:
		print(f"PROCESSING {filename}")
		outfilename = "only_polling_locations_" + filename.split(".")[0] + ".csv"
		headers= get_headers(filename)
		rows = get_rows(filename) #rows['with_polling_location'],rows['without_polling_location'],rows['all']
		pollrows = rows['with_polling_location']
		poll_locations = []
		pollingLocationNames = []
		for row in pollrows:
			if (rowname := row.get('pollingLocationName')) not in pollingLocationNames:
				pollingLocationNames.append(rowname)
				poll_locations.append({key:row.get(key) for key in polling_location_info})
		write_csv(outfilename,polling_location_info,poll_locations)
def get_poll_location_filenames():
	filenames = [filename for filename in os.listdir(csv_combined_directory) if filename.endswith(".csv") and filename.startswith("only_polling_locations_")]
	return filenames

def recall_poll_locations(filename):
	headers= get_headers(filename)
	rows = get_rows(filename)['all']
	return rows

## TODO:
## -uniquify results (eg in case "school" and "university" return places with same 'place_id')
## SWITCH TO "FIND A PLACE SEARCH" - free for first 100k. "NEARBY SEARCH" costs $32/1000 !!!!
def get_nearby_places(coords,n=10):
	# "https://maps.googleapis.com/maps/api/place/textsearch/xml?query=restaurants+in+Sydney&key=YOUR_API_KEY"
	radius = 500 #meters
	interesting = ['aquarium', 'book_store', 'church', 'city_hall', 'courthouse', 'embassy', 'fire_station', 'funeral_home','hospital', 'library', 'light_rail_station','local_government_office', 'mosque', 'museum', 'police', 'post_office', 'primary_school', 'school', 'secondary_school', 'shopping_mall', 'stadium', 'synagogue', 'train_station', 'transit_station','university']
	keys = ['name','types','vicinity','geometry','id','place_id']
	# fillerquery = "https://maps.googleapis.com/maps/api/place/textsearch/xml?query=restaurants+in+Sydney&key={apikey}"
	#NOTE: we can't query FOR these, but we get additional types returned, like "administrative_area_level_1","place_of_worship","political" eg
	# https://developers.google.com/places/web-service/supported_types#table2
	# rdata[0].keys()
	# dict_keys(['name', 'type', 'formatted_address', 'geometry', 'rating', 'icon', 'reference', 'id', 'opening_hours', 'photo', 'price_level', 'user_ratings_total', 'place_id', 'plus_code'])
	results = []
	for qtype in ['school','local_government_office','church','post_office']:
		nearbyquery = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={coords[0]},{coords[1]}&radius={radius}&type={qtype}&key={apikey}"
		r = requests.get(nearbyquery)
		# rdata=json.loads(json.dumps(xmltodict.parse(r.content)))['PlaceSearchResponse']['result']
		rdata = r.json()['results']

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
	# pp.pprint(rdata)
	return results


#TODO: this function
# "https://maps.googleapis.com/maps/api/place/nearbysearch/output?parameters"
# https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={qtype}&inputtype=textquery&fields=formatted_address,name&locationbias=circle:1000@{coords[0]},{coords[1]}&key={apikey}
def get_nearby_places_findplace_api(coords,n=10):
	radius = 500 #meters
	keys = ['name','formatted_address','geometry','id','place_id']
	results = []

	# empirical qtypes (existing): {'point_of_interest': 15, 'establishment': 15, 'school': 12, 'primary_school': 8, 'secondary_school': 3, 'fire_station': 1, 'courthouse': 1, 'local_government_office': 1}

	for qtype in ['school','local_government_office','church','post_office']:
		print(f"Querying qtype {qtype}")
		# nearbyquery = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={coords[0]},{coords[1]}&radius={radius}&type={qtype}&key={apikey}"
		findplacequery = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={qtype}&inputtype=textquery&fields=formatted_address,name,geometry,id,place_id&locationbias=circle:1000@{coords[0]},{coords[1]}&key={apikey}"
		r = requests.get(findplacequery)
		r=r.json()
		print(r)
		# print("here1")
		# breakpoint()
		# print("here2")
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






def add_nearby_places(api = "nearby",nearbyto = "pollingLocations"):
	if nearbyto == "pollingLocations":
		filenames = get_poll_location_filenames()
	else:
		#this is just filenames for general data with polling locations and population points
		filenames = get_combined_results_filenames()
	# extra_headers = ['nearby_rank','name','dist','nearbylat','nearbylon']
	print(filenames)
	os.chdir(csv_combined_directory)
	for filename in filenames:
		print(f"PROCESSING {filename}")

		if api == "nearby":
			outfilename = "with_candidate_locations_nearbyto_" +nearbyto +"_"+ filename.split(".")[0] + ".csv"
		else:
			outfilename = "with_FINDPLACE_API_candidate_locations_nearbyto_" +nearbyto +"_" + filename.split(".")[0] + ".csv"
		poll_locations = recall_poll_locations(filename)
		extra_rows = []
		i = 0
		for i,row in enumerate(poll_locations):
			print(f"Getting nearby places for {row['pollingLocationName']}")
			if nearbyto== "pollingLocation":
				coords = (row.get('pollingLocationLatitude'),row.get('pollingLocationLongitude'))
			else:
				#this is just lat/lon of population point
				coords = (row.get('latitude'),row.get('longitude'))
			if api == "nearby":
				nearby = get_nearby_places(coords,20)
			else:
				nearby = get_nearby_places_findplace_api(coords,20)
			for nearbyplace in nearby:
				for k,v in row.items():
					nearbyplace[k] = v

				extra_rows.append(nearbyplace)
			if i > 10:
				break
		# breakpoint()
		if len(extra_rows) > 0:
			headers = extra_rows[0].keys()
			write_csv(outfilename,headers,extra_rows)
		else:
			print("NO EXTRA ROWS?!")




def get_place_info(row):
	coords = (row.get('pollingLocationLatitude'),row.get('pollingLocationLongitude'))
	pollingLocationName = row.get('pollingLocationName')
	pollingLocationAddress = row.get('pollingLocationAddress')
	pollingLocationCity = row.get('pollingLocationCity')
	pollingLocationState = row.get('pollingLocationState')
	pollingLocationZip = row.get('pollingLocationZip')
	# f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={coords[0]},{coords[1]}&radius={radius}&type={qtype}&key={apikey}"
	# all fields: https://developers.google.com/places/web-service/search#Fields
	q = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?inputtype=textquery&input={pollingLocationName}&locationbias=point:{coords[0]},{coords[1]}&fields=formatted_address,geometry,name,permanently_closed,place_id,plus_code,types&key={apikey}"
	r = requests.get(q)
	rdata = r.json()
	return rdata

def add_place_types():
	filenames = get_poll_location_filenames()
	# extra_headers = ['nearby_rank','name','dist','nearbylat','nearbylon']
	print(filenames)
	os.chdir(csv_combined_directory)
	headers = set()
	for filename in filenames:
		print(f"PROCESSING {filename}")
		outfilename = "with_place_types_" + filename.split(".")[0] + ".csv"
		poll_locations = recall_poll_locations(filename)
		i = 0
		for i,row in enumerate(poll_locations):
			#SHOULD NOT NEED THIS - ONLY FAILS WITHOUT FOR LAFAYETTE?!
			for k,v in row.items():
				headers.add(k)
			print(f"Getting place type for {row['pollingLocationName']}")
			place_query = get_place_info(row)
			if place_query['status'] == 'OK':
				place_info = place_query.get('candidates')[0]
				# row['place_type'] = place_info['types']
				# row['place_id'] = place_info['place_id']
				# row['place_name'] = place_info['place_id']
				for k,v in place_info.items():
					row[k] = v
					#SHOULD NOT NEED THIS - ONLY FAILS WITHOUT FOR LAFAYETTE?!
					headers.add(k)
			if i > 2:
				break
		# breakpoint()
		# headers = poll_locations[0].keys()
		write_csv(outfilename,headers,poll_locations)

def get_all_place_types():
	filenames = get_placetypes_filenames()
	os.chdir(csv_combined_directory)
	all_types = []
	for filename in filenames:
		print(f"PROCESSING {filename}")
		outfilename = "with_place_types_" + filename.split(".")[0] + ".csv"
		poll_locations = recall_poll_locations(filename)
		for row in poll_locations:
			types = row['types']
			# print(f"types is: {types}")
			if types is not None and types != "":
				# typereplace = types.replace("\'",'\"')
				# print(f"FIXED types is: {typereplace}")

				# print(f"types DECODED is: {json.loads(types)}")
				all_types.extend(json.loads(types.replace("\'",'\"')))

	return Counter(all_types)
	#eg: Counter({'point_of_interest': 15, 'establishment': 15, 'school': 12, 'primary_school': 8, 'secondary_school': 3, 'fire_station': 1, 'courthouse': 1, 'local_government_office': 1})

add_nearby_places(api = "nearby",nearbyto = "population")

# get_poll_locations()
# add_place_types()



# print(get_all_place_types())
# PROCESSING with_place_types_only_polling_locations_new_orleans_all_combined.csv
# PROCESSING with_place_types_only_polling_locations_shreveport_all_combined.csv
# PROCESSING with_place_types_only_polling_locations_lafayette_all_combined.csv
# PROCESSING with_place_types_only_polling_locations_baton_rouge_all_combined.csv
# Counter({'point_of_interest': 15, 'establishment': 15, 'school': 12, 'primary_school': 8, 'secondary_school': 3, 'fire_station': 1, 'courthouse': 1, 'local_government_office': 1})

