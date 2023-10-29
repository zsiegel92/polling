import csv
from datetime import datetime
import time

from config import georgia_results_dir as civic_query_results_dir
from geojson_utilities import extract_lon_lat
from shapefile_utilities import get_interested_metro_by_namelsad
from grid_utilities import get_grid_intersection_turfpy, measure_distance
from geocode_utilities import geocode_address, reverse_geocode_latlon
from census_utilities import get_census_data
from civic_api_utilities import get_elections, query_civic_api, state_abbrevs,allPlaceTypes
from census_codes import codes as census_codes, groups as census_groups
from metro_county_delineation import get_metro_intersect_state_by_nameLSAD


def get_pp_latlon(ppAddress,ppCity,ppState):
	lat,lon = geocode_address(ppAddress,ppCity,ppState)
	return lat,lon


def get_desired_election_id(metro=None,testing=False,customAPIkey=None):
	if testing:
		return 2000
	elections = get_elections(customAPIkey=customAPIkey)
	if metro is not None:
		metroname = metro['properties']['NAMELSAD']
		try:
			metro_states = metroname.rsplit(", ",1)[1].split()[0].split("-")
			# metro_state_fullnames = [None if (fn := state_abbrevs.get(abbrev,None)) is None else fn for abbrev in metro_states]
			metro_state_fullnames = [None if state_abbrevs.get(abbrev,None) is None else state_abbrevs.get(abbrev,None) for abbrev in metro_states]
			for election in elections:
				for fn in metro_state_fullnames:
					if fn.lower() in election['name'].lower():
						return election['id']
		except Exception:
			pass
	for election in elections:
		if 'General Election' in election['name']:
			return election['id']
	return None



def time_from_seconds(seconds):
	day = seconds // (24 * 3600)
	seconds = seconds % (24 * 3600)
	hour = seconds // 3600
	seconds %= 3600
	minutes = seconds // 60
	seconds %= 60
	return f"{day}d : {hour}hr : {minutes}m : {seconds}s"

def is_iterable(obj):
	return hasattr(obj,"__setitem__")

def is_already_processed(point):
	for placeType in allPlaceTypes:
		placeTypePrefix = placeType[:-1] #remove trailing "s"
		for k,v in point['properties'].items():
			if k.startswith(placeTypePrefix) and v: #must have actual data, not just reading from blank csv cell
				return True
	return False

def civicProcessPoints(featureCollection,unique_places,election_id,getCensus=False,waitPerPoint=0,lookToFuture=False,only_with_error=False,skipPoints=0,customAPIkey=None):
	totPoints = len(featureCollection['features'])
	start = time.time()
	newStart = start
	nPerReport = 100
	try:
		if not lookToFuture:
			# repeats the last two queries for good measure in case in the crash their data was corrupted
			last_processed_index = max(max([i for i, point in enumerate(featureCollection['features']) if is_already_processed(point)]) - 2,0)
		else:
			last_processed_index = 0
	except ValueError:
		last_processed_index = 0
	print(f"WILL START AT POINT {last_processed_index}")
	points = featureCollection['features']
	for i,point in enumerate(points):
		if only_with_error and point['properties']['civic_query_error'] not in (True,'True'):
			continue
		if i >= last_processed_index and i >= skipPoints:
			try:
				print(f"{i} : ",end="")
				civicProcessPoint(points,i,election_id,getCensus=getCensus,waitPerPoint=waitPerPoint,lookToFuture=lookToFuture,customAPIkey=customAPIkey)
				prunePoint(point,unique_places)
			except Exception as e:
				print(f"ERROR ON POINT {i}")
				print(e)
		else:
			print(f"ALREADY PROCESSED (or skipping) POINT {i}/{totPoints}")
		if i % nPerReport == 1:
			end = time.time()
			elapsed = round(end-start)
			remaining = round(((end-newStart)/ nPerReport) *(totPoints - i))
			newStart = end
			print(f"POINT {i}/{totPoints} (@{time_from_seconds(elapsed)} so far, ETA {time_from_seconds(remaining)})")




def prunePoint(point,unique_places):
	props = point['properties']
	for placeType in allPlaceTypes:
		number_new_of_placetype = 0
		if f"_{placeType}" in props:
			props[f"numberPlaces_total_{placeType}"] = len(props[f"_{placeType}"])
			places = props[f"_{placeType}"]
			for place in places:
				placeName = place['address']['locationName']
				if placeName not in unique_places[placeType]:
					unique_places[placeType][placeName] = place # uniquify on "locationName"
					number_new_of_placetype += 1
			del props[f"_{placeType}"]
		else:
			props[f"numberPlaces_total_{placeType}"] = 0
		props[f"numberPlaces_unseen_{placeType}"] = number_new_of_placetype
		if number_new_of_placetype > 0:
			print(f" {number_new_of_placetype} new {placeType} ",end="")
	print("\n",end="")

def append_votingData(props,votingData):
	for placeType in allPlaceTypes:
		if placeType in votingData:
			props[f"_{placeType}"] = votingData[placeType] # COULD CREATE MEMORY BLOAT #possibly only add unseen places
			samplePlace = votingData[placeType][0]
			placeTypePrefix = placeType[:-1] #remove trailing "s"
			# only get string variables
			for place_key,place_info in samplePlace.items():
				if not is_iterable(place_info):
					props[f"{placeTypePrefix}{place_key.capitalize()}"] = place_info
			if "address" in samplePlace:
				for special_part_key,special_part in samplePlace["address"].items():
					props[f"{placeTypePrefix}Address{special_part_key.capitalize()}"] = special_part
			if "sources" in samplePlace:
				for i, special_dict in enumerate(samplePlace["sources"]):
					for special_part_key,special_part in special_dict.items():
						props[f"{placeTypePrefix}Sources{i}{special_part_key.capitalize()}"] = special_part

def append_votingData_nonredundant(formatted_address,election_id,points,point_ind,waitPerPoint=0,lookToFuture=False,customAPIkey=None):
	new_point_props = points[point_ind]['properties']
	if lookToFuture:
		for placeType in allPlaceTypes:
			placeTypeKey = placeType[:-1] #remove trailing "s"
			for k,v in new_point_props.items():
				if k.startswith(placeTypeKey) and v:
					print("ALREADY HAVE CIVIC DATA FOR POINT!",end="")
					return
	new_point_props['_seen_address'] = False
	for i,point in enumerate(points):
		if i == point_ind: #don't compare point to itself
			if lookToFuture:
				# if looking to the future (postprocessing), allow future points to fill in details.
				continue
			else:
				break
		if point['properties'].get('queryAddress')==formatted_address: # and not point['properties'].get('_seen_address') #only copy from OG source
			# is not a new address
			new_point_props['_seen_address'] = True
			for k,v in point['properties'].items():
				if k not in new_point_props and v:
					new_point_props[k] = v
	if new_point_props['_seen_address']:
		print(f"HAVE ALREADY SEEN ADDRESS {formatted_address}",end="")
		return
	# is a new address
	votingData = query_civic_api(formatted_address,election_id,customAPIkey=customAPIkey)
	time.sleep(waitPerPoint)
	if all([placeType not in votingData for placeType in allPlaceTypes]):
		print(f"No civic data returned for address: {formatted_address}",end="")
		if 'error' in votingData:
			new_point_props['civic_query_error'] = True # for possible post-processing
		return
	else:
		new_point_props['civic_query_error'] = False
		print("Civic data returned!",end="")
	append_votingData(new_point_props,votingData)


def append_census_data(point_properties,lat,lon):
	try:
		census_data = get_census_data(lat,lon)
		for k, v in census_data.items():
			point_properties[k] = v
	except Exception as e:
		print("ERROR GETTING CENSUS DATA")
		print(e)


def civicProcessPoint(points,point_ind,election_id,getCensus=False,waitPerPoint=0,lookToFuture=False,customAPIkey=None):
	p = points[point_ind]
	try:
		props = p['properties']
		lon,lat = extract_lon_lat(p)
		props['latitude'] = lat
		props['longitude'] = lon
		for k,v in census_codes.items():
			props[v] = ''
		if getCensus:
			append_census_data(props,lat,lon)
		if not props.get('queryAddress',None):
			formatted_address = reverse_geocode_latlon(lat,lon)
			props['queryAddress'] = formatted_address
	except Exception as e2:
		print("DID NOT QUERY CIVIC API")
		raise e2
	else:
		append_votingData_nonredundant(props['queryAddress'],election_id,points,point_ind,waitPerPoint=waitPerPoint,lookToFuture=lookToFuture,customAPIkey=customAPIkey)

def initialize_unique_places():
	return {placeType : {} for placeType in allPlaceTypes}


def get_point_dict_rows(featureCollection):
	rows = [feature['properties'] for feature in featureCollection['features']]
	return rows

def get_outfilename(datatype,metroname,spacing_km,translate_north_and_east=0,stateFips=None):
	trans = f"translate_{translate_north_and_east}km_" if translate_north_and_east > 0 else ""
	state_intersected = f"stateIntersect_{stateFips}_" if stateFips else ""
	timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M")
	return f"{civic_query_results_dir}/civicAPI_{datatype}_{metroname.replace(' ','_')}_spacing_{spacing_km}km_{trans}{state_intersected}{timestamp}.csv"

def get_points_outfilename(metroname,spacing_km,translate_north_and_east=0,stateFips=None):
	return get_outfilename("points",metroname,spacing_km,translate_north_and_east=0,stateFips=None)

def get_unique_places_outfilename(metroname,spacing_km,translate_north_and_east=0,stateFips=None):
	return get_outfilename("unique_places",metroname,spacing_km,translate_north_and_east=0,stateFips=None)

def write_points_rows(grid,metroname,spacing_km,postprocessed=False,translate_north_and_east=0,stateFips=None):
	rows = get_point_dict_rows(grid)
	outfilename = get_points_outfilename(metroname,spacing_km,translate_north_and_east=translate_north_and_east,stateFips=stateFips)
	if postprocessed:
		directory,basefilename = outfilename.rsplit("/",1)
		outfilename = f"{directory}/postprocessed/{basefilename}"
	headers = []
	for row in rows:
		headers.extend(k for k in row.keys() if k not in headers)
	headers = [k for k in headers if not k.startswith("_")]
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)

def get_unique_places_dict_rows(unique_places):
	rows = []
	for placeType in unique_places:
		placeTypeName = placeType[:-1] #remove trailing "s"
		for locationName,place in unique_places[placeType].items():
			try:
				row = {place_key : place_info for place_key,place_info in place.items() if not is_iterable(place_info)}
				row['placeType'] = placeTypeName
				if "address" in place:
					for special_part_key,special_part in place["address"].items():
						row[f"address{special_part_key.capitalize()}"] = special_part
				if "sources" in place:
					for i, special_dict in enumerate(place["sources"]):
						for special_part_key,special_part in special_dict.items():
							row[f"sources{i}{special_part_key.capitalize()}"] = special_part
				rows.append(row)
			except Exception as e:
				print("LOST A ROW!")
	return rows

def write_unique_places(unique_places,metroname,spacing_km,postprocessed=False,translate_north_and_east=0,stateFips=None):
	rows = get_unique_places_dict_rows(unique_places)
	outfilename = get_unique_places_outfilename(metroname,spacing_km,translate_north_and_east=translate_north_and_east,stateFips=stateFips)
	if postprocessed:
		directory,basefilename = outfilename.rsplit("/",1)
		outfilename = f"{directory}/postprocessed/{basefilename}"
	headers = []
	for row in rows:
		headers.extend(k for k in row.keys() if k not in headers)
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)


def restart_after_crash(customAPIkey=None):
	civicProcessPoints(grid,unique_places, election_id,getCensus=False,waitPerPoint=waitPerPoint,customAPIkey=customAPIkey)
	write_points_rows(grid,**options)
	write_unique_places(unique_places,**options)

def restart_with_new_wait(newWaitPerPoint,customAPIkey=None):
	global waitPerPoint
	waitPerPoint = newWaitPerPoint
	civicProcessPoints(grid,unique_places, election_id,getCensus=False,waitPerPoint=newWaitPerPoint,customAPIkey=customAPIkey)
	write_points_rows(grid,**options)
	write_unique_places(unique_places,**options)



def reconstruct_grid_and_unique_from_csv(grid_filename,unique_filename):
	unique_places = initialize_unique_places()

	with open(f"{civic_query_results_dir}/to_be_postprocessed/{unique_filename}") as csvfile:
		reader = csv.DictReader(csvfile)
		unique_places_raw = [row for row in reader]
	for row in unique_places_raw:
		placeName = row['addressLocationname']
		placeType = row['placeType']
		placeTypeKey = placeType + "s" # trailing "s" eg "pollingLocation" -> "pollingLocations"
		unique_places[placeTypeKey][placeName] = row

	with open(f"{civic_query_results_dir}/to_be_postprocessed/{grid_filename}") as csvfile:
		reader = csv.DictReader(csvfile)
		grid_raw = [row for row in reader]

	grid = {'features' : [{"geometry" : {"coordinates" : (row['longitude'], row['latitude'])},"properties" : row } for row in grid_raw] }
	return grid,unique_places

def visualize_grid(metro,grid):
	from plotly_utilities import generate_plot_of_grid, save_plot
	fig = generate_plot_of_grid(metro,grid)
	metrotitle = metro['properties']['NAMELSAD'].replace(" ","_")
	outfilename = f"{civic_query_results_dir}/figures/grid_{metrotitle}.html"
	save_plot(fig,outfilename)
	print(f"MAP SAVED TO {outfilename}")
# CURRENT ELECTIONS:
# https://docs.google.com/spreadsheets/d/1ZNcwc9U6dYNzVOQmheECnnZDtH6zxdwbkT9Ns8iOX9k/edit#gid=247788148
# eg.
# metro = get_interested_metro_by_namelsad("Columbus, OH Metro Area")
if __name__=="__main__":
	options = {
		"spacing_km" : 10,
		"metroname" : "Chattanooga, TN-GA Metro Area",
		"stateFips" : "13", # Set to None or omit if not intersecting with state
		"translate_north_and_east" : 0
	}
	waitPerPoint = 0.55

	customAPIkey = None
	# metro = get_interested_metro_by_namelsad(options['metroname'])
	metro = get_metro_intersect_state_by_nameLSAD(options['metroname'],options.get('stateFips'))
	grid = get_grid_intersection_turfpy(metro,km=options['spacing_km'],translate_north_and_east=options['translate_north_and_east'])
	visualize_grid(metro,grid) # Saves a map to ../../data/results/civic_results/Georgia/figures. Can comment this out.
	num_points = len(grid['features'])
	print(f"There are {num_points} points to query!")
	election_id = get_desired_election_id(customAPIkey)
	skipPoints = input("Press ENTER or enter a number of points to skip.")
	try:
		if skipPoints:
			skipPoints = int(skipPoints)
		else:
			skipPoints = 0
		print(f"Skipping {skipPoints}/{num_points} points!")
	except:
		raise
	input("READY TO MAKE QUERIES?")
	unique_places = initialize_unique_places()
	civicProcessPoints(grid,unique_places, election_id,getCensus=False,waitPerPoint=waitPerPoint,skipPoints=skipPoints)
	write_points_rows(grid,**options)
	write_unique_places(unique_places,**options)
	# TO RESTART WITH A NEW API KEY:
	# restart_after_crash(customAPIkey="AIzaSyDUiNs5xvLJU8oBPtbxfjg-CAju4MJBj-E")
	# TO RESTART WITH A NEW WAIT TIME AND API KEY:
	# restart_with_new_wait(newWaitPerPoint=0.7,customAPIkey="AIzaSyDUiNs5xvLJU8oBPtbxfjg-CAju4MJBj-E")

	# postProcessing = True
	# if postProcessing:
	# 	options = {
	# 		"spacing_km" : 1,
	# 		"metroname" : "Charlotte-Concord-Gastonia, NC-SC Metro Area",
	# 	}
	# 	waitPerPoint = 0.55
	# 	customAPIkey = None
	# 	getCensus = False
	# 	# grid_filename 	= "civicAPI_points_New_York-Newark-Jersey_City,_NY-NJ-PA_Metro_Area_spacing_1km_26_10_2020_14_56.csv"
	# 	# unique_filename 	= "civicAPI_unique_places_New_York-Newark-Jersey_City,_NY-NJ-PA_Metro_Area_spacing_1km_26_10_2020_14_56.csv"
	# 	grid_filename 		= "civicAPI_points_Charlotte-Concord-Gastonia,_NC-SC_Metro_Area_spacing_1km_26_10_2020_19_34.csv"
	# 	unique_filename 	= "civicAPI_unique_places_Charlotte-Concord-Gastonia,_NC-SC_Metro_Area_spacing_1km_26_10_2020_19_34.csv"
	# 	grid, unique_places = reconstruct_grid_and_unique_from_csv(grid_filename,unique_filename)
	# 	election_id = get_desired_election_id()
	# 	input("READY TO MAKE QUERIES?  (postprocessing)")
	# 	# use "only_with_error=True" only if data was collected when setting the `civic_query_error` property.
	# 	civicProcessPoints(grid,unique_places, election_id,getCensus=getCensus,waitPerPoint=waitPerPoint,lookToFuture=True,only_with_error=True,customAPIkey=customAPIkey)
	# 	write_points_rows(grid,postprocessed=True,**options)
	# 	write_unique_places(unique_places,postprocessed=True,**options)
	

### GEORGIA METRO AREAS, ordered by population, from georgia_metro_population_density.csv
# Atlanta-Sandy Springs-Alpharetta, GA Metro Area
# Augusta-Richmond County, GA-SC Metro Area
# Chattanooga, TN-GA Metro Area
# Savannah, GA Metro Area
# Columbus, GA-AL Metro Area
# Macon-Bibb County, GA Metro Area
# Athens-Clarke County, GA Metro Area
# Gainesville, GA Metro Area
# Warner Robins, GA Metro Area
# Valdosta, GA Metro Area
# Albany, GA Metro Area
# Dalton, GA Metro Area
# Brunswick, GA Metro Area
# LaGrange, GA-AL Micro Area
# Rome, GA Metro Area
# Hinesville, GA Metro Area
# Statesboro, GA Micro Area
# Jefferson, GA Micro Area
# Dublin, GA Micro Area
# Calhoun, GA Micro Area
# Waycross, GA Micro Area
# St. Marys, GA Micro Area
# Milledgeville, GA Micro Area
# Douglas, GA Micro Area
# Moultrie, GA Micro Area
# Cornelia, GA Micro Area
# Thomasville, GA Micro Area
# Cedartown, GA Micro Area
# Tifton, GA Micro Area
# Vidalia, GA Micro Area
# Americus, GA Micro Area
# Jesup, GA Micro Area
# Eufaula, AL-GA Micro Area
# Bainbridge, GA Micro Area
# Thomaston, GA Micro Area
# Toccoa, GA Micro Area
# Summerville, GA Micro Area
# Cordele, GA Micro Area
# Fitzgerald, GA Micro Area


