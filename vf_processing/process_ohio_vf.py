import os
import csv
from datetime import datetime
# from shapely.geometry import Point, Polygon, box, asShape, asPoint
from geopy.distance import distance
import numpy as np

from utilities import get_headers, get_rows
from ohio_vf_config import vf_directory, vf_full_paths, interested_metro_properties, interested_county_properties

from county_numbers import county_numbers
from geocode_utilities import geocode_address
from process_ohio_shapefiles import add_geo_context, get_interested_metros, plot_shapes
from census_utilities import get_census_data
from get_polling_places import pps


number_desired_geocoded = 200

def get_county_number(county_properties):
	return county_numbers[county_properties['NAME']]

def get_interested_individuals():
	# interested_county_codes = [ county['COUNTYFP'].lstrip("0") for county in interested_county_properties]
	interested_county_numbers = [ str(get_county_number(county)) for county in interested_county_properties]

	print(f"Getting individuals with county code(s) {interested_county_numbers}")

	interested_indiv = []
	for file_ind, vf_full_filename in enumerate(vf_full_paths):
		headers = get_headers(vf_full_filename)
		print(f"Getting individuals from VF {vf_full_filename}")
		selected = 0
		omitted = 0

		for row in get_rows(vf_full_filename):
			# if (rowdict := dict(zip(headers,row)))['COUNTY_NUMBER'] in interested_county_numbers:
			if row[1] in interested_county_numbers:
				rowdict = dict(zip(headers,row))
				interested_indiv.append(rowdict)
				selected += 1
			else:
				omitted += 1
		print(f"{selected}/{selected + omitted} selected from file {file_ind+1}/{len(vf_full_paths)} ({vf_full_filename})")
	print(f"TOTAL: {len(interested_indiv)} individuals")
	return interested_indiv





def get_county_codes():
	return [ str.lstrip(county['COUNTYFP'],"0") for county in interested_county_properties]

def get_county_names():
	return [f"{county['NAME']}{county['GEOID']}" for county in interested_county_properties]



def generate_timestamp():
	# weights_label = "_".join(map(str,weights))
	hour = int(datetime.now().strftime("%H"))
	minRounded = round(int(datetime.now().strftime("%M"))/30)*30
	hourPlus = minRounded // 60
	minFinal = minRounded % 60
	hourFinal = hour + hourPlus
	datestamp = datetime.now().strftime("%Y_%m_%d")
	timestamp = f"{datestamp}_{hourFinal}_{minFinal}"
	return timestamp

def write_geocoded_to_csv(rows):
	headers = rows[0].keys()
	timestamp = generate_timestamp()
	metroname = interested_metro_properties['NAMELSAD']
	metroname = metroname.replace(' ','_')
	outfilename = f"{vf_directory}/augmented_{metroname}_{timestamp}.csv"
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)


def get_most_recent_geocoded_filename():
	os.chdir(vf_directory)
	filenamestart = f"augmented_"
	metroname = interested_metro_properties['NAMELSAD']
	metroname = metroname.replace(' ','_')
	filenames = [filename for filename in os.listdir() if filename.startswith(filenamestart) and filename.endswith(".csv") and metroname in filename]
	filenames.sort()
	filename = filenames[-1]
	return filename

def read_geocoded_from_csv():
	try:
		filename = get_most_recent_geocoded_filename()
		print(f"Getting geocoded individuals from {filename}")
		# interested_county_numbers = [ str(get_county_number(county)) for county in interested_county_properties]
		geocoded_indiv = []
		headers = get_headers(filename)
		geocoded_indiv = [dict(zip(headers,row)) for row in get_rows(filename)]
		return geocoded_indiv
	except:
		return []

def get_lat_lon(individual):
	address = individual['RESIDENTIAL_ADDRESS1']
	city = individual['RESIDENTIAL_CITY']
	state = individual['RESIDENTIAL_STATE']
	lat,lon = geocode_address(address, city, state)
	return lat,lon

def get_individual_census_data(individual):
	lat = individual['latitude']
	lon = individual['longitude']
	census_data = get_census_data(lat,lon)
	return census_data

def augment_individual(individual,pps):
	lat,lon = get_lat_lon(individual)
	individual['latitude'] = lat
	individual['longitude'] = lon
	census_data = get_individual_census_data(individual)
	individual.update(census_data)
	closest_pp = get_closest_pp(individual,pps)
	individual['distance_to_nearest_pp'] = distance((lat,lon),closest_pp).ft
	individual['closest_pp_lat'] = closest_pp[0]
	individual['closest_pp_lng'] = closest_pp[1]

def get_closest_pp(individual,pps):
	lat = individual['latitude']
	lon = individual['longitude']
	closest_pp = pps[np.argmin([distance((lat,lon),(pp['lat'],pp['lng'])) for pp in pps])]
	return closest_pp['lat'],closest_pp['lng']

def augment_individual_thrifty(basic_individual,augmented_individuals,pps):
	augmented = individual_augmented(basic_individual,augmented_individuals)
	if augmented:
		for k in augmented:
			basic_individual[k] = augmented[k]
		return True
	else:
		augment_individual(basic_individual,pps)
		return False

def individual_augmented(basic_individual,augmented_individuals):
	basic_kv = tuple(basic_individual.items())
	for augmented_individual in augmented_individuals:
		# if all( v == augmented_individual[k] for (k,v) in basic_kv):
		if all( v == augmented_individual[k] for (k,v) in basic_kv):
			return augmented_individual
	return False



def get_geocoded(individuals):
	return [individual for individual in individuals if individual.get('latitude') and individual.get('longitude')]

def get_augmented_with_max_new_requests(basic_individuals,augmented_individuals,pps,max_requests=10):
	num_requests = 0
	for basic_individual in basic_individuals:
		if not augment_individual_thrifty(basic_individual,augmented_individuals,pps):
			num_requests += 1
		if num_requests >= max_requests:
			break
	return get_geocoded(basic_individuals)




if __name__ == "__main__":
	metros = get_interested_metros()
	individuals = get_interested_individuals()
	previously_geocoded = read_geocoded_from_csv()
	geocoded = get_augmented_with_max_new_requests(individuals,previously_geocoded,pps,max_requests=10)
	write_geocoded_to_csv(geocoded)
# [individual for individual in individuals if individual['FIRST_NAME'] == 'MARGARET' and individual['LAST_NAME'] == 'PETCHAL']



# def get_all_vf_rows(filename):
# 	lines = []
# 	print(f"Getting individuals from VF {filename}")
# 	for line, number_errors in safeCSV(filename):
# 		lines.append(line)
# 	print(f"There were {number_errors} errors in {filename}")
# 	csv_reader = csv.DictReader(lines,fieldnames=get_vf_headers(filename))
# 	print(f"Converting rows to dictionary")
# 	rows = [row for row in csv_reader]
# 	print(f"Done converting rows to dictionary")
# 	return rows
# def get_interested_individuals(metro_properties):
# 	print(f"Getting individuals with county code {metro_properties['county_codes']}")
# 	interested_indiv = []
# 	for file_ind, vf_full_filename in enumerate(vf_full):
# 		all_rows = get_all_vf_rows(vf_full_filename)
# 		interested_indiv.extend([row for row in all_rows if row['COUNTY_NUMBER'] in metro_properties['county_codes']])
# 		print(f"{len(interested_indiv)}/{len(all_rows)} selected from file {file_ind}/{len(vf_full)} ({vf_full_filename})")
# 	return interested_indiv

# def get_points_in_city(region,rows):
# 	region_geom = asShape(region['geometry'])
# 	region['individuals'] = []
# 	for row in rows:
# 		lat = row['geometry']['lat']
# 		lon = row['geometry']['lon']
# 		if region_geom.contains(asPoint((lat,lon))):
# 			row['city'] = region
# 			region['individuals'].append(row)


