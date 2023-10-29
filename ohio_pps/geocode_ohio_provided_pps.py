import os
import csv
from utilities import get_headers, get_rows
from ohio_vf_config import interested_county_properties
from geocode_utilities import geocode_address

pps_directory = "/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio/precinct_pps_from_ohio_counties"


def get_filenames():
	filenames = os.listdir(pps_directory)
	filetoken = 'precinctcounts'
	filenames = [filename for filename in filenames if filetoken in filename]
	def get_county_names():
		return [county['NAME'] for county in interested_county_properties]
	def extract_county(filename):
		return filename.split(filetoken)[1].split('.')[0]
	county_names = get_county_names()
	filenames = [filename for filename in filenames if extract_county(filename) in county_names]
	return [f"{pps_directory}/{filename}" for filename in filenames]

def get_filenames_fake():
	return ["/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio/precinct_pps_from_ohio_counties/custom_precinctcountsFranklin.csv"]

def get_geocoded_filename(filename):
	current = filename.split("/")
	current[-1] = f"geocoded/{current[-1]}"
	return "/".join(current)


def get_lat_lon(individual):
	address = individual['RESIDENTIAL_ADDRESS1']
	city = individual['RESIDENTIAL_CITY']
	state = individual['RESIDENTIAL_STATE']
	lat,lon = geocode_address(address, city, state)
	return lat,lon


def geocode_pps():
	geocoded_addresses = {}
	for filename in get_filenames_fake():
		new_filename = get_geocoded_filename(filename)
		print(f"Processing {filename}")
		if not os.path.exists(new_filename):
			headers = get_headers(filename)
			rows = [dict(zip(headers,row)) for row in get_rows(filename)]

			for i, row in enumerate(rows):
				pollAddress = row['polladdress']
				if pollAddress in geocoded_addresses:
					lat,lon = geocoded_addresses[pollAddress]
				else:
					try:
						lat,lon = geocode_address(pollAddress, city="", state="")
					except Exception as e:
						print(f"ERROR ON ROW {i} OF FILE {filename}")
						lat = ""
						lon = ""
					geocoded_addresses[pollAddress] = (lat,lon)
				row['latitude'] = lat
				row['longitude'] = lon
			new_filename = get_geocoded_filename(filename)
			write_geocoded_to_csv(new_filename, rows)
		else:
			print(f"{new_filename} exists!")

def write_geocoded_to_csv(outfilename, rows):
	headers = rows[0].keys()
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)

geocode_pps()
