import os
import csv
from utilities import get_headers, get_rows
from ohio_vf_config import interested_metro_properties,interested_county_properties
from geocode_utilities import geocode_address

provided_pps_directory = "/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio/precinct_pps_from_ohio_counties/geocoded"

found_pps_directory = "/Users/zach/Documents/UCLA_classes/research/Polling/metro_shapefiles/results"

def get_rowdicts(filename):
	headers = get_headers(filename)
	return [dict(zip(headers,row)) for row in get_rows(filename)]


def get_provided_filenames():
	filenames = os.listdir(provided_pps_directory)
	filetoken = 'precinctcounts'
	filenames = [filename for filename in filenames if filetoken in filename]
	def get_county_names():
		return [county['NAME'] for county in interested_county_properties]
	def extract_county(filename):
		return filename.split(filetoken)[1].split('.')[0]
	county_names = get_county_names()
	filenames = [filename for filename in filenames if extract_county(filename) in county_names]
	return [f"{provided_pps_directory}/{filename}" for filename in filenames]


def deduplicate_provided_pps():
	all_pps = []
	identifier_keys = ('precname','pollplace','polladdress')
	identifier_sets = {k : [] for k in identifier_keys}
	for filename in get_provided_filenames():
		print(f"Processing {filename}")
		rows =  get_rowdicts(filename)
		for row in rows:
			duplicate = False
			for k, identifier_set in identifier_sets.items():
				try:
					if row[k] != '' and row[k] in identifier_set:
						duplicate = True
						break
					else:
						identifier_set.append(row[k])
				except:
					print("ERROR")
					print(row.keys())
					print(row)
			if not duplicate:
				all_pps.append(row)

	return all_pps

def write_full_provided_pps_to_csv(rows):
	metro_name = interested_metro_properties['NAME'].replace(' ','_')
	headers = rows[0].keys()
	outfilename = f"{provided_pps_directory}/deduplicated_provided_pps_{metro_name}.csv"
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)


def get_and_write_deduplicated_provided_pps():
	all_pps = deduplicate_provided_pps()
	write_full_provided_pps_to_csv(all_pps)
	return all_pps




def get_found_filenames():
	metro_base_name = interested_metro_properties['NAME'].split(",")[0]
	pps_filenames = [filename for filename in os.listdir(found_pps_directory) if metro_base_name in filename and "POLLING.csv" in filename]
	return [f"{found_pps_directory}/{filename}" for filename in pps_filenames]

def get_combined_and_deduplicated_found_pps():
	all_pps = []
	identifier_keys = ('name','pollplace','address')
	identifier_sets = {k : [] for k in identifier_keys}
	for filename in get_found_filenames():
		print(f"Processing {filename}")
		rows =  get_rowdicts(filename)
		for row in rows:
			duplicate = False
			for k, identifier_set in identifier_sets.items():
				try:
					if row[k] != '' and row[k] in identifier_set:
						duplicate = True
						break
					else:
						identifier_set.append(row[k])
				except:
					print("ERROR")
					print(row.keys())
					print(row)
			if not duplicate:
				all_pps.append(row)
	return all_pps


def write_full_found_pps_to_csv(rows):
	metro_name = interested_metro_properties['NAME'].replace(' ','_')
	headers = rows[0].keys()
	outfilename = f"{found_pps_directory}/deduplicated_found_pps_{metro_name}.csv"
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)


def get_and_write_deduplicated_found_pps():
	all_pps = get_combined_and_deduplicated_found_pps()
	write_full_found_pps_to_csv(all_pps)
	return all_pps


if __name__=="__main__":
	all_provided_pps = get_and_write_deduplicated_provided_pps()
	all_found_pps = get_and_write_deduplicated_found_pps()

