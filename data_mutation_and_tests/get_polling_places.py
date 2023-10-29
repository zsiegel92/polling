import os
from config import polling_place_directory,interested_metro_properties
from utilities import get_headers, get_rows


def get_polling_place_filenames():
	metroname = interested_metro_properties['NAMELSAD']
	os.chdir(polling_place_directory)
	filenames = [filename for filename in os.listdir() if metroname in filename and filename.endswith("POLLING.csv")]
	return filenames

def get_polling_places():
	filenames = get_polling_place_filenames()
	rows = []
	for filename in filenames:
		full_filename = f"{polling_place_directory}/{filename}"
		headers = get_headers(full_filename)
		rows.extend([dict(zip(headers,row)) for row in get_rows(filename)])
	unique_addresses = [row['address'] for row in rows]
	unique_rows = []
	for unique_address in unique_addresses:
		for row in rows:
			if row['address'] == unique_address:
				unique_rows.append(row)
				break
	return unique_rows

pps = get_polling_places()

# polling_location_info = ['pollingLocationLatitude', 'pollingLocationLongitude', 'pollingLocationName', 'pollingLocationAddress', 'pollingLocationCity', 'pollingLocationState', 'pollingLocationZip', 'pollingLocationNotes', 'pollingLocationHours']

