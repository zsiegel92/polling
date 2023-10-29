import os
from utilities import get_headers,get_rows,write_csv
from geopy.distance import distance


csvdirectory="/Users/zach/Documents/UCLA_classes/research/Polling/results"
csv_combined_directory=f"{csvdirectory}/combined"
cities =["lafayette","baton_rouge","shreveport","new_orleans"]

def get_combined_results_filenames():
	filenames = [filename for filename in os.listdir(csv_combined_directory) if filename.endswith(".csv") and not ("_with_closest_info" in filename)]
	return filenames

def test_whether_closest():
	filenames = get_combined_results_filenames()
	print(filenames)
	os.chdir(csv_combined_directory)
	polling_location_info = ['pollingLocationLatitude', 'pollingLocationLongitude', 'pollingLocationName', 'pollingLocationAddress', 'pollingLocationCity', 'pollingLocationState', 'pollingLocationZip', 'pollingLocationNotes', 'pollingLocationHours']
	extra_headers = ["is_assigned_closest","true_closest_name","true_closest_straightline","true_closest_lat","true_closest_lon"]
	for filename in filenames:
		print(f"PROCESSING {filename}")
		outfilename = filename.split(".")[0] + "_with_closest_info.csv"
		num_closer = 0
		with_errors = 0
		headers= get_headers(filename)
		full_headers = headers + extra_headers

		rows = get_rows(filename) #rows['with_polling_location'],rows['without_polling_location'],rows['all']
		pollrows = rows['with_polling_location']
		nonpollrows = rows['without_polling_location']
		pollrows_with_closest_location = []
		pollrows_without_closest_location = []

		poll_locations = []
		pollingLocationNames = []
		for row in pollrows:
			if (rowname := row.get('pollingLocationName')) not in pollingLocationNames:
				pollingLocationNames.append(rowname)
				poll_locations.append({key:row.get(key) for key in polling_location_info})
		for row in pollrows:
			coords = (row.get('latitude'),row.get('longitude'))
			try:
				assigned_dist = float(row['distance']) #feet
			except:
				#ERROR - DISTANCE FIELD BLANK
				with_errors += 1
				for key in extra_headers:
					row[key] = "ERROR"
				pollrows_with_closest_location.append(row)
				continue
			is_assigned_closest = True
			for loc in poll_locations:
				poll_coords = (loc.get('pollingLocationLatitude'),loc.get('pollingLocationLongitude'))
				dist = distance(coords,poll_coords).ft
				if dist < assigned_dist:
					is_assigned_closest = False
					true_closest = dist
					row['is_assigned_closest']=False
					row['true_closest_name'] = loc['pollingLocationName']
					row['true_closest_straightline'] = dist
					row['true_closest_lat'] = loc['pollingLocationLatitude']
					row['true_closest_lon'] = loc['pollingLocationLongitude']
			if not is_assigned_closest:
				num_closer += 1
				pollrows_without_closest_location.append(row)
			else:
				pollrows_with_closest_location.append(row)

		data = pollrows_without_closest_location + pollrows_with_closest_location + nonpollrows
		write_csv(outfilename,full_headers,data)
		# print(f"Length pollrows: {len(pollrows)}. Length pollrows_with_closest_location: {len(pollrows_with_closest_location)}. Length pollrows_without_closest_location: {len(pollrows_without_closest_location)}. Length nonpollrows: {len(nonpollrows)}. Length rows[all]: {len(rows['all'])}")
		print(f"RESULTS {filename}: There are {num_closer} locations with closer polling locations than their assigned location and {with_errors} without a valid assigned distance. Length data: {len(data)}\n\n")


test_whether_closest()
