import os
import csv

# The first should be the folder with all the output _points_ and _unique_places_ csv files
# The second can be anything, including "/Users/zach/Desktop"
folder_containing_results = "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/Polling/Data/results/general2020_points_and_polling_places"
ouptut_dir = "/Users/zach/Documents/UCLA_classes/research/Polling/data/results/number_of_query_results_per_metro"

def get_returned_numbers(filepath):
	with open(filepath) as csvfile:
		reader = csv.DictReader(csvfile)
		grid_raw = [row for row in reader]
	columnKeys = ["numberPlaces_total_pollingLocations", "numberPlaces_total_earlyVoteSites", "numberPlaces_total_dropOffLocations"]
	return {k : [int(s) for s in sorted(list(set([row[k] for row in grid_raw if row[k] != '' and row[k] is not None])))]  for k in columnKeys}


def write_results(cities):
	headers = sorted(list(set([k for city in cities for k in city])))
	with open(f"{ouptut_dir}/returned_queries.csv","w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(cities)

def get_metroname_from_filename(filename):
	return filename.split("civicAPI_points_",1)[1].split("_spacing_")[0]

if __name__ == "__main__":
	
	filenames = os.listdir(folder_containing_results)
	cities = []
	for filename in filenames:
		if "civicAPI_points" in filename:
			metroname = get_metroname_from_filename(filename)
			print(metroname)
			city = get_returned_numbers(f"{folder_containing_results}/{filename}")
			city['metroname'] = metroname
			cities.append(city)
	cities = sorted(cities,key = lambda city: (len(city['numberPlaces_total_pollingLocations']),city['numberPlaces_total_pollingLocations']) )
	write_results(cities)
