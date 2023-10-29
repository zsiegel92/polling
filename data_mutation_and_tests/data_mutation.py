import os
from collections import OrderedDict
from utilities import get_headers,get_rows,write_csv


csvdirectory="/Users/zach/Documents/UCLA_classes/research/Polling/results"
csv_combined_directory=f"{csvdirectory}/combined"
cities =["lafayette","baton_rouge","shreveport","new_orleans"]

def get_results_filenames():
	filenames = [filename for filename in os.listdir(csvdirectory) if filename.endswith(".csv")]
	return filenames


def change_filenames():
	filenames = get_results_filenames()
	os.chdir(csvdirectory)
	for filename in filenames:
		if (substr:="Polling_Stations_") in filename:
			pass
		elif (substr:="_Polling_Stations") in filename:
			pass
		newfilename = filename.replace(substr,"").lower()
		print(f"OLD: {filename} ---- NEW: {newfilename}")
		os.rename(filename,newfilename)
	os.chdir("..")


def concatenate_csvs_for_cities():
	os.chdir(csvdirectory)
	for city_name in cities:
		filenames = [filename for filename in get_results_filenames() if city_name.lower() in filename.lower() or city_name.replace(" ","_") in filename.lower()]

		print(f"PROCESSING {city_name}: combining {len(filenames)} csv files.")

		outfilename = city_name + "_all_combined.csv"
		outfilename = f"{csv_combined_directory}/{outfilename}"

		headings = []
		data_with_polling_location = []
		data_without_polling_location = []
		for filename in filenames:
			print(f"Reading file {filename}")
			headings.append(get_headers(filename))
			rows = get_rows(filename)
			data_with_polling_location.extend(rows['with_polling_location'])
			data_without_polling_location.extend(rows['without_polling_location'])
		data = data_with_polling_location + data_without_polling_location

		print(f"RESULTS {city_name}: there are {len(data_with_polling_location)} locations with polling location and {len(data_without_polling_location)} without. Writing to file {outfilename}.\n\n")
		write_csv(outfilename,headings[0],data)


# print(get_results_filenames())
# change_filenames()
# concatenate_csvs_for_cities()
