import os
import csv
import subprocess
import json
import requests
import folium
import matplotlib
import matplotlib.pyplot as plt

from config import geocoding_api_key
from utilities import get_headers, get_rows
from ohio_vf_config import interested_metro_properties
from geocode_utilities import geocode_address



provided_pps_directory = "/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio/precinct_pps_from_ohio_counties/geocoded"

found_pps_directory = "/Users/zach/Documents/UCLA_classes/research/Polling/metro_shapefiles/results"

def get_filenames():
	metro_name = interested_metro_properties['NAME'].replace(' ','_')
	found_filename = [filename for filename in os.listdir(found_pps_directory) if "deduplicated" in filename and metro_name in filename][0]
	provided_filename = [filename for filename in os.listdir(provided_pps_directory) if "deduplicated" in filename and metro_name in filename][0]
	return found_filename, provided_filename

def get_n_colors(number_desired_colors):
	if number_desired_colors < 10:
		default = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
		colors = default[0:number_desired_colors]
	else:
		# https://matplotlib.org/tutorials/colors/colormaps.ht
		cmap = plt.cm.get_cmap('nipy_spectral',number_desired_colors)
		colors = [matplotlib.colors.rgb2hex(cmap(i)) for i in range(number_desired_colors)]
	return colors


def add_geo_context(pp):
	try:
		lat = pp['lat']
		lon = pp['lng']
	except Exception:
		lat = pp['latitude']
		lon = pp['longitude']
	url=f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={geocoding_api_key}"
	data = requests.get(url).json()
	if 'results' not in data:
		print('Error retrieving reverse geocoding for city')
	else:
		result = data['results'][0]
		address_components = result['address_components']
		for component in address_components:
			if "administrative_area_level_1" in component['types']:
				pp['STATELONG'] = component['long_name']
				pp['STATESHORT'] = component['short_name']
			if "administrative_area_level_2" in component['types']:
				pp['COUNTYLONG'] = component['long_name']
				pp['COUNTYSHORT'] = component['short_name']



if __name__=="__main__":
	found_filename, provided_filename = get_filenames()
