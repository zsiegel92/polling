import os
import csv
import pandas as pd
from config import civic_query_results_dir, map_dir
from shapefile_utilities import get_interested_metro_by_namelsad
from folium_utilities import plot_shapes, view_plot, save_plot
from geojson_utilities import create_point, list_of_points_to_featureCollection
from geocode_utilities import geocode_address

from plotly_utilities import metro_to_dataframe, metro_mapbox,add_scatter_pps, save_plot as save_plotly, show_fig, open_in_chrome,generate_plot, init_fig, add_metro_to_fig, add_all_to_plot, add_multiple_metros_to_fig



def get_nameLSAD_from_filename(filename):
	# filename = "civicAPI_unique_places_Providence-Warwick,_RI-MA_Metro_Area_spacing_1km_02_11_2020_21_43.csv"
	return filename.rsplit("_unique_places_",1)[-1].split("_spacing_")[0].replace("_", " ")

def get_all_pp_filenames():
	return [filename for filename in os.listdir(civic_query_results_dir) if 'unique_places' in filename]

def initialize_unique_places():
	# different from civic_api_grid
	allPlaceTypes = ["pollingLocation", "earlyVoteSite", "dropOffLocation"]
	return {placeType : [] for placeType in allPlaceTypes}

def read_pps_to_featureCollection(filename):
	unique_places = initialize_unique_places()
	with open(f"{civic_query_results_dir}/{filename}") as csvfile:
		reader = csv.DictReader(csvfile)
		unique_places_raw = [row for row in reader]
	for row in unique_places_raw:
		placeType = row['placeType']
		unique_places[placeType].append(row)
	return places_to_featureCollection(unique_places)

def read_pps_pandas(filename):
	unique_places =  pd.read_csv(f"{civic_query_results_dir}/{filename}")
	return unique_places


def places_to_featureCollection(unique_places):
	# create_point(lon,lat,properties=None)
	list_of_points = []
	numGeocoded = 0
	numErrors = 0
	for placeType,placesList in unique_places.items():
		for place in placesList:
			try:
				point = create_point(float(place['longitude']),float(place['latitude']),properties=place)
			except Exception:
				numGeocoded +=1
				try:
					append_latlon(place)
					point = create_point(float(place['longitude']),float(place['latitude']),properties=place)
				except Exception as e:
					print(e)
					print("FAILURE EVEN AFTER GEOCODING")
					numErrors += 1
					continue
			list_of_points.append(point)
	# return list_of_points_to_featureCollection([create_point(float(place['longitude']),float(place['latitude']),properties=place) for placeType,placesList in unique_places.items() for place in placesList])
	print(f"{numGeocoded} GEOCODED; {numErrors} ERRORS")
	return list_of_points_to_featureCollection(list_of_points)

def get_map_filename(csv_filename):
	# civicAPI_unique_places_Milwaukee-Waukesha,_WI_Metro_Area_spacing_1.0km_28_10_2020_17_38.html
	nameLSAD = get_nameLSAD_from_filename(csv_filename).replace(" ","_")
	file_basename = csv_filename.rsplit(".",1)[0]
	map_filename = f"placeMap_{file_basename.split('_unique_places_',1)[-1]}.html"
	return f"{map_dir}/{map_filename}"

def get_plotly_filename(csv_filename,full_provided=False):
	if full_provided:
		return f"{map_dir}/{csv_filename}"
	# civicAPI_unique_places_Milwaukee-Waukesha,_WI_Metro_Area_spacing_1.0km_28_10_2020_17_38.html
	nameLSAD = get_nameLSAD_from_filename(csv_filename).replace(" ","_")
	file_basename = csv_filename.rsplit(".",1)[0]
	map_filename = f"placeMap_plotly_{file_basename.split('_unique_places_',1)[-1]}.html"
	return f"{map_dir}/{map_filename}"



def append_latlon(place):
	lat, lng = geocode_address(address= f"{place['addressLocationname']} {place['addressLine1']} {place['addressLine2']}" ,city=place['addressCity'],state=place['addressState'])
	place['longitude'] = lng
	place['latitude'] = lat


def generate_all_leaflet():
	filenames = get_all_pp_filenames()
	for ii,filename in enumerate(filenames):
		nameLSAD = get_nameLSAD_from_filename(filename)
		print(f"MAPPING {nameLSAD}, {ii+1}/{len(filenames)}")
		metro = get_interested_metro_by_namelsad(nameLSAD)
		unique_places = read_pps_to_featureCollection(filename)
		m = plot_shapes([metro,unique_places])
		# view_plot(m,append_random=False)
		save_plot(m,get_map_filename(filename))


def generate_all_plotly():
	filenames = get_all_pp_filenames()
	for ii,filename in enumerate(filenames):
		nameLSAD = get_nameLSAD_from_filename(filename)
		print(f"MAPPING {nameLSAD}, {ii+1}/{len(filenames)}")
		metro = get_interested_metro_by_namelsad(nameLSAD)
		unique_places = read_pps_pandas(filename)
		fig = generate_plot(metro,unique_places)
		map_filename=get_plotly_filename(filename)
		save_plotly(fig,map_filename)
		# open_in_chrome(map_filename)


def create_full_map(plotName="USA_pps",nMetros=None, themes=["basic", "streets", "outdoors", "light", "dark", "satellite", "satellite-streets"] + ["open-street-map", "carto-positron", "carto-darkmatter", "stamen-terrain", "stamen-toner", "stamen-watercolor"] + ["white-bg"]):
	filenames = get_all_pp_filenames()[:nMetros]
	nameLSADs = [get_nameLSAD_from_filename(filename) for filename in filenames]
	unique_places_all = [read_pps_pandas(filename).assign(metro=nameLSAD) for nameLSAD,filename in zip(nameLSADs,filenames)]
	unique_places = pd.concat(unique_places_all,sort=True) #,keys=nameLSADs
	metros = [get_interested_metro_by_namelsad(nameLSAD) for nameLSAD in nameLSADs]
	print("GOT DATA. PLOTTING")
	for theme in themes:
		fig = init_fig()
		add_multiple_metros_to_fig(fig,metros,mapbox_style=theme)
		add_scatter_pps(fig,unique_places,has_metro_as_column=True)
		outfilename = get_plotly_filename(f"USA_pps/{plotName}_{theme}.html",full_provided=True)
		save_plotly(fig,outfilename)
	return fig
	# open_in_chrome(map_filename)

if __name__=="__main__":
	fig = create_full_map(themes=['basic'],plotName="TEST", nMetros=2)
	# fig = init_fig()
	# filenames = get_all_pp_filenames()
	# nameLSADs = [get_nameLSAD_from_filename(filename) for filename in filenames]
	# unique_places_all = [read_pps_pandas(filename).assign(metro=nameLSAD) for nameLSAD,filename in zip(nameLSADs,filenames)]
	# unique_places = pd.concat(unique_places_all,sort=True) #,keys=nameLSADs
	# metros = [get_interested_metro_by_namelsad(nameLSAD) for nameLSAD in nameLSADs]
	# add_multiple_metros_to_fig(fig,metros)
	# add_scatter_pps(fig,unique_places,has_metro_as_column=True)
	# outfilename = get_plotly_filename("USA_partial.html",full_provided=True)
	# save_plotly(fig,outfilename)


# if __name__=="__main__":
	# import pandas as pd
	# import geopandas as gp
	# import plotly.express as px
	# import plotly.graph_objects as go
	# import json
	# import io
	# from config import mapbox_api_key
	# from geojson_utilities import feature_to_featureCollection, get_base_coords
	# filename = get_all_pp_filenames()[15]
	# nameLSAD = get_nameLSAD_from_filename(filename)
	# print(f"MAPPING {nameLSAD}")
	# metro = get_interested_metro_by_namelsad(nameLSAD)
	# # metro_df = metro_to_dataframe(metro)
	# unique_places = read_pps_pandas(filename)
	# df = unique_places
	# fig = metro_mapbox(metro)
	# add_scatter_pps(fig,unique_places)
	# traces = list(fig.select_traces())[1:]
	# trace = traces[0]
	# print(trace.marker)
	# # show_fig(fig)
	# map_filename=get_plotly_filename(filename)
	# save_plotly(fig,map_filename)
	# open_in_chrome(map_filename)

