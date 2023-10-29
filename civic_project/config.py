import os
import pathlib
# okay

if os.getenv("USER").lower() == "zach":
	print("ON ZACH'S COMPUTER")
	api_key = os.environ.get("API_KEY")# Google API - new GCP project
else:
	print("NOT ON ZACH'S COMPUTER")
	api_key = os.environ.get("NON_ZACH_GOOGLE_API_KEY")# GOOGLE API - Auyon/Nic


census_api_key = os.environ.get("CENSUS_API_KEY") # CENSUS API


configpath = pathlib.Path().absolute()
top_dir = configpath.parent.parent



civic_query_results_dir = f"{top_dir}/data/results/civic_results"

georgia_results_dir = f"{top_dir}/data/results/civic_results/Georgia" #to overwrite civic_query_results_dir when examining Georgia


metro_shape_dir = f"{top_dir}/data/shapefiles/metro"
metro_shape_filename = "metro_shapes_2019.json"
metro_props_filename = "metro_shapes_2019_only_properties.json"
metro_shape_full_path = f"{metro_shape_dir}/{metro_shape_filename}"

# For Metro-County Delineation
metro_delineation_file = f"{top_dir}/data/metro_delineation/list1_2020.xls"

# For Metro-County Delineation, State Intersection
county_shape_dir = f"{top_dir}/data/shapefiles/cb_2018_us_county_500k"
county_shape_filename = "cb_2018_us_county_500k.json"
county_properties_filename = "cb_2018_us_county_500k_only_properties.json"
county_shape_full_path = f"{county_shape_dir}/{county_shape_filename}"
county_properties_full_path = f"{county_shape_dir}/{county_properties_filename}"

# For Plotly
mapbox_api_key = os.environ.get('MAPBOX_API_KEY')
