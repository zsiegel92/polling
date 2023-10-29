import os
# okay

if os.getenv("USER").lower() == "zach":
	print("USING ZACH's API KEY")
	api_key = os.environ.get('API_KEY') # Google API - new GCP project
else:
	print("USING AUYON'S API KEY")
	api_key = os.environ.get('NON_ZACH_GOOGLE_API_KEY') # GOOGLE API - Auyon/Nic


census_api_key = os.environ.get('CENSUS_API_KEY') # CENSUS API


top_dir = "/Users/zach/Documents/UCLA_classes/research/Polling" #THE DIRECTORY WHERE THIS .config FILE LIVES
# top_dir = "/Users/zach/Desktop/civic_full_project" #THE DIRECTORY WHERE THIS .config FILE LIVES


civic_query_results_dir = f"{top_dir}/data/results/civic_results"


metro_shape_dir = f"{top_dir}/data/shapefiles/metro"
metro_shape_filename = "metro_shapes_2019.json"
metro_props_filename = "metro_shapes_2019_only_properties.json"
metro_shape_full_path = f"{metro_shape_dir}/{metro_shape_filename}"

# https://www.census.gov/data/tables/time-series/demo/popest/2010s-total-metro-and-micro-statistical-areas.html
metro_populations_dir = f"{top_dir}/data/metro_populations"
metro_populations_full_path = f"{metro_populations_dir}/cbsa-est2019-alldata.csv"
