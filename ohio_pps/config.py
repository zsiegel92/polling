import os
# okay

api_key = os.environ.get('NON_ZACH_GOOGLE_API_KEY')    # GOOGLE API - Auyon/Nic
api_key2 = os.environ.get("GOOGLE_OTHER_PERMISSIONS_KEY") # Google API - Zach
census_api_key = os.environ.get('CENSUS_API_KEY') # CENSUS API
geocoding_api_key = os.environ.get('GEOCODING_API_KEY') #GEOCODING API
geocoding_api_key2 = os.environ.get('GOOGLE_OTHER_PERMISSIONS_KEY') # Geocoding API - Zach


top_dir = "/Users/zach/Documents/UCLA_classes/research/Polling" #THE DIRECTORY WHERE THIS .config FILE LIVES


civic_query_results_dir = f"{top_dir}/results/civic_results"


metro_shape_dir = f"{top_dir}/data/shapefiles"
metro_shape_filename = "metro_shapes_2019.json"
metro_shape_full_path = f"{metro_shape_dir}/{metro_shape_filename}"


### DO NOT NEED THESE FOR CIVIC QUERIES
vf_between_dirs = "/voter_files/Ohio" #change to "" if no 'between' directories...
county_shape_dir = f"{top_dir}{vf_between_dirs}/ohio_shapefiles/from_census/cb_2018_us_county_500k"
county_shape_filename = "cb_2018_us_county_500k.json"
county_shape_full_path = f"{county_shape_dir}/{county_shape_filename}"


city_shape_dir = f"{top_dir}{vf_between_dirs}/ohio_shapefiles/from_census/tl_2013_39_place"
city_shape_filename = "tl_2013_39_place.json"
city_shape_full_path = f"{city_shape_dir}/{city_shape_filename}"




