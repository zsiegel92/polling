import os
# okay 
api_key = os.environ.get('PP_API_KEY') # Google API - "auyon-zach-second"
census_api_key = os.environ.get('PP_CENSUS_API_KEY') # CENSUS API

mapquest_api_key = os.environ.get('PP_MAPQUEST_API_KEY')
mapquest_api_secret = os.environ.get('PP_MAPQUEST_API_SECRET')

geoapify_api_key = os.environ.get('PP_GEOAPIFY_API_KEY')

mapbox_api_key = os.environ.get('PP_MAPBOX_API_KEY')

top_dir = "/Users/zach/Documents/UCLA_classes/research/Polling"
# top_dir = "/Users/zach//Desktop/civic_full_project"
# top_dir = "/Users/zach/Desktop/pp_visualization"


# civic_query_results_dir = f"{top_dir}/data/results/civic_results"
civic_query_results_dir = f"{top_dir}/data/results/civic_results/general2020_points_and_polling_places"


map_dir = f"{top_dir}/data/results/maps_pps"

metro_shape_dir = f"{top_dir}/data/shapefiles/metro"
metro_shape_filename = "metro_shapes_2019.json"
metro_properties_filename = "metro_shapes_2019_only_properties.json"
metro_shape_full_path = f"{metro_shape_dir}/{metro_shape_filename}"
metro_properties_full_path = f"{metro_shape_dir}/{metro_properties_filename}"

