import os

api_key=os.environ.get('API_KEY')
census_api_key=os.environ.get('CENSUS_API_KEY')

mapquest_api_key=os.environ.get('MAPQUEST_API_KEY')
mapquest_api_secret=os.environ.get('MAPQUEST_API_SECRET')

geoapify_api_key=os.environ.get('GEOAPIFY_API_KEY')

mapbox_api_key=os.environ.get('MAPBOX_API_KEY')

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


county_shape_dir = f"{top_dir}/data/shapefiles/cb_2018_us_county_500k"
county_shape_filename = "cb_2018_us_county_500k.json"
county_properties_filename = "cb_2018_us_county_500k_only_properties.json"
county_shape_full_path = f"{county_shape_dir}/{county_shape_filename}"
county_properties_full_path = f"{county_shape_dir}/{county_properties_filename}"


ohio_vf_dir = f"{top_dir}/data/voter_files/Ohio"
ohio_vf_filenames = [f"{ohio_vf_dir}/{fname}" for fname in os.listdir(ohio_vf_dir) if fname.startswith('SWVF') and fname.endswith('.csv')]
ohio_fake_vf_filenames = [f"{ohio_vf_dir}/sample/{fname}" for fname in os.listdir(ohio_vf_dir) if fname.startswith('SWVF') and fname.endswith('.csv')]
ohio_geocoded_vf_dir = f"{top_dir}/data/results/geocoded_vfs"
ohio_fake_geocoded_vf_dir = f"{ohio_geocoded_vf_dir}/sample"

ohio_geocoded_source = f"{ohio_geocoded_vf_dir}/sample/geocoded_state_39_counties_153_133_061_025_165_017_015_035_055_103_085_093_159_127_073_129_097_117_089_041_045_049_057_113_109_02_12_2020_10_58.csv" #remove /sample if not using a sample



# https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html
metro_delineation_file = f"{top_dir}/data/metro_delineation/list1_2020.xls"
