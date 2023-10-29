import os
# okay

api_key = os.environ.get('ACS_PRE_FETCH_API_KEY') # Google API - "auyon-zach-second"
# https://console.cloud.google.com/apis/dashboard?project=auyon-zach&authuser=2&pageState=(%22duration%22:(%22groupValue%22:%22P7D%22,%22customValue%22:null))

census_api_key = os.environ.get('ACS_PRE_FETCH_CENSUS_API_KEY') #new 02/04/2021
mapquest_api_key = os.environ.get('ACS_PRE_FETCH_MAPQUEST_API_KEY')
mapquest_api_secret = "9alIooDtcGbo4mAc"

geoapify_api_key = os.environ.get('ACS_PRE_FETCH_GEOAPIFY_API_KEY')

mapbox_api_key = os.environ.get('ACS_PRE_FETCH_MAPBOX_API_KEY')

top_dir = os.environ.get('ACS_PRE_FETCH_TOP_DIR')
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
ohio_vf_filenames = sorted([f"{ohio_vf_dir}/{fname}" for fname in os.listdir(ohio_vf_dir) if fname.startswith('SWVF') and fname.endswith('.csv')])
ohio_fake_vf_filenames = sorted([f"{ohio_vf_dir}/sample/{fname}" for fname in os.listdir(ohio_vf_dir) if fname.startswith('SWVF') and fname.endswith('.csv')])
# ohio_geocoded_vf_dir = f"{top_dir}/data/results/geocoded_vfs"
# ohio_fake_geocoded_vf_dir = f"{ohio_geocoded_vf_dir}/sample"

# ohio_geocoded_source = f"{ohio_geocoded_vf_dir}/sample/geocoded_state_39_counties_153_133_061_025_165_017_015_035_055_103_085_093_159_127_073_129_097_117_089_041_045_049_057_113_109_02_12_2020_10_58.csv" #remove /sample if not using a sample


georgia_vf_dir = f"{top_dir}/data/voter_files/Georgia"
georgia_vf_filenames = [f"{georgia_vf_dir}/Georgia_Daily_VoterBase.txt"]

# georgia_vf_filenames = [f"{georgia_vf_dir}/Georgia_Daily_VoterBase_{i}.txt" for i in range(8)]

georgia_fake_vf_filenames = [f"{georgia_vf_dir}/preview.txt"]


# https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html
metro_delineation_file = f"{top_dir}/data/metro_delineation/list1_2020.xls"


## Polling Place Data
# /Users/zach/UCLA Anderson Dropbox/Zach Siegel/Polling/Georgia/Voter File (Special Election 2021)
georgia_pp_dir = "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/Polling/Georgia"
georgia_special2021_dir = f"{georgia_pp_dir}/Voter File (Special Election 2021)"
georgia_special2021_fnames = sorted([f"{georgia_special2021_dir}/{fname}" for fname in os.listdir(georgia_special2021_dir) if  fname.startswith('Georgia_Daily_VoterBase_2021')])

georgia_pp_2016_dir = "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/Polling/Data/us-polling-places-2012-2018/data/Georgia"
georgia_pp_2016_fname = f"{georgia_pp_2016_dir}/Georgia_2016.csv"


census_prefetch_dir = f"{top_dir}/data/census_pre_fetch"
census_codes_file = f"{census_prefetch_dir}/census_variables_key.json"


census_acs_file = f'{census_prefetch_dir}/Georgia_tract_ACS5.csv'
census_acs_file_raw = f'{census_prefetch_dir}/Georgia_tract_ACS5_raw.csv'

#for ALAND:
# download: https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.2019.html
# column descr: https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/gaz-record-layouts.2019.html
census_tract_tiger_data = f"{census_prefetch_dir}/2019_Gaz_tracts_national.txt"

# /Users/zach/UCLA Anderson Dropbox/Zach Siegel/Polling/Georgia/Voter History Files/nationbuilder
shared_data_dir = georgia_pp_dir #"/Users/zach/UCLA Anderson Dropbox/Zach Siegel/Polling/Georgia"
nationbuilder_vh_dir = f"{shared_data_dir}/Voter History Files/nationbuilder"
nationbuilder_vh_filename = f"{nationbuilder_vh_dir}/ga_vh.csv"
nationbuilder_vf_filename = f"{nationbuilder_vh_dir}/ga_vf.csv"
nationbuilder_vfvh_dir = f"/Users/zach/UCLA Anderson Dropbox/Zach Siegel/Polling/Georgia/Voter History Files/nationbuilder_joined"
nationbuilder_vfvh_filenames = [f"{nationbuilder_vfvh_dir}/gavfvh-0000000000{n:02}.csv" for n in range(17)]
# ["gavfvh-000000000000.csv", "gavfvh-000000000006.csv", "gavfvh-000000000012.csv", "gavfvh-000000000001.csv", "gavfvh-000000000007.csv", "gavfvh-000000000013.csv", "gavfvh-000000000002.csv", "gavfvh-000000000008.csv", "gavfvh-000000000014.csv", "gavfvh-000000000003.csv", "gavfvh-000000000009.csv", "gavfvh-000000000015.csv", "gavfvh-000000000004.csv", "gavfvh-000000000010.csv", "gavfvh-000000000016.csv", "gavfvh-000000000005.csv", "gavfvh-000000000011.csv"]
