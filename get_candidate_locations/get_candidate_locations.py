import csv

from geojson_utilities import get_lon_lat
from folium_utilities import plot_shapes, view_plot
from ohio_vf_config import get_interested_metro
from grid_utilities import get_grid_intersection_turfpy
from place_api_utilities import get_nearby_places



def get_nearby_places_for_points(featureCollection,radius_km=50):
	nearby_places = {}
	for point in featureCollection['features']:
		lon,lat = get_lon_lat(point)
		nearby_places[(lon,lat)] = get_nearby_places(lon,lat,radius_m=radius_km*1000)
	return get_dict_rows(nearby_places)

def get_dict_rows(nearby):
	rows = []
	for (lon,lat),results in nearby.items():
		for result in results:
			row = {
				"name" : result['name'],
				"types" : str(result['types']),
				"vicinity_address" : result['vicinity'],
				"querylat": lat,
				"querylon": lon,
				"distance_from_query" : result['distance'],
				"lat" : result['place_lat'],
				"lon" : result['place_lon'],
				"place_id" : result['place_id'],
				"nearby_rank" : result['nearby_rank']
			}
			rows.append(row)
	return rows

def write_rows(rows,outfilename):
	headers = rows[0].keys()
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)

def get_outfilename(metroname,spacing_km):
	results_dir = "/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio/places_results"
	return f"{results_dir}/places_nearbysearch_{metroname.replace(' ','_')}_spacing_{spacing_km}km.csv"


if __name__=="__main__":
	metro = get_interested_metro()
	metroname = metro['properties']['NAME']
	spacing_km = 20
	grid = get_grid_intersection_turfpy(metro,km=spacing_km)
	m = plot_shapes([metro,grid])
	view_plot(m,append_random=False)

	# nearby = get_nearby_places_for_points(grid,radius_km=spacing_km)
	# filename = get_outfilename(metroname,spacing_km)
	# write_rows(nearby,filename)




# import requests
# from geopy.distance import distance
# from config import  api_key, geocoding_api_key

# point = grid['features'][0]
# lon,lat = get_lon_lat(point)
# radius_m = spacing_km*1000
# keys = ['name','types','vicinity','geometry','place_id']
# desired_types =  ['school','post_office']
# qtype = desired_types[0]
# results = []

# nearbyquery = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lon}&radius={radius_m}&type={qtype}&key={api_key}"
# rdata = requests.get(nearbyquery).json()
# for i,result in enumerate(rdata['results']):
# 	if i< n:
# 		info = {k:result[k] for k in keys}
# 		info['nearby_rank'] = i
# 		place_lat = float(result['geometry']['location']['lat'])
# 		place_lon = float(result['geometry']['location']['lng'])
# 		info['place_lat'] = place_lat
# 		info['place_lon'] = place_lon
# 		info['distance'] = distance(coords,(place_lat,place_lon)).meters
# 		results.append(info)
