from turfpy.measurement import bbox, destination,distance
from geojson import Polygon, Point, Feature, FeatureCollection
from geojson_utilities import get_lon,get_lat,create_point,set_lon, get_contained_points_shapelystyle
from turfpy.measurement import points_within_polygon



def measure_distance(lon1,lat1,lon2,lat2):
	start = Feature(geometry=Point((lon1,lat1)))
	end = Feature(geometry=Point((lon2,lat2)))
	return distance(start,end)

def get_feature_polygon(geojson_feature):
	return Polygon(geojson_feature['geometry']['coordinates'])


def get_features_multipolygon(geojson_feature):
	return [Polygon(polygon_coordinates) for polygon_coordinates in geojson_feature['geometry']['coordinates']]

# metro should be a geojson object (as a native dict or json or GeoJSON or FeatureCollection, etc)
def get_grid_intersection(metro,km,translate_north_and_east=0):
	if metro['geometry']['type'] == 'MultiPolygon':
		polygons = get_features_multipolygon(metro)
		geo_options = {'units' : 'km'}
		all_points = FeatureCollection([])
		for polygon in polygons:
			lonmin,latmin,lonmax,latmax =  bbox(polygon)
			point = create_point(lonmin,latmin) # (40,-75) is a (lat, lon) for Philadelphia
			if translate_north_and_east > 0:
				point = destination(point,translate_north_and_east,0,geo_options)
				point = destination(point,translate_north_and_east,90,geo_options)
				lonmin,latmin = get_lon(point),get_lat(point)
			points = []
			while get_lat(point) <= latmax:
				set_lon(point,lonmin)
				while get_lon(point) <= lonmax:
					points.append(point)
					point = destination(point,km,90,geo_options)
				point = destination(point,km,0,geo_options)
			points = FeatureCollection(points)
			# points = points_within_polygon(points,polygon) # uses fewer packages, but SLOW
			points = get_contained_points_shapelystyle(points,polygon) # homemade, faster
			all_points['features'].extend(points['features'])
		print(f"Returning {len(all_points['features'])} points.")
		return all_points
	else:
		polygon = get_feature_polygon(metro)
		geo_options = {'units' : 'km'}
		lonmin,latmin,lonmax,latmax =  bbox(polygon)
		point = create_point(lonmin,latmin) # (40,-75) is a (lat, lon) for Philadelphia
		if translate_north_and_east > 0:
			point = destination(point,translate_north_and_east,0,geo_options)
			point = destination(point,translate_north_and_east,90,geo_options)
			lonmin,latmin = get_lon(point),get_lat(point)
		points = []
		while get_lat(point) <= latmax:
			set_lon(point,lonmin)
			while get_lon(point) <= lonmax:
				points.append(point)
				point = destination(point,km,90,geo_options)
			point = destination(point,km,0,geo_options)
		points = FeatureCollection(points)
		# points = points_within_polygon(points,polygon) # uses fewer packages, but SLOW
		points = get_contained_points_shapelystyle(points,polygon) # homemade, faster
		print(f"Returning {len(points['features'])} points.")
		return points


# if __name__=="__main__":
# 	from shapefile_utilities import get_interested_metro_by_namelsad
# 	from folium_utilities import plot_shapes, view_plot
# 	from time import time
# 	metroname = "Los Angeles-Long Beach-Anaheim, CA Metro Area"
# 	metro = get_interested_metro_by_namelsad(metroname)
# 	metroname = metro['properties']['NAME']
# 	grid = get_grid_intersection(metro,km=5)
# 	# ntrials = 5
# 	# start = time()
# 	# for i in range(ntrials):
# 	# 	grid = get_grid_intersection(metro,km=1)
# 	# 	print(f"Ran {i+1}/{ntrials} trials!")
# 	# end = time()
# 	# avg = (end-start)/ntrials
# 	# print(f"Ran {ntrials} trials with average time of {avg} seconds!")
# 	m = plot_shapes([metro,grid])
# 	view_plot(m,append_random=False)

