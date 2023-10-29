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
def get_grid_intersection_turfpy(metro,km,translate_north_and_east=0,shapelyStyle=True,chunk_size=1):
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
			if shapelyStyle:
				points = get_contained_points_shapelystyle(points,polygon) # homemade, faster
			else:
				points = points_within_polygon(points,polygon,chunk_size=chunk_size) # uses fewer packages, but SLOW
			all_points['features'].extend(points['features'])
		# print(f"Returning {len(all_points['features'])} points.")
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
		if shapelyStyle:
			points = get_contained_points_shapelystyle(points,polygon) # homemade, faster
		else:
			points = points_within_polygon(points,polygon,chunk_size=chunk_size) # uses fewer packages, but SLOW
		# print(f"Returning {len(points['features'])} points.")
		return points



if __name__=="__main__":
	from time import time
	from shapefile_utilities import get_interested_metro_by_namelsad
	from folium_utilities import plot_shapes, view_plot
	metro = get_interested_metro_by_namelsad("Los Angeles-Long Beach-Anaheim, CA Metro Area")
	nTests = 5
	params = [{'shapelyStyle' : True, 'chunk_size' : None}] + [{'shapelyStyle' : False, 'chunk_size' : chunk_size} for chunk_size in (100,500,1000,5000,10000,15000)]
	params = [param for param in params if not param['shapelyStyle'] or (param['shapelyStyle'] and param['chunk_size'] == 1)]
	for param in params:
		timeSum = 0
		print(f"{param=}")
		for i in range(nTests):
			start = time()
			grid = get_grid_intersection_turfpy(metro,km=2,**param)
			end = time()
			timeSum += (end-start)
			print(f"Iteration {i+1}/{nTests}",end=" ")
		print(f"Average for {param}: {timeSum/nTests}")
	# m = plot_shapes([metro,grid])
	# view_plot(m,append_random=False)

