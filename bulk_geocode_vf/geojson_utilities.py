from geojson import Polygon, Point, Feature, FeatureCollection
import numpy as np

def get_lon_lat(point):
	lon, lat = point.get('geometry').get('coordinates')
	return lon, lat

def get_lon(point):
	return get_lon_lat(point)[0]

def get_lat(point):
	return get_lon_lat(point)[1]

def create_point(lon,lat,properties=None):
	return Feature(geometry=Point((lon,lat),properties=properties))

def set_coordinates(point,coordinates):
	point['geometry']['coordinates'] = coordinates

def set_lon(point,lon):
	point['geometry']['coordinates'][0] = lon

def set_lat(point,lat):
	point['geometry']['coordinates'][1] = lat

def list_of_points_to_featureCollection(points_list):
	return FeatureCollection(points_list)

def feature_to_featureCollection(feature,multiple=False):
	featureCollection = {
		"type" : "FeatureCollection",
		"features" : [ feature ]
	}
	if multiple:
		featureCollection['features'] = feature
	return featureCollection


def get_nameLSAD(feature):
	return feature['properties']['NAMELSAD']

def get_base_coords(featureThing):
	if 'geometry' in featureThing:
		coordThing = featureThing['geometry']['coordinates']
		try:
			coordThing[0][0][0][0] # multipolygon longitude, else exception
			intlon = np.mean([coord[0] for coordlist in coordThing[0] for coord in coordlist])
			intlat = np.mean([coord[1] for coordlist in coordThing[0] for coord in coordlist])
			print("MULTIPOLYGON")
		except:
			intlon = np.mean([coord[0] for coord in coordThing[0]])
			intlat = np.mean([coord[1] for coord in coordThing[0]])
			print("POLYGON")
	elif 'features' in featureThing:
		#featureThing.type == 'featureCollection'
		intlon = np.mean([feature['geometry']['coordinates'][0] for feature in featureThing['features']])
		intlat = np.mean([feature['geometry']['coordinates'][1] for feature in featureThing['features']])
		print("FEATURECOLLECTION")
	else:
		print("UNKNOWN TYPE")
		raise #unknown type
	print(f"Centering map on ({intlat=}, {intlon=})")
	return intlat, intlon


# rename to make it clearer I'm not geocoding or reverse geocoding.
extract_lon_lat	= get_lon_lat
extract_lon	= get_lon
extract_lat	= get_lat

##
## ONLY FOR exporting get_contained_points_shapelystyle
## NOTE: everything below here is redundant to the above, and uses a whole additional imported library, but it turns out to be much faster.
##

import shapely.geometry
import shapely
def point_to_shapely(point):
	lon,lat = get_lon_lat(point)
	return shapely.geometry.Point(lon,lat)

def get_list_of_shapely_points(featureCollection):
	return [point_to_shapely(point) for point in featureCollection['features']]

def polygon_to_shapely(polygon):
	return shapely.geometry.Polygon(polygon['coordinates'][0])

def shapely_points_to_featureCollection(points):
	geojson_pts = [create_point(point.x,point.y) for point in points]
	return FeatureCollection(geojson_pts)

def get_contained_points_shapelystyle(points,polygon):
	shply_polygon = polygon_to_shapely(polygon)
	shapely_points = [shply_point for shply_point in get_list_of_shapely_points(points) if shply_polygon.contains(shply_point)]
	return shapely_points_to_featureCollection(shapely_points)
