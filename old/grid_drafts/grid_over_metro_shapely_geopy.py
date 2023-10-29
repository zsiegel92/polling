from numpy import sqrt
from shapely.geometry import Polygon
import shapely
import geopy
import geopy.distance
import geojson

from process_ohio_shapefiles import get_interested_metros, get_interested_counties, get_interested_cities, get_base_coords, plot_shapes, view_plot


### THIS FILE IS DEPRECATED IN FAVOR OF USING GEOJSON AND TURFPY

def extract_geometry(metro):
	return metro['geometry']['coordinates'][0]


# class myPoint(geopy.Point,shapely.geometry.Point):
# 	pass

def geopy_to_shapely(point):
	return shapely.geometry.Point(point.longitude,point.latitude)

def get_grid_intersection_with_metro(metro):
	outline = extract_geometry(metro)
	polygon = Polygon(outline)
	grid = generate_point_grid(polygon)
	return [point for point in grid if polygon.contains(geopy_to_shapely(point))]

def generate_point_grid(polygon):
	lonmin,latmin,lonmax,latmax = polygon.bounds
	point = geopy.Point((latmin,lonmin))
	# https://stackoverflow.com/questions/24427828/calculate-point-based-on-distance-and-direction
	# A bearing of 90 degrees corresponds to East, 180 degrees is South, and so on.
	d = geopy.distance.distance(kilometers = 2)
	points = []
	while point.latitude <= latmax:
		point.longitude = lonmin
		while point.longitude < lonmax:
			points.append(point)
			point = d.destination(point=point,bearing=90) # East
		point = d.destination(point=point,bearing=0) # North
	return points



if __name__=="__main__":
	metro = get_interested_metros()[0]
	metroname = metro['properties']['NAME']
