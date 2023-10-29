import json
import os
from config import metro_shape_full_path, county_shape_full_path

def load_geojson_features(path_to_file):
	with open(path_to_file, "r") as content:
		geojson = json.load(content)
	return geojson['features']

def get_interested_places(full_geojson_path,interested_properties,features=None):
	if features is None:
		features = load_geojson_features(full_geojson_path)
	try:
		return [place for place in features if place['properties'] in interested_properties]
	except TypeError:
		return [place for place in features if place['properties'] == interested_properties]



def get_interested_place_by_partial_props(full_geojson_path,partial_properties,features=None):
	if features is None:
		features = load_geojson_features(full_geojson_path)
	for place in features:
		if all(place['properties'].get(k) == v for k,v in partial_properties.items()):
			return place
	return None

def get_interested_metro_by_namelsad(metro_name,features=None):
	return get_interested_place_by_partial_props(metro_shape_full_path,{'NAMELSAD' : metro_name },features=features)

def get_interested_metro_properties_by_namelsad(metro_name,features=None):
	if features is None:
		features = get_properties_only(metro_shape_full_path)
	partial_properties = {'NAMELSAD' : metro_name }
	for place in features:
		if all(place['properties'].get(k) == v for k,v in partial_properties.items()):
			return place
	return None

def get_properties_only(path_to_file,write=False):
	newfilename = path_to_file.rsplit(".",1)[0] + "_only_properties.json"
	if os.path.exists(newfilename):
		with open(newfilename, "r") as content:
			geojson = json.load(content)
	else:
		with open(path_to_file, "r") as content:
			geojson = json.load(content)
		for feature in geojson['features']:
			feature['geometry']['coordinates']= []
	# json = json.dumps(geojson)
	if write:
		with open(newfilename,"w") as f:
			f.write(json.dumps(geojson,indent=5))
	return geojson['features']

if __name__=="__main__":
	props = get_properties_only(metro_shape_full_path,write=True)
