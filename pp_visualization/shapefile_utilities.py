import json

from config import metro_shape_full_path

def load_geojson_features(path_to_file):
	with open(path_to_file, "r") as content:
		geojson = json.load(content)
	return geojson['features']

def get_interested_places(full_geojson_path,interested_properties):
	features = load_geojson_features(full_geojson_path)
	try:
		return [place for place in features if place['properties'] in interested_properties]
	except TypeError:
		return [place for place in features if place['properties'] == interested_properties]



def get_interested_place_by_partial_props(full_geojson_path,partial_properties):
	features = load_geojson_features(full_geojson_path)
	for place in features:
		if all(place['properties'].get(k) == v for k,v in partial_properties.items()):
			return place
	return None

def get_interested_metro_by_namelsad(metro_name):
	return get_interested_place_by_partial_props(metro_shape_full_path,{'NAMELSAD' : metro_name })


def get_properties_only(path_to_file,write=False):
	with open(path_to_file, "r") as content:
		geojson = json.load(content)
	for feature in geojson['features']:
		feature['geometry']['coordinates']= []
	# json = json.dumps(geojson)
	if write:
		newfilename = path_to_file.rsplit(".",1)[0] + "_only_properties.json"
		with open(newfilename,"w") as f:
			f.write(json.dumps(geojson,indent=5))

if __name__=="__main__":
	props = get_properties_only(metro_shape_full_path,write=True)
