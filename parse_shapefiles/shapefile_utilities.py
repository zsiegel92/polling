import json
import subprocess


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

def get_interested_metro_by_namelsad(metro_shape_full_path,metro_name):
	return get_interested_place_by_partial_props(metro_shape_full_path,{'NAMELSAD' : metro_name })


def get_properties_only(path_to_file,write=False):
	if path_to_file.endswith('.shp'):
		path_to_file = path_to_file.rsplit(".",1)[0] + '.json'
	with open(path_to_file, "r") as content:
		geojson = json.load(content)
	for feature in geojson['features']:
		feature['geometry']['coordinates']= []
	# json = json.dumps(geojson)
	if write:
		newfilename = path_to_file.rsplit(".",1)[0] + "_only_properties.json"
		with open(newfilename,"w") as f:
			f.write(json.dumps(geojson,indent=5))


# cd /Users/zach/Downloads/cb_2018_us_county_500k
# ogr2ogr -f "GeoJSON" cb_2018_us_county_500k.json cb_2018_us_county_500k.shp
def parse_shapefile(path_to_file):
	fsplit = path_to_file.rsplit("/",1)
	wd = fsplit[0]
	fname =  fsplit[1]
	json_filename = fname.rsplit(".",1)[0] + ".json"
	cmd = ' '.join(['ogr2ogr','-f','"GeoJSON"',json_filename,fname])
	print(f"Running command {cmd} in {wd}")
	subprocess.run(cmd,cwd=wd,shell=True)


if __name__=="__main__":
	# props = get_properties_only(metro_shape_full_path,write=True)
	# props = get_properties_only("/Users/zach/Documents/UCLA_classes/research/Polling/data/shapefiles/cb_2018_us_county_500k/cb_2018_us_county_500k.json",write=True)
	# props = get_properties_only("/Users/zach/Downloads/cb_2018_us_county_500k/cb_2018_us_county_500k.json",write=True)
	shapefilename = "/Users/zach/Downloads/cb_2018_us_csa_20m/cb_2018_us_csa_20m.shp"
	parse_shapefile(shapefilename)
	props = get_properties_only(shapefilename,write=True)
