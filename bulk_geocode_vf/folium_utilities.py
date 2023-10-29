import subprocess
import folium
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from geojson_utilities import get_base_coords


# https://www.census.gov/library/reference/code-lists/legal-status-codes.html#:~:text=Component%20ID%3A%20%23ti1521383369,legislation%2C%20resolution%2C%20or%20ordinance.
LSAD_keys = {'06' : 'county', '25' : 'city', 'M1': 'metro'}

def get_n_colors(number_desired_colors):
	if number_desired_colors < 10:
		default = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
		colors = default[0:number_desired_colors]
	else:
		# https://matplotlib.org/tutorials/colors/colormaps.ht
		cmap = plt.cm.get_cmap('nipy_spectral',number_desired_colors)
		colors = [matplotlib.colors.rgb2hex(cmap(i)) for i in range(number_desired_colors)]
	return colors



def plot_shapes(featureCollections):
	# styles = [{'fillColor': '#228B22', 'color': '#228B22'}, {'fillColor': '#00FFFF', 'color': '#00FFFF'},{'fillColor': '#ff0000', 'color': '#ff0000'},{'fillColor': '#f6ff00', 'color': '#f6ff00'},{'fillColor': '#fc03df', 'color': '#fc03df'}]
	num_colors = len(featureCollections)
	colors = get_n_colors(num_colors)
	lat,lon = get_base_coords(featureCollections[0])
	m = folium.Map(
	               location=[lat, lon],
	               zoom_start=9,
	               prefer_canvas=True
	               )
	for featureThing in featureCollections:
		if featureThing['type'] == 'FeatureCollection':
			featureThingName = '(Multiple Features)'
		else:
			try:
				featureThingName = f"{featureThing['properties']['NAME']}  ({LSAD_keys[featureThing['properties']['LSAD']]})"
			except Exception:
				featureThingName = "(Feature)"
		color=colors.pop()
		legend_entry = f"<span style='color: {color};'>{featureThingName}</span>"
		overlay = folium.GeoJson(
			featureThing,
			name=legend_entry,
			# legend_name=featureThing['properties']['NAME'],
			style_function= lambda x,style={'fillColor' : color, 'color': color}: style
		)
		overlay.add_to(m)
	return m

def view_plot(map_object,append_random=True):
	tmpdir = subprocess.check_output("echo $TMPDIR",shell=True,text=True).strip()
	if append_random:
		rand_id = str(np.random.randint(0,10000))
	else:
		rand_id = ''
	tmpfile = f"{tmpdir}folium_map{rand_id}.html"
	map_object.save(tmpfile)
	open_in_chrome(tmpfile)

def save_plot(map_object,path):
	map_object.save(path)

def save_and_view_plot(map_object,path,path2=None):
	map_object.save(path)
	if path2 is not None:
		subprocess.run(['cp',path,path2])
	open_in_chrome(path)


def open_in_chrome(filename):
	subprocess.run(['osascript', '/Users/Zach/Library/Scripts/OpenFileInChrome.applescript', filename])


def extract_geometry(feature):
	return feature['geometry']['coordinates']
