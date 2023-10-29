# import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mpc
import numpy as np
import geopandas as gp
import plotly.express as px
import plotly.graph_objects as go
import json
import io
import subprocess
from config import mapbox_api_key
from geojson_utilities import feature_to_featureCollection, get_base_coords, get_nameLSAD
## Google My Maps
# https://www.google.com/maps/d/u/0/edit?mid=1wRwl1SHrl-nHEqFgkSXv-OwVluNZDaW7&ll=34.24991125594836%2C-118.10906613178243&z=10

## Plotly
# https://plotly.com/python/mapbox-layers/


def initialize_map(metro):
	px.line_mapbox()


def geojson_to_dataframe(geojson_obj):
	return gp.read_file(io.StringIO(json.dumps(geojson_obj)))

def metro_to_dataframe(metro):
	# f = io.StringIO()
	# json.dump(metro,f)
	# return gp.read_file(f)
	return geojson_to_dataframe(metro)


def init_fig():
	return go.Figure(go.Scattermapbox())
# https://plotly.com/python/filled-area-on-mapbox/
def metro_mapbox(metro):
	centerLat,centerLon = get_base_coords(metro)
	nameLSAD = get_nameLSAD(metro)
	fig = init_fig()
	fig.update_layout(
		mapbox = {
			'style': "open-street-map",
			'center': { 'lon': centerLon, 'lat': centerLat},
			'zoom': 12, 'layers': [{
				'source': feature_to_featureCollection(metro),
				'type': "fill", 'below': "traces", 'color': "royalblue", 'opacity' : 0.15}
				]
				},
		margin = {'l':0, 'r':0, 'b':0, 't':0},
		title=nameLSAD
		)
	# styles = ['dark', 'basic','streets','outdoors','light','satellite','satellite-streets']
	fig.update_layout(mapbox_style="light", mapbox_accesstoken=mapbox_api_key)

	# fig.update_layout(mapbox_style="open-street-map")
	return fig

# TODO: DO NOT OVERWRITE FIGURE

# Themes: https://plotly.com/python/mapbox-layers/
# ["basic", "streets", "outdoors", "light", "dark", "satellite", or "satellite-streets"]
def add_metro_to_fig(fig,metro,mapbox_style='light'):
	centerLat,centerLon = get_base_coords(metro)
	nameLSAD = get_nameLSAD(metro)
	fig.update_layout(
		mapbox = {
			'style': "open-street-map",
			'center': { 'lon': centerLon, 'lat': centerLat},
			'zoom': 5, 'layers': [{
				'source': metro,
				'type': "fill", 'below': "traces", 'color': "royalblue", 'opacity' : 0.25}]
				},
		margin = {'l':0, 'r':0, 'b':0, 't':0},
		title=nameLSAD
		)
	# styles = ['dark', 'basic','streets','outdoors','light','satellite','satellite-streets']
	fig.update_layout(mapbox_style=mapbox_style, mapbox_accesstoken=mapbox_api_key)

	# fig.update_layout(mapbox_style="open-street-map")
	return fig

# Themes: https://plotly.com/python/mapbox-layers/
# ["basic", "streets", "outdoors", "light", "dark", "satellite", or "satellite-streets"]
def add_multiple_metros_to_fig(fig,metros,mapbox_style='light'):
	colors = get_n_colors(len(metros)+4)
	centerLat,centerLon = get_base_coords(metros[0])
	fig.update_layout(
		mapbox = {
			'style': "open-street-map",
			'center': { 'lon': centerLon, 'lat': centerLat},
			'zoom': 5,
			'layers': [
				{
					'source': metro,
					'type': "fill", 'below': "traces", 'color': colors.pop(), 'opacity' : 0.25
				}
			for metro in metros ]
		},
		margin = {'l':0, 'r':0, 'b':0, 't':0},
		title="Multiple Metropolitan Regions"
	)
	# styles = ['dark', 'basic','streets','outdoors','light','satellite','satellite-streets']
	fig.update_layout(mapbox_style=mapbox_style, mapbox_accesstoken=mapbox_api_key)

	# fig.update_layout(mapbox_style="open-street-map")
	return fig


# def get_n_colors(number_desired_colors):
# 	return ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"][0:number_desired_colors]

def get_n_colors(number_desired_colors,defaults=False):
	if defaults and number_desired_colors < 10:
		default = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
		colors = default[0:number_desired_colors]
		return colors
	cmap = plt.cm.get_cmap('nipy_spectral',number_desired_colors)
	colors = [mpc.to_hex(cmap(i),keep_alpha=False) for i in range(number_desired_colors)]
	return colors


def show_fig(fig):
	# fig.show()
	config = {'displayModeBar': False}
	fig.show(config=config)


def add_scatter(fig,df,hover_name=None,hover_data=None,layer_name=None,color=None,text=None,lat='latitude',lon='longitude'):
	scat = px.scatter_mapbox(df, lat=lat, lon=lon, color=color, hover_data=hover_data, hover_name=hover_name,text=text)
	change_markers_traces(scat.data)
	fig.add_traces(scat.data)

	# for layer in scat.data:
	#	fig.add_trace(layer)

def add_scatter_pps(fig,df,has_metro_as_column=False):
	hover_text_column = df.apply(lambda row: f"{row.addressLocationname}<br>{row.addressLine1}<br>{row.addressCity}, {row.addressState} {row.addressZip}<br>{row.placeType}@({row.latitude},{row.longitude})",axis=1)
	df['Location'] = hover_text_column
	if has_metro_as_column:
		df['Location'] = df.apply(lambda row: f"{row.Location}<br>{row.metro}",axis=1)
	hover_data = {'placeType' : False, 'latitude' : False,'longitude' : False, 'Location' : True}
	add_scatter(fig,df,hover_name="addressLocationname",hover_data=hover_data,color='placeType',lat='latitude',lon='longitude')
	df.drop('Location',axis=1)




def save_plot(map_object,path):
	config = {'displayModeBar': False}
	map_object.write_html(path,config=config)


def open_in_chrome(filename):
	try:
		subprocess.run(['osascript', '/Users/Zach/Library/Scripts/OpenFileInChrome.applescript', filename])
	except:
		print(f"Not on correct system!")

# doesn't work except for 'circle', the default...
# https://community.plotly.com/t/how-to-add-a-custom-symbol-image-inside-map/6641
def change_markers(map_object):
	traces = list(map_object.select_traces())[1:]
	# marker_names = ["circle","triangle","triangle-stroked"]
	sizes = list(np.linspace(5,10,num=len(traces)))
	# markers =  [go.scattermapbox.Marker({'size': 10, 'symbol': [symb]}) for symb in marker_names]
	for trace in traces:
		# trace.update(marker_symbol=marker_names.pop())
		# fig.update_traces(marker=dict(size=12,line=dict(width=2,color='DarkSlateGrey')),selector=dict(mode='markers'))
		trace.update(marker=dict(size = sizes.pop()))

def change_markers_traces(traces):
	# traces = list(map_object.select_traces())[1:]
	# marker_names = ["circle","triangle","triangle-stroked"]
	sizes = list(np.linspace(3,8,num=len(traces)))
	# markers =  [go.scattermapbox.Marker({'size': 10, 'symbol': [symb]}) for symb in marker_names]
	for trace in traces:
		# trace.update(marker_symbol=marker_names.pop())
		# fig.update_traces(marker=dict(size=12,line=dict(width=2,color='DarkSlateGrey')),selector=dict(mode='markers'))
		trace.update(marker=dict(size = sizes.pop()))

def generate_plot(metro,unique_places):
	fig = metro_mapbox(metro)
	add_scatter_pps(fig,unique_places)
	return fig


def add_grid_scatter(fig,grid):
	df = geojson_to_dataframe(grid)
	df['lat'] = [p.y for p in df.geometry]
	df['lon'] = [p.x for p in df.geometry]
	add_scatter(fig,df,lat='lat',lon='lon')


def generate_plot_of_grid(metro,grid):
	fig = metro_mapbox(metro)
	add_grid_scatter(fig,grid)
	return fig

def add_all_to_plot(fig, metro,unique_places):
	fig = add_metro_to_fig(fig,metro)
	add_scatter_pps(fig,unique_places)
	return fig
# # RAW OPENSTREETMAP
# def OSM_demo():
# 	us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")
# 	fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
# 	                        color_discrete_sequence=["fuchsia"], zoom=3, height=300)
# 	fig.update_layout(mapbox_style="open-street-map")
# 	fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
# 	fig.show()

# ## OPENSTREETMAP + MAPBOX
# def OSM_Mapbox_demo():
# 	us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")
# 	fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
# 	                        color_discrete_sequence=["fuchsia"], zoom=3, height=300)
# 	fig.update_layout(
# 	    mapbox_style="white-bg",
# 	    mapbox_layers=[
# 	        {
# 	            "below": 'traces',
# 	            "sourcetype": "raster",
# 	            "sourceattribution": "United States Geological Survey",
# 	            "source": [
# 	                "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}"
# 	            ]
# 	        }
# 	      ])
# 	fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
# 	fig.show()


# ## DARK TILES DEMO
# def dark_tiles_demo():
# 	us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")
# 	fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
# 	                        color_discrete_sequence=["fuchsia"], zoom=3, height=300)
# 	fig.update_layout(mapbox_style="dark", mapbox_accesstoken=mapbox_api_key)
# 	fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
# 	fig.show()



