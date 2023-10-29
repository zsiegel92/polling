import requests
from config import geocoding_api_key


### THIS FUNCTION ADDS A SMALL AMOUNT OF ADDITIONAL DATA TO EACH ENTRY IN THE METRO .json FILE


def add_geo_context(city):
	lat = city['properties']['INTPTLAT']
	lon = city['properties']['INTPTLON']
	url=f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={geocoding_api_key}"
	data = requests.get(url).json()
	if 'results' not in data:
		print('Error retrieving reverse geocoding for city')
	else:
		result = data['results'][0]
		address_components = result['address_components']
		for component in address_components:
			if "administrative_area_level_1" in component['types']:
				city['properties']['STATELONG'] = component['long_name']
				city['properties']['STATESHORT'] = component['short_name']
				print(f"ADDED STATE {city['properties']['STATESHORT']} to CITY {city['properties']['NAME']}")
			if "administrative_area_level_2" in component['types']:
				city['properties']['COUNTYLONG'] = component['long_name']
				city['properties']['COUNTYSHORT'] = component['short_name']






# if __name__=="__main__":
#	import folium
# 	from ohio_vf_config import  mapPath, mapPath2, get_interested_metros, get_interested_counties, get_interested_cities
#	from folium_utilities import plot_shapes, view_plot
# 	counties = get_interested_counties()
# 	cities = get_interested_cities()
# 	metros = get_interested_metros()
# 	featureCollections = cities + counties + metros
# 	m = plot_shapes(featureCollections)
# 	folium.LayerControl().add_to(m,mapPath,path2=mapPath2)
# 	# save_and_view_plot(m)
# 	view_plot(m)
# 	# pass
