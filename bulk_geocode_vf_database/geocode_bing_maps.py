# 50,000 billable transactions per 24-hour period with Education license.
# http://mapsforenterprise.binginternal.com/en-us/maps/licensing/licensing-options

# Compare to Google Distance Matrix: $250 / 50,0000
# https://developers.google.com/maps/documentation/distance-matrix/usage-and-billing

## Georgia travelMode='transit' coverage:
## https://docs.microsoft.com/en-us/bingmaps/coverage/transit-coverage/north-america
# Georgia	Athens	University of Georgia
# Georgia	Atlanta	Metropolitan Atlanta Rapid Transit Authority
# Georgia	Atlanta	SRTA
# Georgia	Marietta	Authority
# Georgia	Regional	Gwinnett County Transit
# Georgia	Savannah	Chatham Area Transit

import requests
from pprint import pprint

bing_api_key = os.environ.get('BING_API_KEY')


# origins and destinations are iterables of (lat, lon) iterables
# 'travelMode' should be 'driving', 'walking', 'transit'
def dist_mat(origins,destinations,travelMode='driving'):
	url = f'https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix?key={bing_api_key}&origins={";".join(",".join(map(str,origin)) for origin in origins)}&destinations={";".join(",".join(map(str,destination)) for destination in destinations)}&travelMode={travelMode}'
	# print(url)
	r = requests.get(url).json()
	if not r['statusCode'] == 200:
		raise
	numberEstimates = sum(thing['estimatedTotal'] for thing in  r['resourceSets'])
	return r['resourceSets'][0]['resources'][0]['results']




def calc_dist(lat1,lon1, lat2,lon2, travelMode='driving'):
	result =  dist_mat(((lat1,lon1),),((lat2,lon2),),travelMode)
	return {k: result[0][k] for k in ('travelDistance','travelDuration')}

def calc_multi_dist(*args,**kwargs):
	return {travelMode :  calc_dist(*args,**kwargs,travelMode=travelMode) for travelMode in ('driving','walking','transit')}

if __name__=="__main__":
	lat,lon= 34.039844, -118.361659 #home
	lat2, lon2 = 34.073713, -118.445337 #UCLA

	pprint(calc_multi_dist(lat,lon, lat2,lon2))
