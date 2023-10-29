from config import metro_shape_dir, metro_props_filename, metro_populations_full_path, metro_populations_dir
from shapefile_utilities import load_geojson_features
from csv_utilities import get_rows, write_csv
# sorted by metro/micro and land area
def get_metros_by_state_abbrev(state_abbrev):
	metros = load_geojson_features(f"{metro_shape_dir}/{metro_props_filename}")
	state_metros = [metro for metro in metros if state_abbrev in metro['properties']['NAMELSAD']]
	state_metros = sorted(state_metros,key=lambda metro: metro['properties']['ALAND'],reverse=True)
	state_metros = sorted(state_metros,key=lambda metro: metro['properties']['LSAD'],reverse=False)
	return state_metros

def get_metro_populations():
	metroPops = get_rows(metro_populations_full_path,pandas_safe=True)
	return metroPops

def sort_metros_by_pops(metros,metroPops):
	for metro in metros:
		# print(f"Processing {metro['properties']['NAME']}")
		cbsa = metro['properties']['CBSAFP']
		metro_row = metroPops.loc[(metroPops['CBSA']==str(cbsa)) & ((metroPops['LSAD'] == 'Metropolitan Statistical Area') | (metroPops['LSAD'] == 'Micropolitan Statistical Area'))].iloc[0]
		metro['properties']['population'] = metro_row['POPESTIMATE2019']
	return sorted(metros,key=lambda metro: int(metro['properties']['population']),reverse=True)

if __name__=="__main__":
	ga_metros = get_metros_by_state_abbrev('GA')
	metroPops = get_metro_populations()
	ga_metros = sort_metros_by_pops(ga_metros,metroPops)		


	ga_metro_names =[metro['properties']['NAMELSAD'] for metro in ga_metros]

	print("\n".join(ga_metro_names))
	print(len(ga_metro_names))
	for metro in ga_metros:
		area = int(metro['properties']['ALAND']) + int(metro['properties']['AWATER'])
		area_water = int(metro['properties']['AWATER'])
		population = int(metro['properties']['population'])
		density = (population/area) * 1000000
		metro['properties']['pop_per_sq_km'] = f"{density:f}"
		print(f"{metro['properties']['NAMELSAD']}, density {density :.02}, pop {population}, area {area} ({area_water} water)")


	desired_keys = ['NAMELSAD','CBSAFP','population','pop_per_sq_km','ALAND', 'AWATER']
	props = [{k: metro['properties'][k] for k in desired_keys} for metro in ga_metros ]


	write_csv(f'{metro_populations_dir}/georgia_metro_population_density.csv',rows=props)


	# 12060			Atlanta-Sandy Springs-Alpharetta, GA	Metropolitan Statistical Area	5286728	5286718	5302598	5366462	5444473	5510530	5593204	5686048	5787965	5872432	5945303	6020364	15880	63864	78011	66057	82674	92844	101917	84467	72871	75061	18132	73833	72554	71136	72496	73726	73312	73339	72422	72487	7717	32074	31386	33699	34075	36037	36493	37477	39676	41038	10415	41759	41168	37437	38421	37689	36819	35862	32746	31449	3052	9746	14289	16599	12543	18234	26353	16783	13908	12541	2099	12256	22129	11894	31819	37014	38767	31808	26200	31043	5151	22002	36418	28493	44362	55248	65120	48591	40108	43584	314	103	425	127	-109	-93	-22	14	17	28




	# Rome, GA Metro Area
	# Hinesville, GA Metro Area
	# Statesboro, GA Micro Area
	# Jefferson, GA Micro Area
	# Dublin, GA Micro Area
	# Augusta-Richmond County, GA-SC Metro Area
	# Atlanta-Sandy Springs-Alpharetta, GA Metro Area
	# Calhoun, GA Micro Area
	# Chattanooga, TN-GA Metro Area
	# Waycross, GA Micro Area
	# St. Marys, GA Micro Area
	# Milledgeville, GA Micro Area
	# Douglas, GA Micro Area
	# Moultrie, GA Micro Area
	# Cornelia, GA Micro Area
	# Thomasville, GA Micro Area
	# Cedartown, GA Micro Area
	# Tifton, GA Micro Area
	# Savannah, GA Metro Area
	# Vidalia, GA Micro Area
	# Americus, GA Micro Area
	# Columbus, GA-AL Metro Area
	# Jesup, GA Micro Area
	# Eufaula, AL-GA Micro Area
	# Bainbridge, GA Micro Area
	# Thomaston, GA Micro Area
	# Toccoa, GA Micro Area
	# Summerville, GA Micro Area
	# Macon-Bibb County, GA Metro Area
	# Cordele, GA Micro Area
	# Athens-Clarke County, GA Metro Area
	# Gainesville, GA Metro Area
	# Warner Robins, GA Metro Area
	# Fitzgerald, GA Micro Area
	# Valdosta, GA Metro Area
	# Albany, GA Metro Area
	# Dalton, GA Metro Area
	# Brunswick, GA Metro Area
	# LaGrange, GA-AL Micro Area
