from shapefile_utilities import get_interested_places
from config import metro_shape_full_path, county_shape_full_path, city_shape_full_path
# OKAY

vf_directory = "/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio/full_vf"
vf_filenames = ["SWVF_1_22.csv","SWVF_23_44.csv","SWVF_45_66.csv","SWVF_67_88.csv"]
# vf_filenames = ["utf8_SWVF_23_44.csv"]
vf_full_paths = [f"{vf_directory}/{vf_filename}" for vf_filename in vf_filenames]
# SWVF_1_22 (Adams-Erie)
# SWVF_23_44 (Fairfield-Lawrence)
# SWVF_45_66 (Licking-Pike)
# SWVF_67_88 (Portage-Wyandot)
polling_place_directory = "/Users/zach/Documents/UCLA_classes/research/Polling/metro_shapefiles/results" #"../../metro_shapefiles/results/"
### COLUMBUS, OHIO
def get_columbus():
	mapfilename = 'map_metro_county_city_columbus.html'
	interested_metro_properties = { "CSAFP": "198", "CBSAFP": "18140", "GEOID": "18140", "NAME": "Columbus, OH", "NAMELSAD": "Columbus, OH Metro Area", "LSAD": "M1", "MEMI": "1", "MTFCC": "G3110", "ALAND": 12423081966, "AWATER": 138975190, "INTPTLAT": "+39.9685617", "INTPTLON": "-082.8359105" }
	interested_county_properties = [
		{ "STATEFP": "39", "COUNTYFP": "049", "COUNTYNS": "01074037", "AFFGEOID": "0500000US39049", "GEOID": "39049", "NAME": "Franklin", "LSAD": "06", "ALAND": 1378944435, "AWATER": 29034069 },
		{ "STATEFP": "39", "COUNTYFP": "041", "COUNTYNS": "01074033", "AFFGEOID": "0500000US39041", "GEOID": "39041", "NAME": "Delaware", "LSAD": "06", "ALAND": 1147779363, "AWATER": 36698376 },
		{ "STATEFP": "39", "COUNTYFP": "117", "COUNTYNS": "01074071", "AFFGEOID": "0500000US39117", "GEOID": "39117", "NAME": "Morrow", "LSAD": "06", "ALAND": 1051881046, "AWATER": 2819351 },
		# { "STATEFP": "39", "COUNTYFP": "101", "COUNTYNS": "01074063", "AFFGEOID": "0500000US39101", "GEOID": "39101", "NAME": "Marion", "LSAD": "06", "ALAND": 1045726872, "AWATER": 908577 },
		{ "STATEFP": "39", "COUNTYFP": "159", "COUNTYNS": "01074091", "AFFGEOID": "0500000US39159", "GEOID": "39159", "NAME": "Union", "LSAD": "06", "ALAND": 1118226926, "AWATER": 13266460 },
		{ "STATEFP": "39", "COUNTYFP": "097", "COUNTYNS": "01074061", "AFFGEOID": "0500000US39097", "GEOID": "39097", "NAME": "Madison", "LSAD": "06", "ALAND": 1206498201, "AWATER": 2066974 },
		{ "STATEFP": "39", "COUNTYFP": "129", "COUNTYNS": "01074077", "AFFGEOID": "0500000US39129", "GEOID": "39129", "NAME": "Pickaway", "LSAD": "06", "ALAND": 1298196084, "AWATER": 13769343 },
		{ "STATEFP": "39", "COUNTYFP": "045", "COUNTYNS": "01074035", "AFFGEOID": "0500000US39045", "GEOID": "39045", "NAME": "Fairfield", "LSAD": "06", "ALAND": 1306415009, "AWATER": 10754493 },
		{ "STATEFP": "39", "COUNTYFP": "089", "COUNTYNS": "01074057", "AFFGEOID": "0500000US39089", "GEOID": "39089", "NAME": "Licking", "LSAD": "06", "ALAND": 1767505133, "AWATER": 12752911 },
		{ "STATEFP": "39", "COUNTYFP": "073", "COUNTYNS": "01074049", "AFFGEOID": "0500000US39073", "GEOID": "39073", "NAME": "Hocking", "LSAD": "06", "ALAND": 1091232036, "AWATER": 5959241 },
		{ "STATEFP": "39", "COUNTYFP": "127", "COUNTYNS": "01074076", "AFFGEOID": "0500000US39127", "GEOID": "39127", "NAME": "Perry", "LSAD": "06", "ALAND": 1056544342, "AWATER": 11790392 },
		# { "STATEFP": "39", "COUNTYFP": "081", "COUNTYNS": "01074053", "AFFGEOID": "0500000US39081", "GEOID": "39081", "NAME": "Jefferson", "LSAD": "06", "ALAND": 1057037026, "AWATER": 7335031 }
	]
	interested_city_properties = { "STATEFP": "39", "PLACEFP": "18000", "PLACENS": "01086101", "GEOID": "3918000", "NAME": "Columbus", "NAMELSAD": "Columbus city", "LSAD": "25", "CLASSFP": "C6", "PCICBSA": "Y", "PCINECTA": "N", "MTFCC": "G4110", "FUNCSTAT": "A", "ALAND": 564391903, "AWATER": 15421972, "INTPTLAT": "+39.9839384", "INTPTLON": "-082.9848818" }
	# County Codes
	# https://jfs.ohio.gov/PerformanceCenter/FastFacts/Ohio_Counties_with_County_Codes.pdf
	return mapfilename, interested_metro_properties, interested_county_properties, interested_city_properties


### CLEVELAND-ELYRIA, OHIO
def get_cleveland():
	mapfilename = 'map_metro_county_city_cleveland.html'
	interested_metro_properties = { "CSAFP": "184", "CBSAFP": "17460", "GEOID": "17460", "NAME": "Cleveland-Elyria, OH", "NAMELSAD": "Cleveland-Elyria, OH Metro Area", "LSAD": "M1", "MEMI": "1", "MTFCC": "G3110", "ALAND": 5178438794, "AWATER": 5128507540, "INTPTLAT": "+41.7603919", "INTPTLON": "-081.7242168" }
	interested_county_properties = [
		{ "STATEFP": "39", "COUNTYFP": "035", "COUNTYNS": "01074030", "AFFGEOID": "0500000US39035", "GEOID": "39035", "NAME": "Cuyahoga", "LSAD": "06", "ALAND": 1184103772, "AWATER": 2041800662 },
		{ "STATEFP": "39", "COUNTYFP": "093", "COUNTYNS": "01074059", "AFFGEOID": "0500000US39093", "GEOID": "39093", "NAME": "Lorain", "LSAD": "06", "ALAND": 1272238893, "AWATER": 1119451593 },
		{ "STATEFP": "39", "COUNTYFP": "055", "COUNTYNS": "01074040", "AFFGEOID": "0500000US39055", "GEOID": "39055", "NAME": "Geauga", "LSAD": "06", "ALAND": 1036775608, "AWATER": 20699574 },
		{ "STATEFP": "39", "COUNTYFP": "085", "COUNTYNS": "01074055", "AFFGEOID": "0500000US39085", "GEOID": "39085", "NAME": "Lake", "LSAD": "06", "ALAND": 593807216, "AWATER": 1942301618 },
		{ "STATEFP": "39", "COUNTYFP": "103", "COUNTYNS": "01074064", "AFFGEOID": "0500000US39103", "GEOID": "39103", "NAME": "Medina", "LSAD": "06", "ALAND": 1091406482, "AWATER": 4198456 }
	]
	interested_city_properties = { "STATEFP": "39", "PLACEFP": "16000", "PLACENS": "01085963", "GEOID": "3916000", "NAME": "Cleveland", "NAMELSAD": "Cleveland city", "LSAD": "25", "CLASSFP": "C2", "PCICBSA": "Y", "PCINECTA": "N", "MTFCC": "G4110", "FUNCSTAT": "A", "ALAND": 201255195, "AWATER": 12332138, "INTPTLAT": "+41.4784623", "INTPTLON": "-081.6794351" }
	# County Codes
	# https://jfs.ohio.gov/PerformanceCenter/FastFacts/Ohio_Counties_with_County_Codes.pdf

	return mapfilename, interested_metro_properties, interested_county_properties, interested_city_properties


# def _get_county_names_():
# 	return [county['NAME'] for county in interested_county_properties]

def get_metro_info():
	return get_columbus()
	# return get_cleveland()



mapfilename, interested_metro_properties, interested_county_properties, interested_city_properties = get_metro_info()


mapfolder = "map_graphics"
mapfolder2 = "/Users/zach/code/web_visualizations"
mapPath = f'{mapfolder}/{mapfilename}'
mapPath2 = f"{mapfolder2}/{mapfilename}"



def get_interested_metros():
	return get_interested_places(metro_shape_full_path,interested_metro_properties)

def get_interested_metro():
	return get_interested_metros()[0]

def get_interested_counties():
	return get_interested_places(county_shape_full_path,interested_county_properties)

def get_interested_cities():
	return get_interested_places(city_shape_full_path,interested_city_properties)






# # County Codes
# # https://jfs.ohio.gov/PerformanceCenter/FastFacts/Ohio_Counties_with_County_Codes.pdf
# rich_data = {
# 	"Columbus" : {'counties' : ['Franklin'], 'county_codes' : ['25'],'zoom': 9},
# 	"Cleveland" : {	'counties' : ['Cuyahoga', 'Lorain'], 'county_codes' : [18, 47],'zoom': 9}
# }

# HEADERS:
# 	SOS_VOTERID	COUNTY_NUMBER	COUNTY_ID	LAST_NAME	FIRST_NAME	MIDDLE_NAME	SUFFIX	DATE_OF_BIRTH	REGISTRATION_DATE	VOTER_STATUS	PARTY_AFFILIATION	RESIDENTIAL_ADDRESS1	RESIDENTIAL_SECONDARY_ADDR	RESIDENTIAL_CITY	RESIDENTIAL_STATE	RESIDENTIAL_ZIP	RESIDENTIAL_ZIP_PLUS4	RESIDENTIAL_COUNTRY	RESIDENTIAL_POSTALCODE	MAILING_ADDRESS1	MAILING_SECONDARY_ADDRESS	MAILING_CITY	MAILING_STATE	MAILING_ZIP	MAILING_ZIP_PLUS4	MAILING_COUNTRY	MAILING_POSTAL_CODE	CAREER_CENTER	CITY	CITY_SCHOOL_DISTRICT	COUNTY_COURT_DISTRICT	CONGRESSIONAL_DISTRICT	COURT_OF_APPEALS	EDU_SERVICE_CENTER_DISTRICT	EXEMPTED_VILL_SCHOOL_DISTRICT	LIBRARY	LOCAL_SCHOOL_DISTRICT	MUNICIPAL_COURT_DISTRICT	PRECINCT_NAME	PRECINCT_CODE	STATE_BOARD_OF_EDUCATION	STATE_REPRESENTATIVE_DISTRICT	STATE_SENATE_DISTRICT	TOWNSHIP	VILLAGE	WARD	PRIMARY-03/07/2000	GENERAL-11/07/2000	SPECIAL-05/08/2001	GENERAL-11/06/2001	PRIMARY-05/07/2002	GENERAL-11/05/2002	SPECIAL-05/06/2003	GENERAL-11/04/2003	PRIMARY-03/02/2004	GENERAL-11/02/2004	SPECIAL-02/08/2005	PRIMARY-05/03/2005	PRIMARY-09/13/2005	GENERAL-11/08/2005	SPECIAL-02/07/2006	PRIMARY-05/02/2006	GENERAL-11/07/2006	PRIMARY-05/08/2007	PRIMARY-09/11/2007	GENERAL-11/06/2007	PRIMARY-11/06/2007	GENERAL-12/11/2007	PRIMARY-03/04/2008	PRIMARY-10/14/2008	GENERAL-11/04/2008	GENERAL-11/18/2008	PRIMARY-05/05/2009	PRIMARY-09/08/2009	PRIMARY-09/15/2009	PRIMARY-09/29/2009	GENERAL-11/03/2009	PRIMARY-05/04/2010	PRIMARY-07/13/2010	PRIMARY-09/07/2010	GENERAL-11/02/2010	PRIMARY-05/03/2011	PRIMARY-09/13/2011	GENERAL-11/08/2011	PRIMARY-03/06/2012	GENERAL-11/06/2012	PRIMARY-05/07/2013	PRIMARY-09/10/2013	PRIMARY-10/01/2013	GENERAL-11/05/2013	PRIMARY-05/06/2014	GENERAL-11/04/2014	PRIMARY-05/05/2015	PRIMARY-09/15/2015	GENERAL-11/03/2015	PRIMARY-03/15/2016	GENERAL-06/07/2016	PRIMARY-09/13/2016	GENERAL-11/08/2016	PRIMARY-05/02/2017	PRIMARY-09/12/2017	GENERAL-11/07/2017	PRIMARY-05/08/2018	GENERAL-08/07/2018	GENERAL-11/06/2018	PRIMARY-05/07/2019	PRIMARY-09/10/2019	GENERAL-11/05/2019	PRIMARY-03/17/2020

