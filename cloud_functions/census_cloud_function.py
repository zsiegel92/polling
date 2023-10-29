import requests
import json
import urllib.parse
from traceback import format_exc

def build_census_query_url(demographic_data_tables_lookup):


    groups = [
        "B01001",
    ]

    census_api_query_url = "https://api.census.gov/data/2017/acs/acs5?&get="
    get_items = []
    for group in groups:
        get_items.append("group(" + group + ")")

    for table_id in demographic_data_tables_lookup:
        inGroup = False
        for group in groups:
            if table_id.startswith(group):
                inGroup = True
                break
        if not inGroup:
            get_items.append(table_id)

    census_api_query_url += ','.join(get_items)
    return census_api_query_url


def query(request):

    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    response_headers = {
        'Access-Control-Allow-Origin': '*'
    }

    # table lookup
    demographic_data_tables_lookup = {
        "B01003_001E": "Total Pop",
        "B19013_001E": "Median Income",
        "B01001_002E": "Male Pop",
        "B01001_003E": "Male Pop - Under 5 years",
        "B01001_004E": "Male Pop - 5-9 years",
        "B01001_005E": "Male Pop - 10-14 years",
        "B01001_006E": "Male Pop - 15-17 years",
        "B01001_007E": "Male Pop - 18-19 years",
        "B01001_008E": "Male Pop - 20 years",
        "B01001_009E": "Male Pop - 21 years",
        "B01001_010E": "Male Pop - 22-24 years",
        "B01001_011E": "Male Pop - 25-29 years",
        "B01001_012E": "Male Pop - 30-34 years",
        "B01001_013E": "Male Pop - 35-39 years",
        "B01001_014E": "Male Pop - 40-44 years",
        "B01001_015E": "Male Pop - 45-49 years",
        "B01001_016E": "Male Pop - 50-54 years",
        "B01001_017E": "Male Pop - 55-59 years",
        "B01001_018E": "Male Pop - 60-61 years",
        "B01001_019E": "Male Pop - 62-64 years",
        "B01001_020E": "Male Pop - 65-66 years",
        "B01001_021E": "Male Pop - 67-69 years",
        "B01001_022E": "Male Pop - 70-74 years",
        "B01001_023E": "Male Pop - 75-79 years",
        "B01001_024E": "Male Pop - 80-84 years",
        "B01001_025E": "Male Pop - 85 years and over",
        "B01001_026E": "Female Pop",
        "B01001_027E": "Female Pop - Under 5 years",
        "B01001_028E": "Female Pop - 5-9 years",
        "B01001_029E": "Female Pop - 10-14 years",
        "B01001_030E": "Female Pop - 15-17 years",
        "B01001_031E": "Female Pop - 18-19 years",
        "B01001_032E": "Female Pop - 20 years",
        "B01001_033E": "Female Pop - 21 years",
        "B01001_034E": "Female Pop - 22-24 years",
        "B01001_035E": "Female Pop - 25-29 years",
        "B01001_036E": "Female Pop - 30-34 years",
        "B01001_037E": "Female Pop - 35-39 years",
        "B01001_038E": "Female Pop - 40-44 years",
        "B01001_039E": "Female Pop - 45-49 years",
        "B01001_040E": "Female Pop - 50-54 years",
        "B01001_041E": "Female Pop - 55-59 years",
        "B01001_042E": "Female Pop - 60-61 years",
        "B01001_043E": "Female Pop - 62-64 years",
        "B01001_044E": "Female Pop - 65-66 years",
        "B01001_045E": "Female Pop - 67-69 years",
        "B01001_046E": "Female Pop - 70-74 years",
        "B01001_047E": "Female Pop - 75-79 years",
        "B01001_048E": "Female Pop - 80-84 years",
        "B01001_049E": "Female Pop - 85 years and over",
        "B02001_002E": "White Pop",
        "B02001_003E": "Black Pop",
        "B02001_004E": "American Indian Pop",
        "B02001_005E": "Asian Pop",
        "B02001_006E": "Native Hawaiian Pop",
        "B02001_007E": "Other Pop",
        "B03001_002E": "Not Hispanic Pop",
        "B03001_003E": "Hispanic Pop",
        "B08014_002E": "No vehicles",
        "B08014_003E": "1 vehicle",
        "B08014_004E": "2 vehicles",
        "B08014_005E": "3 vehicles",
        "B08014_006E": "4 vehicles",
        "B08014_007E": "5 or more vehicles",
        "B08015_001E": "Aggregate vehicles used in commuting",
        "B08006_002E": "Work commute - Driving",
        "B08006_008E": "Work commute - Transit",
        "B08006_014E": "Work commute - Bike",
        "B08006_015E": "Work commute - Walk",
        "B08006_016E": "Work commute - Other",
        "B08006_017E": "Work commute - Work from home",
        "B15003_017E": "High school diploma",
        "B15003_018E": "GED",
        "B15003_019E": "Some college less than 1 year",
        "B15003_020E": "Some college more than 1 year",
        "B15003_021E": "Associates degree",
        "B15003_022E": "Bachelor's degree",
        "B15003_023E": "Master's degree",
        "B15003_024E": "Professional school degree",
        "B15003_025E": "Doctorate degree"
    }

    # check parameters
    request_json = request.get_json()

    # just add a flag to tell the api to return nothing. We can use this if we want to skip getting census data
    if request.args and 'noReturn' in request.args:
        noReturnFlag = True
    elif request_json and 'noReturn' in request_json:
        noReturnFlag = True
    else:
        noReturnFlag = False

    if noReturnFlag:
        return ('{}', 200, response_headers)

    if request.args and 'census_api_key' in request.args:
        census_api_key = request.args.get('census_api_key')
    elif request_json and 'key' in request_json:
        census_api_key = request_json['census_api_key']
    else:
        out = {
            'status': 'MISSING PARAMETER',
            'message': 'census_api_key parameter is missing from request. Please ensure you include "census_api_key=YOUR_GOOGLE_API_KEY" in your url.',
        }
        out = json.dumps(out, indent=3)
        return (out, 200, response_headers)

    if request.args and 'lat' in request.args:
        lat = request.args.get('lat')
    elif request_json and 'key' in request_json:
        lat = request_json['lat']
    else:
        out = {
            'status': 'MISSING PARAMETER',
            'message': 'lat parameter is missing from request. Please ensure you include "lat=" in your url.',
        }
        out = json.dumps(out, indent=3)
        return (out, 200, response_headers)

    if request.args and 'lng' in request.args:
        lng = request.args.get('lng')
    elif request_json and 'key' in request_json:
        lng = request_json['lng']
    else:
        out = {
            'status': 'MISSING PARAMETER',
            'message': 'lng parameter is missing from request. Please ensure you include "lng=" in your url.',
        }
        out = json.dumps(out, indent=3)
        return (out, 200, response_headers)


    # get base census info url
    census_api_query_url = build_census_query_url(demographic_data_tables_lookup)

    # get census tract id from lat lng
    url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={}&y={}&benchmark=Public_AR_Census2010&vintage=Census2010_Census2010&layers=Census%20Tracts&format=json".format(lng,lat)

    try:
        r = requests.get(url)
        if r.status_code != 200:
            out = {
                'error': "NON 200 STATUS CODE",
                'message': "Query to get census tract ID from lat lng returned status code of: {}".format(r.status_code)
            }
            out = json.dumps(out, indent=3)
            return (out, 200, response_headers)
        else:
            data = r.json()
            if 'result' in data:
                result = data['result']
                if 'geographies' in result:
                    geographies = result['geographies']
                    if 'Census Tracts' in geographies:
                        census_tract_id = geographies['Census Tracts'][0]['TRACT']
                        state_id = geographies['Census Tracts'][0]['STATE']
                        county_id = geographies['Census Tracts'][0]['COUNTY']

                        slug = '&for=tract:' + census_tract_id + '&in=state:' + state_id + '+county:' + county_id
                        url = census_api_query_url + slug + '&key=' + census_api_key

                        r = requests.get(url)
                        if r.status_code != 200:
                            out = {
                                'error': "NON 200 STATUS CODE",
                                'message': "Query to get census details from lat lng returned status code of: {}".format(r.status_code)
                            }
                            out = json.dumps(out, indent=3)
                            return (out, 200, response_headers)
                        else:
                            try:
                                data = r.json()
                                if len(data) > 1:
                                    headers = data[0]
                                    values = data[1]

                                    i = 0
                                    out = {}
                                    for table_id in headers:
                                        if table_id in demographic_data_tables_lookup:
                                            table_name = demographic_data_tables_lookup[table_id]
                                            out[table_name] = values[i]
                                        i += 1

                                    out = json.dumps(out, indent=3)
                                    return (out, 200, response_headers)

                                else:
                                    out = {
                                        'error': "NO RESULTS",
                                        'message': "Query to get census details returned no results."
                                    }
                                    out = json.dumps(out, indent=3)
                                    return (out, 200, response_headers)

                            except:
                                out = {
                                    'error': "COULD NOT PARSE CENSUS DETAIL RESPSONSE",
                                    'message': "Query to get census details was sucessful, but there was an error reading the response. Traceback: {}".format(format_exc())
                                }
                                out = json.dumps(out, indent=3)
                                return (out, 200, response_headers)


                    else:
                        out = {
                            'error': "NO RESULTS",
                            'message': "Query to get census tract ID from lat lng returned no results"
                        }
                        out = json.dumps(out, indent=3)
                        return (out, 200, response_headers)
                else:
                    out = {
                        'error': "NO RESULTS",
                        'message': "Query to get census tract ID from lat lng returned no results"
                    }
                    out = json.dumps(out, indent=3)
                    return (out, 200, response_headers)
            else:
                out = {
                    'error': "NO RESULTS",
                    'message': "Query to get census tract ID from lat lng returned no results"
                }
                out = json.dumps(out, indent=3)
                return (out, 200, response_headers)

    except:
        out = {
            'error': "UKNOWN ERROR",
            'message': "Process failed with unknown error. Traceback: {}".format(format_exc())
        }
        out = json.dumps(out, indent=3)
        return (out, 200, response_headers)
