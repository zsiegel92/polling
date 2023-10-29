import requests
import json
import urllib.parse
from traceback import format_exc

def geocode(request):

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

    # check parameters
    request_json = request.get_json()

    if request.args and 'api_key' in request.args:
        api_key = request.args.get('api_key')
    elif request_json and 'census_api_key' in request_json:
        api_key = request_json['api_key']
    else:
        out = {
            'status': 'MISSING PARAMETER',
            'message': 'api_key parameter is missing from request. Please ensure you include "api_key=YOUR_GOOGLE_API_KEY" in your url.',
        }
        out = json.dumps(out, indent=3)
        return (out, 200, response_headers)

    if request.args and 'address' in request.args:
        address = request.args.get('address')
    elif request_json and 'address' in request_json:
        address = request_json['address']
    else:
        address = None




    if address:
        # a normal geocode request
        encoded_address = urllib.parse.quote_plus(address)
        url = "https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}".format(encoded_address,api_key)
        try:
            r = requests.get(url)
            status = r.json()['status']

            if status == 'ZERO_RESULTS':
                out = {
                    'status': 'GEOCODER ERROR',
                    'message': 'No results from geocoder, please make sure address is valid (you could try checking address in'
                               ' Google Maps to see if it returns any results).',
                }
                out = json.dumps(out, indent=3)
                return (out, 200, response_headers)

            if status == 'REQUEST_DENIED':
                out = {
                    'status': 'GEOCODER ERROR',
                    'message': 'Geocoder denied your request. This usually happens when you use an invalid API key. Please '
                               'ensure your Google API key is valid and has the Geocode API enabled. If you are making a '
                               'large number of requests, this error may be triggered by Google rate limiting your requests. '
                               'Consider self limiting requests to no more than 1 per second if you think this is the case.',
                }
                out = json.dumps(out, indent=3)
                return (out, 200, response_headers)

            result = r.json()['results'][0]
            coords = result['geometry']['location']
            lat = coords['lat']
            lng = coords['lng']
            out = {
                'status': 'OK',
                'result': {
                    'lat': lat,
                    'lng': lng
                },
            }
            out = json.dumps(out, indent=3)
            return (out, 200, response_headers)
        except:
            out = {
                'status': "UKNOWN ERROR",
                'message': "Process failed with unknown error. Traceback: {}".format(format_exc())
            }
            out = json.dumps(out, indent=3)
            return (out, 200, response_headers)

    else:
        # must be a reverse geocode request
        if request.args and 'lat' in request.args:
            lat = request.args.get('lat')
        elif request_json and 'lat' in request_json:
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
        elif request_json and 'lat' in request_json:
            lng = request_json['lng']
        else:
            out = {
                'status': 'MISSING PARAMETER',
                'message': 'lng parameter is missing from request. Please ensure you include "lng=" in your url.',
            }
            out = json.dumps(out, indent=3)
            return (out, 200, response_headers)

        url = "https://maps.googleapis.com/maps/api/geocode/json?latlng={},{}&key={}".format(lat,lng, api_key)
        try:
            r = requests.get(url)
            status = r.json()['status']

            if status == 'ZERO_RESULTS':
                out = {
                    'status': 'GEOCODER ERROR',
                    'message': 'No results from geocoder, please make sure address is valid (you could try checking address in'
                               ' Google Maps to see if it returns any results).',
                }
                out = json.dumps(out, indent=3)
                return (out, 200, response_headers)

            if status == 'REQUEST_DENIED':
                out = {
                    'status': 'GEOCODER ERROR',
                    'message': 'Geocoder denied your request. This usually happens when you use an invalid API key. Please '
                               'ensure your Google API key is valid and has the Geocode API enabled. If you are making a '
                               'large number of requests, this error may be triggered by Google rate limiting your requests. '
                               'Consider self limiting requests to no more than 1 per second if you think this is the case.',
                }
                out = json.dumps(out, indent=3)
                return (out, 200, response_headers)

            result = r.json()['results'][0]
            formatted_address = result['formatted_address']
            out = {
                'status': 'OK',
                'result': {
                    'formatted_address': formatted_address,
                },
            }
            out = json.dumps(out, indent=3)
            return (out, 200, response_headers)
        except:
            out = {
                'status': "UKNOWN ERROR",
                'message': "Process failed with unknown error. Traceback: {}".format(format_exc())
            }
            out = json.dumps(out, indent=3)
            return (out, 200, response_headers)
