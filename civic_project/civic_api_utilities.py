import requests

from config import api_key

def get_elections(customAPIkey=None):
	try:
		if customAPIkey:
			url = f'https://www.googleapis.com/civicinfo/v2/elections?key={customAPIkey}'
		else:
			url = f'https://www.googleapis.com/civicinfo/v2/elections?key={api_key}'
		response = requests.get(url).json()
		elections = response['elections']
		return elections
	except Exception as e:
		print(e)
		raise e


# data has keys 'election', 'normalizedInput', 'pollingLocations', 'earlyVoteSites', 'dropOffLocations'
def query_civic_api(address,election_id,customAPIkey=None):
	if customAPIkey:
		params = { 'address' : address, 'election_id' : election_id, 'key' : customAPIkey}
	else:
		params = { 'address' : address, 'election_id' : election_id, 'key' : api_key}
	# print(params['key'])
	# url = f'https://www.googleapis.com/civicinfo/v2/voterinfo?address={address}&electionId={election_id}&key={api_key}'
	url = f'https://www.googleapis.com/civicinfo/v2/voterinfo'
	response = requests.get(url,params=params)
	data = response.json()
	# print("PRINTING DATA")
	# print(data)
	if 'error' in data:
		print(f"ERROR CIVIC API QUERY: {address = } :: {election_id = }")
		print(data)
		return data
	return data



allPlaceTypes = ["pollingLocations", "earlyVoteSites", "dropOffLocations"]


state_abbrevs = {
	'AL': 'Alabama',
	'AK': 'Alaska', 'AZ': 'Arizona',
	'AR': 'Arkansas', 'CA': 'California',
	'CO': 'Colorado', 'CT': 'Connecticut',
	'DE': 'Delaware', 'FL': 'Florida',
	'GA': 'Georgia', 'HI': 'Hawaii',
	'ID': 'Idaho', 'IL': 'Illinois',
	'IN': 'Indiana', 'IA': 'Iowa',
	'KS': 'Kansas', 'KY': 'Kentucky',
	'LA': 'Louisiana', 'ME': 'Maine',
	'MD': 'Maryland', 'MA': 'Massachusetts',
	'MI': 'Michigan', 'MN': 'Minnesota',
	'MS': 'Mississippi', 'MO': 'Missouri',
	'MT': 'Montana', 'NE': 'Nebraska',
	'NV': 'Nevada', 'NH': 'New Hampshire',
	'NJ': 'New Jersey', 'NM': 'New Mexico',
	'NY': 'New York', 'NC': 'North Carolina',
	'ND': 'North Dakota', 'OH': 'Ohio',
	'OK': 'Oklahoma', 'OR': 'Oregon',
	'PA': 'Pennsylvania', 'RI': 'Rhode Island',
	'SC': 'South Carolina', 'SD': 'South Dakota',
	'TN': 'Tennessee', 'TX': 'Texas',
	'UT': 'Utah', 'VT': 'Vermont',
	'VA': 'Virginia', 'WA': 'Washington',
	'WV': 'West Virginia', 'WI': 'Wisconsin',
	'WY': 'Wyoming', 'DC': 'District of Columbia',
	'MH': 'Marshall Islands', 'AE': 'Armed Forces Middle East',
	'AA': 'Armed Forces Americas', 'AP': 'Armed Forces Pacific'
}
