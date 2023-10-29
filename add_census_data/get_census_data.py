import requests
from utilities import get_headers,get_rows,write_csv


census_api_key = os.environ.get("CENSUS_API_KEY") # CENSUS API
resultsdir = "results"
filename = "lafayette_la_no-census.csv"
limit = 5

# census_api_query_url = "https://api.census.gov/data/2017/acs/acs5?&get="
# get_items = [f"group({group})" for group in groups]
# get_items.extend([key for key in codes if not any([key.startswith(groupkey) for groupkey in groups])])
# census_api_query_url += get_items.join(',');



file_location = resultsdir + "/" + filename
outfilename = file_location.split(".")[0] + "_census_added.csv"


headings = get_headers(file_location)
rows = get_rows(file_location)
polling_rows =rows['with_polling_location']

i = 0
for row in polling_rows:
	i += 1
	if i <= limit:
		lat = row['latitude']
		lng = row['longitude']
		url = "https://us-central1-corded-palisade-136523.cloudfunctions.net/add_census_data?census_api_key="+census_api_key+"&lat="+lat+"&lng="+lng
		response = requests.get(url)
		print(response)
		data = response.json()
		for key in row:
			if key in data:
				row[key]= data[key]



write_csv(outfilename,headings,polling_rows[0:limit])
