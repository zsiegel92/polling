# Find these here:
# https://api.census.gov/data/2019/acs/acs5/variables.json
import requests
import json
from pprint import pprint
import pandas as pd

from config import census_api_key,census_codes_file,census_prefetch_dir,census_acs_file,census_acs_file_raw,census_tract_tiger_data

from census_utilities import build_census_query_url
from census_codes import groups, codes as demographic_data_tables_lookup, codes_old as demographic_data_tables_lookup_old

def get_codes():
	url = "https://api.census.gov/data/2019/acs/acs5/variables.json"
	variables = requests.get(url).json()['variables']
	# variables = {k : v.split("Estimate!!",1)[1] for k,v in variables.items()}
	json.dump(variables,open(f"{census_prefetch_dir}/census_variables_all.json",'w'),indent=5)
	group_vars = {variable : content for variable,content in variables.items() if content['group'] in groups }
	code_vars = {variable: variables[variable] for variable in demographic_data_tables_lookup_old}
	desired_variables = {**group_vars,**code_vars}
	max_col_length = 42
	for k, d in desired_variables.items():
		d['label'] = d['label'].split("Estimate!!",1)[1].replace('!','').replace(':','_').replace(' ','_').replace('(','_').replace(')','_').replace('__','_')
		if len(d['label']) > max_col_length:
			d['label'] = d['label'][:max_col_length]
	json.dump(desired_variables,open(f"{census_prefetch_dir}/census_variables.json","w"),indent=5)
	variables_keys = { variable: content['label'] for variable,content in desired_variables.items()}
	json.dump(variables_keys,open(census_codes_file,"w"),indent=5)
	# concepts = [content['concept'] for content in group_vars.values()]



def save_all_census_data():
	url_acs = build_census_query_url(tract="*",state="13")
	r = requests.get(url_acs)
	r = r.json()
	rows = (row for row in r)
	headers = next(rows)
	rows = [row for row in rows]
	# variables = json.load(open('census_variables.json','r'))
	# legible_headers = [variables[header]['label'] if header in variables else header for header in headers]
	df = pd.DataFrame(dict(zip(headers,row)) for row in rows)
	df.to_csv(census_acs_file_raw,index=False)

	pkeys = ["GEO_ID","NAME","state","county","tract"]
	legible_indices = [i for i in range(len(headers)) if headers[i] in demographic_data_tables_lookup or headers[i] in pkeys]
	legible_headers = [demographic_data_tables_lookup[headers[i]] if headers[i] in demographic_data_tables_lookup else headers[i] for i in legible_indices]
	legible_rows = [[row[i] for i in legible_indices] for row in rows]
	df = pd.DataFrame(dict(zip(legible_headers,row)) for row in legible_rows)
	df.to_csv(census_acs_file,index=False)

# def get_all_census_info():
if __name__=="__main__":
	# get_codes()
	# save_all_census_data()
	url_acs = build_census_query_url(tract="*",state="13")
	r = requests.get(url_acs)
	r = r.json()
	pass

# url_acs='https://api.census.gov/data/2019/acs/acs5?get=group(B01001),B01003_001E,B19013_001E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B02001_006E,B02001_007E,B03001_002E,B03001_003E,B08014_002E,B08014_003E,B08014_004E,B08014_005E,B08014_006E,B08014_007E,B08015_001E,B08006_002E,B08006_008E,B08006_014E,B08006_015E,B08006_016E,B08006_017E,B15003_017E,B15003_018E,B15003_019E,B15003_020E,B15003_021E,B15003_022E,B15003_023E,B15003_024E,B15003_025E&for=tract:*&in=state:13&key=e9cbebf0cdc461ed9e80991e8526774ab568c690'

# url_acs='https://api.census.gov/data/2019/acs/acs5?get=group(B01001),B01003_001E,B19013_001E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B02001_006E,B02001_007E,B03001_002E,B03001_003E,B08014_002E,B08014_003E,B08014_004E,B08014_005E,B08014_006E,B08014_007E,B08015_001E,B08006_002E,B08006_008E,B08006_014E,B08006_015E,B08006_016E,B08006_017E,B15003_017E,B15003_018E,B15003_019E,B15003_020E,B15003_021E,B15003_022E,B15003_023E,B15003_024E,B15003_025E&for=tract:*&in=state:13&key=e9cbebf0cdc461ed9e80991e8526774ab568c690'
