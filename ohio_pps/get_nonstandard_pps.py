import requests
import csv
from bs4 import BeautifulSoup
from selenium_utilities import get_html_as_string

def setmeta(**kwargs):
	def namer(f):
		for k,v in kwargs.items():
			setattr(f,k,v)
		return f
	return namer

@setmeta(county_name="Franklin")
def get_franklin_county():
	url = "https://vote.franklincountyohio.gov/Voters/Polling-Locations"
	# page = requests.get(url)
	page = get_html_as_string(url)
	soup = BeautifulSoup(page, 'html.parser')


	table = soup.find('table')
	# th = table.find('thead').find('tr').find_all('th')
	# headers = [x.text for x in th]
	## Code	Precinct	House	Senate	Congress	Location
	table_rows = table.find('tbody').find_all('tr')
	precinctcounts_keys = ('precinct', 'precname', 'pollplace', 'polladdress', 'DEM', 'REP', 'NON', 'CON', 'GRE', 'LIB', 'SOC', 'TOTAL')
	pps = []
	for tr in table_rows:
		pp = {k : None for k in precinctcounts_keys}
		td = tr.find_all('td')
		address_td = td[-1]
		placename = address_td.find('strong').text
		address_full = address_td.find('a').text.replace("\n","; ")
		pp['precinct'] = td[0].text
		pp['precname'] = td[1].text
		pp['pollplace'] = placename
		pp['polladdress'] =  address_full

		pps.append(pp)

	return pps


def get_morrow_county_page_from_file():
	downloaded_html_filename = 'precinct_pps_from_ohio_counties/Morrow County Precinct Locations - where to vote - Morrow County Sentinel.html'
	with open(downloaded_html_filename,r) as f:
		page = f.read()
	return page
def get_morrow_county_page_from_web():
	url = "https://www.morrowcountysentinel.com/news/4738/morrow-county-precinct-locations-where-to-vote"
	page = get_html_as_string(url)
	return page

@setmeta(county_name="Morrow")
def get_morrow_county():
	# page = get_morrow_county_page_from_file()
	page = get_morrow_county_page_from_web()
	soup = BeautifulSoup(page, 'html.parser')
	main_article = soup.find('article')
	pp_section = main_article.find('article')

	pp_strings = pp_section.find_all('p')
	# headrow = table_rows[0]
	# th = headrow.find_all('th')
	# headers = [i.text for i in th]
	precinctcounts_keys = ('precinct', 'precname', 'pollplace', 'polladdress', 'DEM', 'REP', 'NON', 'CON', 'GRE', 'LIB', 'SOC', 'TOTAL')
	pps = []
	for pp in pp_strings:
		address_full = pp.text
		pp = {k : None for k in precinctcounts_keys}
		pp['polladdress'] =  address_full
		pps.append(pp)
	return pps

@setmeta(county_name="Fairfield")
def get_fairfield_county():
	url = "https://www.fairfieldcountyohioelections.gov/Polls/"
	page = requests.get(url).text
	# soup = BeautifulSoup(page, 'html5lib')
	soup = BeautifulSoup(page, 'html.parser')
	table = soup.find('table', attrs={'class':'grid'})
	table_rows = table.find_all('tr')
	# headrow = table_rows[0]
	# th = headrow.find_all('th')
	# headers = [i.text for i in th]
	precinctcounts_keys = ('precinct', 'precname', 'pollplace', 'polladdress', 'DEM', 'REP', 'NON', 'CON', 'GRE', 'LIB', 'SOC', 'TOTAL')
	pps = []
	for tr in table_rows[1:-1]:
		td = tr.find_all('td')
		row = [i.text for i in td]
		address = row[1]
		address_parts = address.split('\x00')
		for junk in [' Bldg ',' Apt ']:
			try:
				address_parts.remove(junk)
			except ValueError:
				pass
		address_full = ' '.join(address_parts[1:])

		pp = {k : None for k in precinctcounts_keys}
		pp['precinct'] = tr.get('id')
		pp['precname'] = row[0]
		pp['pollplace'] = address_parts[0]
		pp['polladdress'] =  address_full

		pps.append(pp)

	return pps



def write_to_csv(rows,county_name):
	headers = rows[0].keys()
	outfilename = f"precinct_pps_from_ohio_counties/custom_precinctcounts{county_name}.csv"
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)


def write_all():
	# for fn in (get_fairfield_county,get_morrow_county,get_franklin_county):
	for fn in (get_franklin_county,):
		county_name = fn.county_name
		write_to_csv(fn(),county_name)
		print(f"Wrote {county_name}!!")
write_all()




# get_franklin_county()
