import subprocess
from selenium_utilities import visit_site, click_by_xpath, NoSuchElementException
from ohio_vf_config import interested_county_properties


def get_county_names():
	return [county['NAME'] for county in interested_county_properties]

def get_county_pps_url(county_name):
	return f"https://lookup.boe.ohio.gov/vtrapp/{county_name}/precandpoll.aspx"

def rename_pps(county_name):
	filename = "/Users/zach/Downloads/precinctcounts{}.csv"
	oldfname,newfname = filename.format(""), filename.format(f"_{county_name}")
	storagefname = f"/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio/precinct_pps_from_ohio_counties/precinctcounts{county_name}.csv"
	subprocess.run(['mv', oldfname, storagefname])
	# subprocess.run(['mv', oldfname, newfname])

county_names = get_county_names()
urls = [get_county_pps_url(county_name) for county_name in county_names]
succeeded, failed = [], []

for county_name in county_names:
	print(f"Getting PPs for {county_name}")
	url = get_county_pps_url(county_name)
	print(f"Visiting: {url}")
	visit_site(url)
	try:
		click_by_xpath('//*[@id="rdo_file"]')
		# click_by_xpath("//label[contains(text(), 'Downloadable File (Comma Separated Values)')]") #works
		# click_by_xpath("//input[contains(text(), 'Download My File')]") #fails
		# click_by_xpath("//input[contains(text(), 'Begin CSV File Creation')]") #fails
		click_by_xpath('//*[@id="btnsubmit"]')
		click_by_xpath('//*[@id="btn_download"]')
		rename_pps(county_name)
		succeeded.append(county_name)
	except NoSuchElementException as e:
		print(f"{county_name} Failed!")
		print(e)
		failed.append(county_name)


print(f"SUCCEEDED: {', '.join(succeeded)}\nFAILED: {', '.join(failed)}")



