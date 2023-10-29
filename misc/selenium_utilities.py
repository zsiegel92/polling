from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException
import time
import subprocess


mimetypes = "application/pdf,application/octet-stream,application/x-winzip,application/x-pdf,application/x-gzip,text/csv,application/vnd.ms-excel,text/txt"
# options = Options()
# options.set_preference("browser.helperApps.neverAsk.openFile", mimetypes)
# options.headless = True
# export MOZ_HEADLESS=1 #alternative
# driver = webdriver.Firefox(options = options)

fireFoxOptions = webdriver.FirefoxOptions()
fireFoxOptions.set_headless()
fireFoxOptions.set_preference("browser.helperApps.neverAsk.openFile", mimetypes)
fireFoxOptions.set_preference("browser.helperApps.neverAsk.saveToDisk", mimetypes)
driver = webdriver.Firefox(firefox_options=fireFoxOptions)


driver.implicitly_wait(5)

def click(element,xpath=None,canRecurse=True):
	try:
		element.click()
	except:
		print(f"Failed (1) to click element with text {element.text}")
		try:
			driver.execute_script("arguments[0].click();", element)
		except:
			print(f"Failed (2) to click element with text {element.text}")
			try:
				if xpath is not None and canRecurse:
					time.sleep(5)
					new_element = driver.find_element_by_xpath(xpath)
					click(new_element,canRecurse=False)
			except:
				raise

def click_by_xpath(xpath):
	element = driver.find_element_by_xpath(xpath)
	click(element,xpath=xpath)


def show_alert(title,content,subtitle):
	osa = '''
		display notification "{content}." with title "{title}" subtitle "{subtitle}" sound name "default"'''
	osa = osa.format(title=title,content=content,subtitle=subtitle)
	scpt = f"osascript -e '{osa}'"
	subprocess.run(scpt,shell=True)



def visit_site(url):
	driver.get(url)

def get_html_as_string(url):
	fireFoxOptions = webdriver.FirefoxOptions()
	fireFoxOptions.set_headless()
	driver = webdriver.Firefox(firefox_options=fireFoxOptions)
	driver.get(url)
	return driver.page_source
