import os
import csv
from collections import OrderedDict

def get_headers(filename):
	with open(filename) as csv_file:
		csv_reader = csv.reader(csv_file)
		return next(csv_reader)


def get_rows(filename):
	with open(filename) as csv_file:
		csv_reader = csv.DictReader(csv_file)
		# print([row for row in csv_reader])
		rows = [row for row in csv_reader]
		data_with_polling_location = [OrderedDict(row)for row in rows if ((row.get('pollingLocationName')  is not None) and (row.get('pollingLocationName') != ''))]
	with open(filename) as csv_file:
		csv_reader = csv.DictReader(csv_file)
		rows = [row for row in csv_reader]
		data_without_polling_location = [OrderedDict(row)for row in rows if ((row.get('pollingLocationName') == None) or (row.get('pollingLocationName') == ''))]
	with open(filename) as csv_file:
		csv_reader = csv.DictReader(csv_file)
		rows = [row for row in csv_reader]
		data_all = [OrderedDict(row)for row in rows]
	return {"with_polling_location" : data_with_polling_location,"without_polling_location":data_without_polling_location,"all":data_all}

def write_csv(outfilename,headings,rows):
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headings, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)


