import csv
# import os
# from collections import OrderedDict
import chardet
import pandas as pd

def get_headers(filename):
	with open(filename) as csv_file:
		csv_reader = csv.reader(csv_file)
		return next(csv_reader)

def get_rows(filename,safe=False,safe_fast=False,pandas=False):
	print(f"Getting rows from {filename}")
	if pandas:
		return pd.read_excel(filename)
	elif safe_fast:
		return get_rows_safe(filename)
	elif safe:
		headers = get_headers(filename)
		ss = safeCSV(filename)
		next(ss)
		return [dict(zip(headers,row)) for row in ss]
	else:
		with open(filename) as csv_file:
			csv_reader = csv.DictReader(csv_file)
			return [row for row in csv_reader]


def get_rows_safe(filename):
	print(f"Getting rows from {filename}")
	rows = []
	error_rows = []
	with open(filename) as csv_file:
		csv_reader = csv.DictReader(csv_file)
		i = 0
		while True:
			try:
				row = next(csv_reader)
			except UnicodeDecodeError:
				print(f"Error reading row {i}")
				# print(e)
				row = None
				error_rows.append(i)
			except StopIteration:
				break
			rows.append(row)
			i += 1
	if error_rows:
		headers = get_headers(filename)
		ss = safeCSV(filename)
		next(ss)
		for i,row in enumerate(ss):
			if i in error_rows:
				rows[i] = dict(zip(headers,row))
	return rows


## Test of StopIteration
# x = [1,2,3,4]
# it = x.__iter__()
# while True:
# 	try:
# 		print(next(it))
# 	except UnicodeDecodeError:
# 		print("DECODE ERROR")
# 	except StopIteration:
# 		print("STOPPING")
# 		break

def write_csv(outfilename,rows,headers=None,quote_all=False):
	quoting = csv.QUOTE_ALL if quote_all else None
	if headers is None:
		headers = []
		for row in rows:
			headers.extend(k for k in row.keys() if k not in headers)
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore',quoting=quoting)
		fp.writeheader()
		fp.writerows(rows)

def safeCSV(filename):
	# number_errors = 0
	with open(filename,'rb') as openFile:
		for i,line in enumerate(openFile):
				try:
					line = line.decode("utf-8")
				except UnicodeDecodeError as e:
					detected_encoding = chardet.detect(line)["encoding"]
					line = line.decode(detected_encoding)
					# print(f"\n\tError on line {i} in file {openFile.name}\n\t'{line}'\n\t'{e}'\n\t")
					# print(f"\n\tError on line {i} in file {openFile.name}\n\t'{e}'\n\t")
					# number_errors += 1
					print(f"Error reading line {i}")
				csv_reader = csv.reader( [ line ] )
				row = next(csv_reader)
				yield row
