import chardet
from collections import OrderedDict
import csv
import numpy
import os

def safeCSV(filename,return_errors=False):
	number_errors = 0
	with open(filename,'rb') as openFile:
		for i,line in enumerate(openFile):
				try:
					line = line.decode("utf-8")
				except UnicodeDecodeError as e:
					detected_encoding = chardet.detect(line)["encoding"]
					line = line.decode(detected_encoding)
					# print(f"\n\tError on line {i} in file {openFile.name}\n\t'{line}'\n\t'{e}'\n\t")
					# print(f"\n\tError on line {i} in file {openFile.name}\n\t'{e}'\n\t")
					number_errors += 1
				csv_reader = csv.reader( [ line ] )
				row = next(csv_reader)
				if return_errors:
					yield row, number_errors
				else:
					yield row

def get_headers(filename):
	with open(filename) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=",", quotechar='"')
		return next(csv_reader)

def get_rows_as_dicts(filename,return_errors=False):
	headers = get_headers(filename)
	ss = safeCSV(filename,return_errors)
	next(ss)
	return [dict(zip(headers,row)) for row in ss]


def get_sample(filename,nrows=10):
	all_rows_as_dicts = get_rows_as_dicts(filename)
	N = len(all_rows_as_dicts)
	indices = sorted(numpy.random.choice(range(0,N),size=min(N,nrows),replace=False))
	return [all_rows_as_dicts[i] for i in indices]

def write_csv(outfilename,rows):
	headers = rows[0].keys()
	with open(outfilename,"w") as outfile:
		fp = csv.DictWriter(outfile, headers, extrasaction='ignore')
		fp.writeheader()
		fp.writerows(rows)



def replace_csv_with_sample(filename,nrows=1000):
	sample = get_sample(filename,nrows)
	write_csv(filename,sample)
	print(f"Replaced CSV {filename} with {len(sample)} row sample!")


def get_ohio_filenames():
	ohio_dir = "/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio"
	return [f"{ohio_dir}/{filename}" for  filename in os.listdir(ohio_dir) if filename.startswith("SWVF_") and filename.endswith(".csv")]

def get_oklahoma_filenames():
	ok_dir = "/Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Oklahoma"
	subdirs = [element for element in os.listdir(ok_dir) if element.startswith("CDSW")]
	return [f"{ok_dir}/{subdir}/{filename}" for subdir in subdirs for filename in os.listdir(f"{ok_dir}/{subdir}") if filename.startswith("CD") and filename.endswith(".csv")]

if __name__ == "__main__":
	ohio_filenames = get_ohio_filenames()
	ok_filenames = get_oklahoma_filenames()
	for filename in ohio_filenames + ok_filenames:
		replace_csv_with_sample(filename,nrows=10000)
