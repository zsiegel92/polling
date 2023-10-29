import chardet
from collections import OrderedDict
import csv
from io import StringIO

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

def safeCSV2(filename):
	filestring = ''
	with open(filename,'rb') as openFile:
		for i,line in enumerate(openFile):
				try:
					line = line.decode("utf-8")
				except UnicodeDecodeError as e:
					detected_encoding = chardet.detect(line)["encoding"]
					line = line.decode(detected_encoding)
					# print(f"\n\tError on line {i} in file {openFile.name}\n\t'{line}'\n\t'{e}'\n\t")
					# print(f"\n\tError on line {i} in file {openFile.name}\n\t'{e}'\n\t")
				filestring += line + "\n"
	f = StringIO(filestring)
	reader = csv.reader(f,delimiter=",")
	for row in reader:
		yield row
def get_headers(filename):
	with open(filename) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=",", quotechar='"')
		return next(csv_reader)

def get_rows(filename,return_errors=False):
	ss = safeCSV(filename)
	next(ss) #remove headers
	return ss



# import chardet

# with open(filename,'rb') as csv_file:
# 	for i,line in enumerate(csv_file):
# 		detected_encoding = chardet.detect(line)["encoding"]
# 		try:
# 			line = line.decode("utf-8")
# 		except UnicodeDecodeError as e:
# 			line = line.decode(detected_encoding)
# 			print(line)
# 			print(f"Error on line {i} in file {filename}")
# 			print(e)
# 			number_errors += 1
# 		lines.append(line)

# class SafeCSV:
# 	def __init__(self,csv_file):
# 		self.csv_file = csv_file
# 		self.i = 0
# 	def __iter__(self):
# 		self.i = 0
# 		return self

# 	def __next__(self):
# 		line = next(self.csv_file)
# 		try:
# 			line = line.decode("utf-8")
# 		except UnicodeDecodeError as e:
# 			line = line.decode(detected_encoding)
# 			print(line)
# 			print(e)
# 		return line

# def safeCSV(openFile):
# 	for i,line in enumerate(csv_file):
# 		detected_encoding = chardet.detect(line)["encoding"]
# 		try:
# 			line = line.decode("utf-8")
# 		except UnicodeDecodeError as e:
# 			line = line.decode(detected_encoding)
# 			print(line)
# 			print(f"Error on line {i} in file {csv_file.name}")
# 			print(e)
# 		yield line
