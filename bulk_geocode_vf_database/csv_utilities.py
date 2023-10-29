import csv
# import os
# from collections import OrderedDict
import chardet
import pandas as pd
import numpy as np



def get_number_rows(filename):
	num_rows = -1 # exclude header
	for row in open(filename,'rb'):
		num_rows += 1
	return num_rows

def get_headers(filename,sep=","):
	with open(filename) as csv_file:
		csv_reader = csv.reader(csv_file,delimiter=sep)
		return next(csv_reader)




def get_rows(filename,safe=False,safe_fast=False,pandas=False,pandas_safe=False):
	print(f"Getting rows from {filename}")
	if pandas_safe:
		return get_rows_pandas_safe(filename)
	elif pandas:
		return pd.read_csv(filename)
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


def get_max_characters_risky(filename,chunksize=50000,max_rows=1000000,sep=","):
	print(f"Getting max column characters in '{filename}'")
	headers = get_headers(filename,sep=sep)
	maxColumnLengths = [0 for header in headers]
	columnLengths = np.vectorize(len)
	nrows = get_number_rows(filename)
	for i,chunk in enumerate(pd.read_csv(filename,chunksize=chunksize,sep=sep,dtype=str)):
		print(f"PROCESSING CHUNK {i}/{min(nrows//chunksize,max_rows//chunksize)}")
		maxColumnLengths = np.maximum(columnLengths(chunk.values.astype(str)).max(axis=0),maxColumnLengths)
		if i * chunksize > max_rows:
			break
	return dict(zip(headers,maxColumnLengths))


def get_max_characters_in_cols(filename,sep=",",risky=True):
	if risky:
		try:
			return get_max_characters_risky(filename,sep=sep)
		except Exception as e:
			print(e)
			print(f"Risky csv reading {filename} failed!")
	headers = get_headers(filename,sep=sep)
	ncols = len(headers)
	max_chars = [0 for header in headers]
	ss = safeCSV(filename,sep=sep)
	# print(f"COUNTING COLUMN CHARACTERS IN {filename}")
	for row in ss:
		try:
			for i in range(ncols):
				max_chars[i] = max(max_chars[i],len(str(row[i])))
		except:
			for i in range(len(row)):
				max_chars[i] = max(max_chars[i],len(str(row[i])))
	return dict(zip(headers,max_chars))


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

def safeCSV(filename,sep=",",quotenone=False,quoting2=None):
	if quotenone:
		quoting = csv.QUOTE_NONE
	else:
		quoting= csv.QUOTE_MINIMAL
	if quoting2 is not None:
		quoting = quoting2
	with open(filename,'rb') as openFile:
		for i,line in enumerate(openFile):
				try:
					text = line.decode("utf-8")
				except UnicodeDecodeError as e:
					detected_encoding = chardet.detect(line)["encoding"]
					text = line.decode(detected_encoding)
					# print(f"\n\tError on line {i} in file {openFile.name}\n\t'{line}'\n\t'{e}'\n\t")
					# print(f"\n\tError on line {i} in file {openFile.name}\n\t'{e}'\n\t")
					# number_errors += 1
					print(f"Error reading line {i}")
				row = next(csv.reader( ( text, ) ,delimiter=sep, quoting=quoting))
				# print(f"ROW IS {row}")
				yield row


def get_rows_pandas_from_files(fnames,safe=False,safe_fast=False,pandas=False,pandas_safe=False):
	return pd.concat([row for fname in fnames for row in get_rows(fname,safe=safe,safe_fast=safe_fast,pandas=pandas,pandas_safe=pandas_safe)])


def get_pandas_in_chunks_safe(filename,chunk_size,skip_blocks=0,sep=",",quotenone=False):
	print(f"Getting rows IN CHUNKS of size {chunk_size} from {filename} using Pandas, safe mode")
	ss = safeCSV(filename,sep=sep,quotenone=quotenone)
	headers = next(ss)
	nBlocks = 0
	while True:
		nBlocks+=1
		# rows = list(zip(*([ss]*chunk_size)))
		rows = [x for x,_ in zip(ss,range(chunk_size))]
		if len(rows) == 0:
			break
		if nBlocks <= skip_blocks:
			yield pd.DataFrame()
			continue
		# print(f"DEBUG:\nheaders: {headers}\nrows (shape {np.array(rows).shape}): {np.array(rows)}\n")
		# pd.DataFrame(np.array(rows),columns=headers)
		# print("DONE")
		try:
			yield pd.DataFrame(np.array(rows), columns=headers)
		except:
			print(np.array(rows))
	# yield pd.DataFrame(np.array([row for row in ss]), columns=headers)

def get_pandas_chunks_basic(filename,chunk_size=None,sep=","):
	return pd.read_csv(filename,chunksize=chunk_size,sep=sep)

def get_pandas_in_chunks(filename,chunk_size,skip_blocks=0,sep=",",quotenone=False):
	print(f"Getting rows IN CHUNKS (basic) of size {chunk_size} from {filename} using Pandas, safe mode")
	ss = safeCSV(filename,sep=sep,quotenone=quotenone)
	headers = next(ss)
	nBlocks = 0
	for nBlocks,rows in enumerate(zip(*([ss]*chunk_size))):
		if nBlocks < skip_blocks:
			yield pd.DataFrame()
			continue
		try:
			yield pd.DataFrame(np.array(rows), columns=headers)
		except:
			print("ERROR IN CHUNK!!!")
			print(np.array(rows))
	# yield pd.DataFrame(np.array([row for row in ss]), columns=headers)


def get_rows_pandas_safe(filename):
	print(f"Getting rows from {filename} using Pandas, safe mode")
	headers = get_headers(filename)
	ss = safeCSV(filename)
	headers = next(ss)
	return pd.DataFrame(np.array([row for row in ss]), columns=headers)


def split_file(filenames, rows_per_file=1000000):
	for fname in filenames:
		with open(fname,'r') as ff:
			basename = fname.rsplit(".",1)[0]
			ext = fname.rsplit(".",1)[1]
			header = next(ff)
			outf = None
			for i,line in enumerate(ff):
				if i % rows_per_file == 0:
					if outf is not None:
						outf.close()
					out_fname = f"{basename}_{i//rows_per_file}.{ext}"
					outf = open(out_fname,'w')
					outf.write(header)
				outf.write(line)


# if __name__=="__main__":
# 	filename = "/Users/zach/Documents/UCLA_classes/research/Polling/data/voter_files/Ohio/SWVF_1_22.csv"
# 	for i, chunk_df in enumerate(get_pandas_in_chunks(filename,10000,skip_blocks=200)):
# 		print(f"CHUNK {i}")
	# maxChars = get_max_characters_in_cols(filename)

	# headers = get_headers(filename)
	# ncols = len(headers)
	# max_chars = [0 for header in headers]
	# ss = safeCSV(filename)
	# print(f"COUNTING COLUMN CHARACTERS IN {filename}")
	# nWeirdRows=0
	# # for row in enumerate(ss):
	# # 	if len(row) != ncols:
	# # 		nWeirdRows+=1
	# # 	for i in range(ncols):
	# # 		# pass
	# # 		max_chars[i] = max(max_chars[i],len(str(row[i])))
	# # print(f"nWeirdRows: {nWeirdRows}")
	# # max_chars = dict(zip(headers,max_chars))
