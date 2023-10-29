import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, Table, Column, String, MetaData, func, select, Integer#, sessionmaker, Integer,
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base
# from psycopg2 import create_engine
import psycopg2
import subprocess
from multiprocessing import  Pool

subprocess.call(['diskutil mount ZS'],shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.call(['pg_ctl start -D /Volumes/ZS/db/pg13'],shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


### UTILITIES START

# method (pd.read_sql) FETCHES all results of the query BEFORE iterating. Uses too much memory.
# Problem Description by Joris: https://stackoverflow.com/questions/31837979/pandas-sql-chunksize/31839639#31839639
# Problem described: https://stackoverflow.com/questions/59568034/how-does-the-chunksize-parameter-in-pandas-read-sql-avoid-loading-data-into-me
# Solution: https://docs.sqlalchemy.org/en/13/_modules/examples/performance/large_resultsets.html
# https://github.com/pandas-dev/pandas/issues/12265
def get_engine(stream_results=True,**kwargs):
	return create_engine('postgresql://zach@localhost:5432/zach', echo=False,execution_options={'stream_results':stream_results},**kwargs)

def get_meta():
	engine = get_engine()
	meta = MetaData()
	meta.reflect(bind=engine)
	return meta


# https://stackoverflow.com/questions/44193823/get-existing-table-using-sqlalchemy-metadata/44205552
def get_table(tablename):
	meta = get_meta()
	table = meta.tables[tablename]
	return table

def get_table_names():
	meta = get_meta()
	return list(meta.tables.keys())

# https://docs.sqlalchemy.org/en/14/core/reflection.html
def delete_table(tablename):
	engine = get_engine()
	table = get_table(tablename)
	engine.execute(table.delete())


def drop_table(tablename):
	engine=get_engine(stream_results=False)
	table = Table(tablename, MetaData() )
	table.drop(engine,checkfirst=True)


def table_exists(tablename):
	engine=get_engine()
	return engine.dialect.has_table(engine, tablename)


def get_padder(padstyle):
	if padstyle=='multiple':
		return lambda colSize: max(int(colSize*1.5),2)
	elif isinstance(padstyle,int):
		return lambda colSize: colSize + padstyle
	elif padstyle is None:
		return lambda colSize: colSize
	else:
		return lambda colSize: colSize

# https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
# https://docs.sqlalchemy.org/en/14/core/metadata.html
def create_table(tablename,col_sizes=None,primary_key=None,primary_keys=[],padstyle='multiple',counter_index=None):
	engine=get_engine(stream_results=False)
	padder = get_padder(padstyle)
	columns = [
		Column(
			colname,
			String(padder(colSize)),
			primary_key=((primary_key==colname) or (colname in primary_keys))
		)
		for colname, colSize in col_sizes.items()
	]
	if counter_index is not None:
		columns = [Column(counter_index,Integer, primary_key=True) ] + columns# Auto-increment should be default
	table = Table(tablename, MetaData(), *columns )
	table.create(engine, checkfirst=True)


def create_table_pandas(tablename,df,pkeys=[]):
	engine = get_engine(stream_results=False)
	with engine.begin() as conn:
		# print("COLUMNS:", "\n".join(df.columns))
		df.to_sql(name=tablename, con=conn, if_exists="replace")
		if type(pkeys) != str:
			pkeys = ",".join(quote_only(col) for col in pkeys)
		else:
			pkeys= quote_only(pkeys)
		conn.execute(f'ALTER TABLE "{tablename}" ADD PRIMARY KEY ({pkeys});')
	engine.dispose()


def table_colnames(tablename):
	table = get_table(tablename)
	return [col.name for col in table.columns]

### UTILITIES END


### WRITING START

def update_records(tablename,df,pkey):
	engine = get_engine(stream_results=False)
	conn = engine.connect()
	table = get_table(tablename)
	for record in df.to_dict(orient='records'):
		stmt = table.update().where(getattr(table.c,pkey)==record[pkey]).values(**record)
		conn.execute(stmt)

def update_records_fast(tablename,df,pkey,columns):
	drop_table('tmp_df')
	df = df[[pkey] + columns] #just added this
	columnset = ", ".join(f"\"{column}\" = tmp_df.\"{column}\"" for column in columns)
	update_query = f"""UPDATE "{tablename}"
		SET {columnset}
		FROM tmp_df
		WHERE tmp_df."{pkey}" = "{tablename}"."{pkey}"
	"""
	engine = get_engine(stream_results=False)
	with engine.begin() as conn:
		# print("COLUMNS:", "\n".join(df.columns))
		df.to_sql(name="tmp_df", con=conn, if_exists="replace")
		conn.execute(f'CREATE INDEX "ix_{pkey}" ON tmp_df("{pkey}");')
		# print(f"EXECUTING UPDATE QUERY {update_query}")
		conn.execute(update_query)
		drop_query = "DROP TABLE IF EXISTS tmp_df;"
		conn.execute(drop_query)
	engine.dispose()
# def update_records_fast_concise(tablename,df,pkey,column):
# 	engine = get_engine(stream_results=False)
# 	df.to_sql(name="tmp_df", con=engine, if_exists="replace")
# 	execute_raw_sql(f'CREATE INDEX ix_{pkey} ON tmp_df({pkey});')
# 	update_query = f"""UPDATE {tablename}
# 			SET {tablename}.{column} = tmp_df.{column}
# 			FROM tmp_df, {tablename}
# 			WHERE tmp_df.{pkey} = {tablename}.{pkey}
# 		"""
# 	execute_raw_sql(update_query)
# 	drop_table('tmp_df')




def add_pkey_constraint(tablename,colname):
	execute_raw_sql(f'ALTER TABLE {tablename} ADD PRIMARY KEY({quote_only(colname)});')

# https://stackoverflow.com/questions/54039093/pandas-to-sql-insert-ignore
# https://www.postgresql.org/docs/9.5/sql-insert.html
def add_rows_from_table_to_other_table_where_not_existing(table_add_to,table_add_from,existing_col):
	query = f'INSERT INTO {table_add_to} (SELECT * FROM {table_add_from}) ON CONFLICT ({quote_only(existing_col)}) DO NOTHING'
	# add_pkey_constraint(table_add_to,existing_col)
	execute_raw_sql(query)
	# query = f'select * FROM {table_add_from} t1 LEFT JOIN {table_add_to} t2 ON t2.{quote_only(existing_col)} = t1.{quote_only(existing_col)} WHERE t2.{quote_only(existing_col)} IS NULL'
	# print(f"QUERYING:\n{query}")
	# for i,chunk in enumerate(query_chunks(query,chunksize=chunksize)):
	# 	print(f"ADDING CHUNK {i}")
	# 	append_df_to_table(table_add_to,chunk,if_exists='append')


### WRITING END


### QUERY BUILDING START

def is_quoted(col):
	return col[0] == col[-1] == '"'

def quote_only(col):
	if is_quoted(col):
		return col
	return f"\"{col}\"" if col != "*" else col

def quotecol(col,tablename=None,as_dotted=False,dotted_prefix=None,dotchar="."):
	table_prefix= f'{quote_only(tablename)}.' if tablename else ""
	alias_prefix = table_prefix if dotted_prefix is None else f"{dotted_prefix}{dotchar}"
	as_alias = "" if not as_dotted else f" AS {quote_only(f'{alias_prefix}{col}')}"
	return f"{table_prefix}{quote_only(col)}{as_alias}"

def where_empty(colnames,tablename=None):
	return " OR ".join([f"({quotecol(col,tablename)} = '' OR {quotecol(col,tablename)} IS NULL)" for col in colnames])

def where_any_null(colnames,tablename=None):
	return " OR ".join([f"({quotecol(col,tablename)} IS NULL)" for col in colnames])

def where_all_null(colnames,tablename=None):
	return " AND ".join([f"({quotecol(col,tablename)} IS NULL)" for col in colnames])

def where_nonnull(colnames,tablename=None):
	return " AND ".join([f"({quotecol(col,tablename)} IS NOT NULL)" for col in colnames])


def where_nonempty(colnames,tablename=None):
	return " AND ".join([f"({quotecol(col,tablename)} <> '' AND {quotecol(col,tablename)} IS NOT NULL)" for col in colnames])

def where_equals(colmap,tablename=None):
	return " AND ".join([f"({quotecol(col,tablename)} = '{colval}')" for col,colval in colmap.items()])

def where_in(colmap,tablename=None):
	adj_colmap = {quotecol(col,tablename) : ", ".join(f"'{item}'" for item in col_list) for col,col_list in colmap.items()}
	return " AND ".join([f"({colname} in ({col_list_str}))" for colname,col_list_str in adj_colmap.items()])

#use "RAND()" on other SQL implementations.
def where_rand(prob):
	return f"RANDOM() <= {prob}" if (prob and prob < 1) else ""

# def append_tablename(tablename,col_structure):
# 	if isinstance(col_structure, dict):
# 		return {f"{tablename}.{col}" : v for col,v in col_structure.items()}
# 	if isinstance(col_structure, list):
# 		return [f"{tablename}.{col}" for col in col_structure]

def where_multiple(emptycols=[],nonemptycols=[],equalscols={},incols={},nullcols=[],allnullcols=[],nonnullcols=[],randprob=1,extra_where=None,tablename=None):
	return joinwhere([
					where_empty(emptycols,tablename=tablename),
					where_nonempty(nonemptycols,tablename=tablename),
					where_equals(equalscols,tablename=tablename),
					where_in(incols,tablename=tablename),
					where_any_null(nullcols,tablename=tablename),
					where_all_null(allnullcols,tablename=tablename),
					where_nonnull(nonnullcols,tablename=tablename),
					where_rand(randprob),
					extra_where])

def joinwhere(clauses):
	return " AND ".join(f"({clause})" for clause in clauses if clause)


### QUERY BUILDING END


### CORE QUERYING START

# https://docs.sqlalchemy.org/en/13/_modules/examples/performance/large_resultsets.html
def query_chunks_raw(tablename,query,chunksize=200000,con=None):
	headers = table_colnames(tablename)
	engine= get_engine()
	table=get_table(tablename)
	if con is None:
		con = engine.connect()
	with con:
			result = con.execution_options(stream_results=True).execute(
				table.select()
			)
			while True:
				df= pd.DataFrame(result.fetchmany(chunksize),columns=headers)
				if len(df) == 0:
					break
				yield df

def query_chunks(query,chunksize=10000,con=None):
	print(query)
	if con is None:
		con = get_engine().connect()
		# con = con.begin()
		# con.__enter__()
		# con.__exit__()
	# print(f"QUERY: {query}")
	it = pd.read_sql(query,con,chunksize=chunksize)
	return it
	# for chunk in it:
	#   yield chunk


def get_distinct_values(tablename,column,chunksize=None,con=None):
	return query_chunks(f'''SELECT DISTINCT "{column}" FROM {tablename};''', chunksize=chunksize,con=con)[column].values

def select_where(tablename,where="",chunksize=10000,onlyquery=False,cols=None):
	if cols:
		col_placeholder = ", ".join(f'{colname}' for colname in cols)
	else:
		col_placeholder="*"
	whereclause = f" WHERE {where}" if where else ""
	query = f"SELECT {col_placeholder} from {tablename} {whereclause}"
	if onlyquery:
		return query
	return query_chunks(query,chunksize=chunksize)

# Note: in SQL, double-quotes are used for identifiers, single-quotes for strings.
def select_by_empty_columns(tablename,nonempty=[],empty=[],chunksize=200000,onlyquery=False,cols=None,equalscols={}):
	where = joinwhere([where_empty(empty),where_nonempty(nonempty),where_equals(equalscols)])
	return select_where(tablename,where=where,chunksize=10000,onlyquery=onlyquery,cols=cols)


def select_by_multiple(tablename,nonempty=[],empty=[],null=[],nonnull=[],chunksize=200000,onlyquery=False,cols=None,equals={}):
	where = where_multiple(emptycols=empty,nonemptycols=nonempty,nullcols=null,nonnullcols=nonnull,equalscols=equals)
	return select_where(tablename,where=where,chunksize=chunksize,onlyquery=onlyquery,cols=cols)

# Replace all instances of "*" with all col names.
# If "*" appears multiple times, so do all columns.
def prefix_join_cols(cols,tablename,expand_star=False,as_dotted=True,dotted_prefix=None,table_alias=None,dotchar="."):
	while expand_star and "*" in cols:
		i = cols.index("*")
		cols[i:i+1] = table_colnames(tablename)
	table_alias = tablename if table_alias is None else table_alias
	return [quotecol(col,table_alias,as_dotted=as_dotted,dotted_prefix=dotted_prefix,dotchar=dotchar) for col in cols]

def select_join(tablename1,tablename2,jointuple,cols1=['*'],cols2=['*'],where1=None,where2=None,dotted_prefix2=None,**kwargs):
	# where = joinwhere([f'{tablename1}.{jointuple[0]} = {tablename2}.{jointuple[1]}',where1,where2])
	where = joinwhere([where1,where2])
	compound_tablename = f"{tablename1} JOIN {tablename2} ON ({quotecol(jointuple[0],tablename1)} = {quotecol(jointuple[1],tablename2)})"
	cols1, cols2 = prefix_join_cols(cols1,tablename1,expand_star=False,as_dotted=False), prefix_join_cols(cols2,tablename2,expand_star=True,as_dotted=True,dotted_prefix=dotted_prefix2)
	cols = cols1 + cols2
	# query = f'SELECT * FROM {compound_tablename} WHERE  {where};'
	return select_where(compound_tablename,where=where,cols=cols,**kwargs)



def select_join_3(tablename1,tablename2,tablename3,jointuple12,jointuple13,cols1=['*'],cols2=['*'],cols3=['*'],where1=None,where2=None,where3=None, dotted_prefix2=None, dotted_prefix3=None,**kwargs):
	# where = joinwhere([f'{tablename1}.{jointuple[0]} = {tablename2}.{jointuple[1]}',where1,where2])
	where = joinwhere([where1,where2,where3])
	compound_tablename = f"{tablename1} JOIN {tablename2} ON ({quotecol(jointuple12[0],tablename1)} = {quotecol(jointuple12[1],tablename2)}) JOIN {tablename3} ON ({quotecol(jointuple13[0],tablename1)} = {quotecol(jointuple13[1],tablename3)})"
	cols1, cols2,cols3 = prefix_join_cols(cols1,tablename1,expand_star=False,as_dotted=False), prefix_join_cols(cols2,tablename2,expand_star=True,as_dotted=True,dotted_prefix=dotted_prefix2), prefix_join_cols(cols3,tablename3,expand_star=True,as_dotted=True,dotted_prefix=dotted_prefix3)
	cols = cols1 + cols2 + cols3
	# query = f'SELECT * FROM {compound_tablename} WHERE  {where};'
	return select_where(compound_tablename,where=where,cols=cols,**kwargs)




# # jointuples 		is a dict whose keys are a tuple (tablename,table_dot_prefix) (table n) and values are 2-tuples of columns in table 1 and table n
# # jointuples is a list of tuples of the form (tablenamen, table1_col,tablen_col). The table1_col becomes the dot for cols in that table prefix.
# # cols 				is a dict whose keys are a tuple tablename and values are lists of columns in that table
# def select_join_n(tablenames, jointuples,cols={},where1=None,**kwargs):
# 	# where = joinwhere([f'{tablename1}.{jointuple[0]} = {tablename2}.{jointuple[1]}',where1,where2])
# 	table_1 = tablenames[0]
# 	compound_tablename = table_1
# 	for table_n in tablenames:
# 		if table_n not in cols:
# 			cols[table_n] = ["*"]
# 	all_cols = prefix_join_cols(cols[table_1],table_1,expand_star=False,as_dotted=False)
# 	for (table_n, table_1_col, table_n_col) in jointuples:
# 		compound_tablename += f" JOIN {table_n} on ({quotecol(table_1_col,table_1)} = {quotecol(table_n_col,table_n)})"
# 		all_cols += prefix_join_cols(cols[table_n],table_n,expand_star=True,as_dotted=True,dotted_prefix=table_1_col)
# 	return select_where(compound_tablename,where=where1,cols=all_cols,**kwargs)


# jointuples 		is a dict whose keys are a tuple (tablename,table_dot_prefix) (table n) and values are 2-tuples of columns in table 1 and table n
# jointuples is a list of tuples of the form (tablenamen, table1_col,tablen_col). The table1_col becomes the dot for cols in that table prefix.
# cols 				is a dict whose keys are a tuple (tablename,table1_col, tablen_col)  and values are lists of columns in that table
def select_join_n(table_1, jointuples,cols={},table_1_cols=['*'], where1=None,dotchar=".",**kwargs):
	compound_tablename = table_1
	all_cols = prefix_join_cols(table_1_cols,table_1,expand_star=False,as_dotted=False)
	for (table_n, table_1_col, table_n_col) in jointuples:
		if (table_n, table_1_col, table_n_col) not in cols:
			cols[(table_n, table_1_col, table_n_col)] = ['*']
		table_n_alias = table_1_col
		compound_tablename += f" JOIN {table_n} AS {quote_only(table_n_alias)} on ({quotecol(table_1_col,table_1)} = {quotecol(table_n_col,table_n_alias)})"
		all_cols += prefix_join_cols(cols[(table_n, table_1_col, table_n_col)],table_n,expand_star=True,as_dotted=True,dotted_prefix=table_n_alias,table_alias=table_n_alias,dotchar=dotchar)
	return select_where(compound_tablename,where=where1,cols=all_cols,**kwargs)



def get_all_rows(tablename):
	engine = get_engine()
	return pd.read_sql_table(tablename,engine,chunksize=None)

# use chunksize=None for one single dataframe
def select_all(tablename,chunksize):
	engine = get_engine()
	return pd.read_sql_table(tablename,engine,chunksize=chunksize)

### CORE QUERYING END


### COUNTING START

def count_where(tablename,where):
	engine = get_engine()
	whereclause = f" WHERE {where}" if where else ""
	query = f"SELECT count(*) FROM \"{tablename}\"{whereclause}"
	# print(query)
	return engine.execute(query).scalar()

def count_empty_rows(tablename,colnames=[]):
	where = where_empty(colnames)
	return count_where(tablename,where)

def count_nonempty_rows(tablename,colnames=[]):
	where = where_nonempty(colnames)
	return count_where(tablename,where)

def get_number_records(tablename):
	engine = get_engine()
	table = get_table(tablename)
	query = select([func.count()]).select_from(table)
	return engine.execute(query).scalar()



### COUNTING END


### CUSTOM QUERIES START
def sample_where(tablename,where=None,fraction=.001,chunksize=200000,cols=None):
	full_where = where_multiple(randprob=fraction,extra_where=where)
	return select_where(tablename,where=full_where,chunksize=chunksize,cols=cols)


def sample_where_exact(tablename,where=None,fraction=.001,chunksize=200000,cols=None):
	nrows = get_number_records(tablename)
	indices = sorted(np.random.choice(range(nrows),size=int(fraction*nrows),replace=False))
	it = select_where(tablename,where=where,chunksize=chunksize,cols=cols)
	if fraction >= 1:
		return it
	nSeen = 0
	for chunk in it:
		nInChunk = chunk.shape[0]
		indices_in_chunk = [ind for ind in range(nInChunk) if (ind + nSeen) in indices]
		nSeen += nInChunk
		subchunk = chunk.iloc[indices_in_chunk]
		if subchunk.shape[0] == 0:
			continue
		yield subchunk




### GENERATING REPORTS START ###

def generate_report(tablename,filename,chunksize=200000):
	# print(f"Exporting 'SELECT * FROM {tablename};':\n\nto{filename}")
	while os.path.isfile(filename):
		filename = f"{filename.rsplit('.',1)[0]}_.csv"
	for i,df in enumerate(select_all(tablename,chunksize)):
		df.to_csv(
			filename,
			mode='a',
			header=(True if (i == 0 and not os.path.isfile(filename)) else False),
			index=False
		)

def generate_report_query(filename,query,chunksize=200000):
	print(f"Executing: {query}\n\nto {filename}")
	while os.path.isfile(filename):
		filename = f"{filename.rsplit('.',1)[0]}_.csv"
	for i,df in enumerate(query_chunks(query,chunksize = chunksize)):
		df.to_csv(
			filename,
			mode='a',
			header=(True if (i == 0 and not os.path.isfile(filename)) else False),
			index=False
		)

# https://www.postgresqltutorial.com/export-postgresql-table-to-csv-file/
def generate_report_query_pg(filename,query,saveCSV=True,save_table=True,indices=[]):
	tabname = filename.rsplit("/",1)[-1].rsplit(".",1)[0]
	drop_table(tabname)
	dump_report_sql(tabname,query,indices=indices)
	if saveCSV:
		execute_raw_sql(f'''
						COPY {tabname} TO '{filename}' DELIMITER ',' CSV HEADER;
						''')
	if not save_table:
		drop_table(tabname)


# https://www.oreilly.com/library/view/mysql-cookbook-2nd/059652708X/ch04s03.html
def dump_report_sql(sql_report_tablename,query,chunksize=200000,indices=[]):
	drop_table(sql_report_tablename)
	engine_read=get_engine(stream_results=True,pool_pre_ping=True)
	engine_write=get_engine(stream_results=False,pool_pre_ping=True)
	execute_raw_sql(f'CREATE TABLE "{sql_report_tablename}" as {query};')
	for index in indices:
		create_index(sql_report_tablename,index)
	# with engine_read.begin() as con:
	# 	for i,df in enumerate(query_chunks(query,chunksize = chunksize,con=con)):
	# 		print(f"Dumping chunk {i} to {sql_report_tablename}")
	# 		df.to_sql(sql_report_tablename, con=engine_write, schema=None, if_exists='append', index=False, index_label=None, chunksize=None, dtype=None, method=None)




# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
def append_df_to_table(tablename,df,if_exists='append',con=None):
	if con is None:
		con = get_engine(stream_results=False)
	df.to_sql(tablename, con=con, schema=None, if_exists=if_exists, index=False, index_label=None, chunksize=None, dtype=None, method=None)
### GENERATING REPORTS END ###

def rename_table(oldname,newname):
	# table = get_table(oldname)
	# table.rename(newname)
	execute_raw_sql(f"ALTER TABLE {oldname} RENAME TO {newname};")

# re: async vs async_ in Python 3.7
# https://github.com/psycopg/psycopg2/issues/714
def get_psycopg2_connection(dbname='zach',host='localhost',port='5432', user='zach',asynchronous=False):
	if asynchronous:
		return psycopg2.connect(dbname=dbname, host=host, port=port, user=user, async_=1)
	else:
		return psycopg2.connect(dbname=dbname, host=host, port=port, user=user)

def execute_raw_sql(query, asynchronous=False):
	print(f"EXECUTING:\n{query}\n")
	conn = get_psycopg2_connection(asynchronous=asynchronous)
	cur = conn.cursor()
	cur.execute(query)
	conn.commit()


def get_datatype(column_type,col_names=None):
	if column_type=='Integer' or column_type==int:
		datatype= "int"
	elif column_type==bool or column_type=='boolean' or column_type=='Boolean' or column_type=='BOOLEAN':
		datatype='BOOLEAN'
	else:
		try:
			datatype= f"VARCHAR({int(column_type)})"
		except Exception:
			datatype = "text"
	return datatype

def add_null_column(tablename,col_name,column_type=None):
	existing_cols = table_colnames(tablename)
	if col_name in existing_cols:
		return
	datatype=get_datatype(column_type)
	query = f"ALTER TABLE {tablename} ADD COLUMN \"{col_name}\" {datatype};"
	execute_raw_sql(query)


def add_null_columns(tablename,col_names=None,column_type=None):
	existing_cols = table_colnames(tablename)
	col_names = [col_name for col_name in col_names if col_name not in existing_cols]
	if not col_names:
		return
	datatype=get_datatype(column_type,col_names=col_names)
	command = ", ".join([f"ADD COLUMN \"{col}\" {datatype}" for col in col_names])
	query = f"ALTER TABLE {tablename} {command};"
	execute_raw_sql(query)

# https://stackoverflow.com/questions/7599519/alter-table-add-column-takes-a-long-time
# pkeys is a list of column names
# fkeys is a list of (colname,foreign_colname,foreign_tablename) tuples
def add_null_columns_fast(tablename,col_names=None,column_type=None,pkeys=[],fkeys=[]):
	drop_table('tmp_df')
	existing_cols = table_colnames(tablename)
	col_names = [col_name for col_name in col_names if col_name not in existing_cols]
	if len(col_names) > len(set(col_names)):
		print("ATTEMPTING TO ADD DUPLICATE COLUMNS!")
		print(col_names)
		raise
	if not col_names:
		return
	engine = get_engine(stream_results=False)
	# execute_raw_sql(f"CREATE TABLE tmp_df LIKE {tablename}")
	execute_raw_sql(f"CREATE TABLE tmp_df AS (SELECT * FROM {tablename} LIMIT 0)")
	add_null_columns('tmp_df',col_names=col_names,column_type=column_type)
	with engine.begin() as conn:
		for pkey in pkeys:
			conn.execute(f"ALTER TABLE tmp_df ADD PRIMARY KEY(\"{pkey}\");")
		for col_name, foreign_col_name, foreign_tablename in fkeys:
			if col_name in existing_cols:
				conn.execute(f"ALTER TABLE tmp_df ADD FOREIGN KEY (\"{col_name}\") REFERENCES {foreign_tablename}(\"{foreign_col_name}\");")
		# print("Inserting into tmp_df...")
		conn.execute(f"INSERT INTO tmp_df SELECT *, NULL FROM {tablename}")
	engine.dispose()
	# print("Renaming...")
	rename_table(tablename,f"{tablename}_old")
	rename_table("tmp_df",tablename)
	# print("Dropping...")
	drop_table(f"{tablename}_old")

def add_fkey_column(tablename,col_name,foreign_tablename,foreign_col_name=None,foreign_col_type=int):
	if not foreign_col_name:
		foreign_col_name = col_name
	add_null_column(tablename,col_name,column_type=foreign_col_type)
	query = f'''ALTER TABLE {tablename} ADD FOREIGN KEY ({quote_only(col_name)}) REFERENCES {foreign_tablename}({quote_only(foreign_col_name)});'''
	# print(query)
	execute_raw_sql(query)

def drop_column(tablename,col_name):
	query = f"ALTER TABLE \"{tablename}\" DROP COLUMN \"{col_name}\";"
	execute_raw_sql(query)



def copy_table(tablename,new_tablename):
	query = f'CREATE TABLE {new_tablename} AS TABLE {tablename};'
	execute_raw_sql(query)

# USE EXAMPLE:
# vf_chunk = parallelize_dataframe(vf_chunk,append_census_df,n_cores=4)
def parallelize_dataframe(df, func, n_cores=4):
	if n_cores==1:
		return func(df)
	df_split = np.array_split(df, n_cores)
	pool = Pool(n_cores)
	df = pd.concat(pool.map(func, df_split))
	pool.close()
	pool.join()
	# pool.terminate()
	# pool.restart()
	return df


def modify_table_verbose(tablename,df_function,pkey, chunksize=200000,fraction=1,where=None,max_applications=np.inf,cols=None,n_cores=1):
	nApplied = 0
	cols = [quotecol(col) for col in cols] if cols else None
	total_results = int(count_where(tablename,where=where)*fraction)
	print(f"Applying {df_function.__name__} to {total_results} rows of {tablename}")

	for i,df in enumerate(sample_where(tablename,where=where,fraction=fraction,chunksize=chunksize,cols=cols)):
		nApplied += df.shape[0]
		print(f"Chunk {i :05} - Applying {df_function.__name__} to {nApplied}/{total_results} rows of {tablename}...")
		df = parallelize_dataframe(df,df_function,n_cores=n_cores)
		update_records(tablename,df,pkey=pkey)
		print(f"Finished Chunk {i :05}.")
		if nApplied > max_applications:
			print(f"Applied {df_function.__name__} to maximum ({max_applications}) rows of {tablename}")
			break

def modify_table(tablename,df_function,pkey,chunksize=200000,fraction=1,where=None,max_applications=np.inf,cols=None,nCores=1):
	nApplied = 0
	cols = [quotecol(col) for col in cols]
	for i,df in enumerate(sample_where(tablename,where=where,fraction=fraction,chunksize=chunksize,cols=cols)):
		df = parallelize_dataframe(df,df_function,n_cores=4)
		# df = df_function(df)
		update_records(tablename,df,pkey=pkey)
		if nApplied > max_applications:
			break

def create_index(tablename, colname):
	query = f'''CREATE INDEX ON {tablename} ({quote_only(colname)});'''
	# print(query)
	execute_raw_sql(query)

if __name__=="__main__":
	# oldname= "voterstesting"
	# newname= "voterstesting2"
	# table = get_table(oldname)
	# vf_tablename = "voterstesting_georgia"
	# print( table_colnames(vf_tablename))
	pass
	# rename_table(oldname,newname)
	# df = pd.read_sql('select * from voters',engine)




# def add_autoincrement_index(tablename,col_name):
#   query = f"ALTER TABLE {tablename} ADD {col_name} INT NOT NULL PRIMARY KEY IDENTITY(1,1)"
#   execute_raw_sql(query)

### UNUSED
# https://docs.sqlalchemy.org/en/13/orm/session_basics.html
# def get_session():
#   engine = get_engine()
#   # create a configured "Session" class
#   Session = sessionmaker(bind=engine)
#   # create a Session
#   session = Session()
#   return session
###
