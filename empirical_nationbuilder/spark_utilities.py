import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, DateType,FloatType
from pyspark.sql.functions import dayofmonth,from_unixtime,month, unix_timestamp, year,datediff, to_date, lit
from pyspark.sql.functions import col as colFun, isnan, when, trim
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, Binarizer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array

# import logging
# from pyspark import SparkContext
# s_logger = logging.getLogger('py4j.java_gateway')
# s_logger.setLevel(logging.ERROR)
# from pyspark import SparkContext
# SparkContext.setSystemProperty('spark.executor.memory', '2g')

# sc = SparkContext("local", "App Name")

# from log_reg_model import log_reg_cols, date_cols, categorical_cols, float_cols, outcome,log_reg_cols_and_outcome, year_cols

tmp='tmp'
spark_storage_dir = '/Volumes/ZS/spark_datastore/spark_datastore'
# See:
# /usr/local/Cellar/apache-spark/3.0.2/libexec/conf/spark-env.sh
# https://www.edureka.co/community/5268/how-to-change-the-spark-session-configuration-in-pyspark
def get_session(extramemory=True):
	spark_session = SparkSession \
		.builder \
		.appName("Polling_ML") \
		.config("spark.jars", "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/empirical_nationbuilder/spark_pgsql_jdbc_driver/postgresql-42.2.20.jar") \
		.config('spark.driver.memory','3g')\
		.config('spark.executor.memory','3g')\
		.getOrCreate()
	# if extramemory:
		# sc = spark_session.sparkContext
		# sc.setSystemProperty('spark.executor.memory', '3g')
		# sc.setSystemProperty('spark.driver.memory', '3g')
	# spark_session.conf.set("spark.executor.memory", '1g')
	# spark_session.conf.set("spark.driver.memory",'1g')
	return spark_session
		# .config("spark.jars", "/Users/zach/code/spark_pgsql_jdbc_driver/postgresql-42.2.19.jar") \


def test_session_variables():
	spark_session = SparkSession \
		.builder \
		.appName("Polling_ML") \
		.config("spark.jars", "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/empirical_nationbuilder/spark_pgsql_jdbc_driver/postgresql-42.2.19.jar") \
		.config('spark.driver.memory','2g')\
		.config('spark.executor.memory','2g')\
		.getOrCreate()
	sc = spark_session.sparkContext
	print(sc._conf.getAll())
	# sc.setSystemProperty('spark.executor.memory', '3g')
	# sc.setSystemProperty('spark.driver.memory', '3g')

# https://stackoverflow.com/questions/34948296/using-pyspark-to-connect-to-postgresql
def get_table(tablename,printSchema=False):
	spark = get_session()
	df = spark.read \
		.format("jdbc") \
		.option("url", "jdbc:postgresql://localhost:5432/zach") \
		.option("user", "zach") \
		.option("dbtable", tablename) \
		.option("driver", "org.postgresql.Driver") \
		.load()
		# .option('inferSchema','True') \
	if printSchema:
		df.printSchema()
	return df

# https://blog.knoldus.com/how-to-read-from-hdfs-persist-in-postgresql-via-spark/ # method 1
# https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html #method 2
def persist_df(df,out_tablename):
	# method 1
	# df.write.mode('overwrite').jdbc("jdbc:postgresql://localhost:5432/zach")

	# method 2
	# spark = get_session()
	df.write\
		.mode('overwrite')\
		.format("jdbc") \
		.option("url", "jdbc:postgresql://localhost:5432/zach") \
		.option("user", "zach") \
		.option("dbtable", out_tablename) \
		.option("driver", "org.postgresql.Driver") \
		.save()

def fromPandas(pd_df):
	spark = get_session()
	sparkDF=spark.createDataFrame(pd_df)
	return sparkDF

# https://sparkbyexamples.com/pyspark/convert-pyspark-dataframe-to-pandas/
# https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch04.html
# https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
def persist_df_to_parquet(df,out_tablename,from_pandas=False):
	# df.write.mode('overwrite').option("path", spark_storage_dir).saveAsTable(out_tablename)
	if from_pandas:
		df = fromPandas(df)
	df.write.mode('overwrite').parquet(f"{spark_storage_dir}/{out_tablename}.parquet")

# https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
def read_persisted_table(out_tablename,pandas=False):
	spark = get_session()
	df = spark.read.parquet(f"{spark_storage_dir}/{out_tablename}.parquet")
	if pandas:
		df = df.toPandas()
	return df


def save_json_in_persistent_storage(dd,filename):
	with open(f"{spark_storage_dir}/{filename}.json",'w') as f:
		json.dump(dd,f)

def load_json_in_persistent_storage(filename):
	with open(f"{spark_storage_dir}/{filename}.json",'r') as f:
		return json.load(f)

# https://stackoverflow.com/questions/35684018/how-to-convert-dataframe-columns-from-string-to-float-double-in-pyspark-1-6
def cast_col(df,col,casttype='float'):
	return df.withColumn(col,df[col].cast(casttype).alias(col))

#FloatType.typeName() -> 'float'
#DateType.typeName() -> 'date'
def cast_cols(df,cols,casttype='float'):
	print(f"Casting {', '.join(cols[:3])}{'...' if len(cols)>3 else ''} as {casttype}")
	for col in cols:
		df = cast_col(df,col,casttype=casttype)
	return df


def cast_categorical_string_indexed(df,col):
	df = df.na.replace('','NA',[col]) #https://stackoverflow.com/questions/33089781/spark-dataframe-handing-empty-string-in-onehotencoder
	df = rename_column(df,col,tmp)
	stringIndexer = StringIndexer(inputCol=tmp, outputCol=col,handleInvalid='skip')
	model = stringIndexer.fit(df)
	labels = model.labels
	indexed = model.transform(df)
	df = indexed
	df = drop_column(df,tmp)
	return df,labels

# model.getDropLast() returns True
# the "all zeros" category is the "missing data" category.
# handleInvalid in ('keep','skip','error')
def cast_indexed_onehot(df,col):
	df = rename_column(df,col,tmp)
	encoder = OneHotEncoder(inputCol=tmp, outputCol=col,dropLast=True)
	model = encoder.fit(df)
	encoded = model.transform(df)

	df = encoded
	df = drop_column(df,tmp)
	return df



# https://spark.apache.org/docs/1.5.2/ml-features.html
def cast_cols_categorical_onehot(df,cols):
	print(f"Casting {', '.join(cols)} as one-hot")
	labels_dict = {}
	for col in cols:
		df,labels = cast_categorical_string_indexed(df,col)
		labels_dict[col] = labels
		df = cast_indexed_onehot(df,col)
	return df, labels_dict


# https://stackoverflow.com/questions/46779993/creating-datetime-from-string-column-in-pyspark
# https://stackoverflow.com/questions/53935964/convert-date-column-to-age-with-scala-and-spark
# https://stackoverflow.com/questions/44020818/how-to-calculate-date-difference-in-pyspark
# unix_timestamp(timestamp='19880922',format='yyyyMMdd')
def cast_cols_date(df,cols,endDate="02-18-2021",datefmt='yyyyMMdd'):
	print(f"Casting {', '.join(cols)} as days-to {endDate}")
	for col in cols:
		# df = df.withColumn(col, from_unixtime(unix_timestamp(df[f'`{col}`']), 'yyyyMMdd'))
		df = rename_column(df,col,tmp)
		df = df.withColumn('today',to_date(lit(endDate),'MM-dd-yyyy')) #44 days after 20210105
		df = df.withColumn('tmpDate', to_date(df[tmp],datefmt))
		df = df.withColumn(col,datediff('today','tmpDate'))
		df = drop_column(df,tmp)
		df = drop_column(df,'today')
		df = drop_column(df,'tmpDate')
	return df

def filter_dates_before(df,datecol,only_before="02-18-2021",datefmt='yyyy-MM-dd',drop_after_filter=True):
	print(f"Removing rows with value of {datecol} after {only_before}")
	# df = df.withColumn(col, from_unixtime(unix_timestamp(df[f'`{col}`']), 'yyyyMMdd'))
	# df = rename_column(df,datecol,tmp)
	df = df.withColumn('end_date',to_date(lit(only_before),datefmt)) #44 days after 20210105
	df = df.withColumn('tmpDate', to_date(df[datecol],datefmt))
	df = df.withColumn('datediff',datediff('end_date','tmpDate'))
	df = df.filter(df['datediff']>=0)
	df = drop_column(df,'end_date')
	df = drop_column(df,'tmpDate')
	df = drop_column(df,'datediff')
	if drop_after_filter:
		print(f"DROPPING {datecol}")
		df = drop_column(df,datecol)
	return df

def cast_cols_year(df,cols,endYear=2021):
	print(f"Casting {', '.join(cols)} as years to {endYear}")
	for col in cols:
		# df = df.withColumn(col, from_unixtime(unix_timestamp(df[f'`{col}`']), 'yyyyMMdd'))
		df = cast_col(df,col,casttype='double')
		df = rename_column(df,col,tmp)
		df = df.withColumn(col, lit(endYear)- df[tmp])
		df = drop_column(df,tmp)
	return df

def cast_cols_date_as_year(df,cols,endYear=2021,endDate="02-18-2021",datefmt='yyyyMMdd'):
	print(f"Casting {', '.join(cols)} (with full dates) as years to {endYear}")
	for col in cols:
		# df = df.withColumn(col, from_unixtime(unix_timestamp(df[f'`{col}`']), 'yyyyMMdd'))
		df = rename_column(df,col,tmp)
		df = df.withColumn('today',to_date(lit(endDate),'MM-dd-yyyy')) #44 days after 20210105
		df = df.withColumn('tmpDate', to_date(df[tmp],datefmt))
		df = df.withColumn(col,datediff('today','tmpDate'))
		df = drop_column(df,tmp)
		df = drop_column(df,'today')
		df = drop_column(df,'tmpDate')
		df = multiply_column_by_constant(df,col,1/365)
	return df


def add_column_to_constant(df,col,const):
	df = df.withColumn(tmp,const + df[col])
	df = drop_column(df,col)
	df = rename_column(df,tmp,col)
	return df


def multiply_column_by_constant(df,col,const):
	df = df.withColumn(tmp,const*df[col])
	df = drop_column(df,col)
	df = rename_column(df,tmp,col)
	return df

# pdf = sample_pandas(df,nrows=10,printdf=True)
def cast_numeric_binary(df,col,lower_threshold=44,default_val=10000000):
	print(f"Thresholding {col} to binary at {lower_threshold}")
	df = cast_col(df,col,casttype='double')
	df = multiply_column_by_constant(df,col,-1)
	df = rename_column(df,col,tmp)
	binarizer = Binarizer(threshold=-1*lower_threshold-0.5, inputCol=tmp, outputCol=col)
	df = binarizer.transform(df)
	df = drop_column(df,tmp)
	return df






# https://stackoverflow.com/questions/29600673/how-to-delete-columns-in-pyspark-dataframe
def drop_column(df,col):
	return df.drop(col)

def rename_column(df,col,newcol):
	return df.withColumnRenamed(col,newcol)

def df_subset(df,cols):
	return df.select(*cols)


def printSchema(df):
	df.printSchema()


def print_pdf(pdf):
	printrows = pd.get_option('display.max_rows')
	pd.set_option('display.max_rows',len(pdf.columns))
	print(pdf.transpose())
	pd.set_option('display.max_rows',printrows)


# https://towardsdatascience.com/machine-learning-with-pyspark-and-mllib-solving-a-binary-classification-problem-96396065d2aa
def sample_pandas(df,nrows=20,printdf=False):
	if nrows is None:
		pdf = pd.DataFrame(df.collect(),columns=df.columns)
	else:
		pdf = pd.DataFrame(df.take(nrows),columns=df.columns)
	if printdf:
		print_pdf(pdf)
	return pdf

def run_query(query):
	spark = get_session(extramemory=False)
	return spark.sql(query)


def run_query_on_tmp(df,query_with_tmp):
	df.createTempView("tmp")
	try:
		result = pd.DataFrame(run_query(query_with_tmp).collect())
	except Exception as e:
		print(f"Error executing\n'{query_with_tmp}'")
		print(e)
		result = pd.DataFrame()
	drop_temp_view('tmp')
	return result


def get_distinct_values(df,column):
	query = f'''
		SELECT DISTINCT "{column}" FROM tmp;
	'''
	return run_query_on_tmp(df,query)[0].values



# eg returns two-column pandas dataframe with values (eg of tract ID) and frequencies
def get_frequency_of_vals(df,col):
	query = f'''
	select {col}, count(*) as num
	from tmp
	GROUP BY
	  {col}
	ORDER BY num DESC;
	'''
	return run_query_on_tmp(df,query)


def drop_temp_view(viewname):
	spark = get_session(extramemory=False)
	spark.catalog.dropTempView(viewname)

# https://stackoverflow.com/questions/48421651/removing-null-nan-empty-space-from-pyspark-dataframe
# def drop_na_and_empty(df,cols=None):
# 	if cols is None:
# 		cols = df.columns
# 	def to_null(c):
# 		return when(~(colFun(c).isNull() | isnan(colFun(c)) | (trim(colFun(c)) == "")), colFun(c))
# 	df = df.select([to_null(c).alias(c) for c in cols]).na.drop()
# 	# df = df.na.drop()
# 	# df.fill()
# 	return df

# https://stackoverflow.com/questions/33287886/replace-empty-strings-with-none-null-values-in-dataframe
def replace_empty_with_na(df,cols=None):
	def blank_as_null(c):
		return when(colFun(c) != "", colFun(c)).otherwise(None)
	if cols is None:
		cols = df.columns
	for col in cols:
		df = df.withColumn(col, blank_as_null(col))
	return df

def drop_na(df,cols=None):
	if cols is None:
		cols = df.columns
	df = df.na.drop(subset=cols)
	return df


def replace_na_with_default(df,col,default):
	return df.na.fill(default,subset=[col])

# https://datascience.stackexchange.com/questions/14648/replace-all-numeric-values-in-a-pyspark-dataframe-by-a-constant-value
# CF: to get all distinct values
# df.select(column).distinct().show()
def cast_nonnull_to_default(df,column,nullval=0,nonnullval=1):
	print(f"Replacing non-null values in {column} with {nonnullval} and otherwise {nullval}")
	df = rename_column(df,column,tmp)
	df = df.withColumn(column, when(df[tmp].isNotNull(), nonnullval).otherwise(nullval))
	df = drop_column(df,tmp)
	return df

# https://stackoverflow.com/questions/42537051/pyspark-when-function-with-multiple-outputs
def relabel_vals(df,column,valsdict,change_others_to_default=False,default_val=0,):
	print(f"Replacing values in {column} via {valsdict} with default value {default_val} (changing to default: {change_others_to_default}")
	df = rename_column(df,column,tmp)
	clause = None
	for (oldval,newval) in valsdict.items():
		clause = clause.when(df[tmp]==oldval,newval) if clause is not None else when(df[tmp]==oldval,newval)
	if change_others_to_default:
		clause = clause.otherwise(default_val)
	df = df.withColumn(column, clause)
	df = drop_column(df,tmp)
	return df

# https://towardsdatascience.com/machine-learning-with-pyspark-and-mllib-solving-a-binary-classification-problem-96396065d2aa
#https://stackoverflow.com/questions/41362295/sparkexception-values-to-assemble-cannot-be-null
def get_feature_vector_col(df,cols,featureOutputCol='lrFeatures'):
	assembler = VectorAssembler(inputCols=cols, outputCol=featureOutputCol)
	# assembler = assembler.setHandleInvalid('skip') # necessary if getting issues iterating through data
	df = assembler.transform(df)
	return df

# https://stackoverflow.com/questions/38384347/how-to-split-vector-into-columns-using-pyspark
## Usage:
# for col, labels in labels_dict.items():
# 	df = vector_column_to_array_columns(df,col,col_indices=labels)
def vector_column_to_array_columns(df,col,col_indices=None):
	df = rename_column(df,col,tmp)
	df = df.withColumn(col, vector_to_array(tmp))
	df = drop_column(df,tmp)
	nComponents =  len(df.select(col).take(1)[0][col])
	if col_indices is None:
		col_indices = list(range(nComponents))

	for i,col_ind in zip(range(nComponents),col_indices):
		df = df.withColumn(f"{col}[{col_ind}]",df[col][i])
	df = drop_column(df,col)
	return df
