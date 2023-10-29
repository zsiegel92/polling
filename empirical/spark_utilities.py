import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, DateType,FloatType
from pyspark.sql.functions import dayofmonth,from_unixtime,month, unix_timestamp, year,datediff, to_date, lit
from pyspark.sql.functions import col as colFun, isnan, when, trim
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, Binarizer, VectorAssembler
from pyspark.ml import Pipeline

# import logging
# from pyspark import SparkContext
# s_logger = logging.getLogger('py4j.java_gateway')
# s_logger.setLevel(logging.ERROR)


# from log_reg_model import log_reg_cols, date_cols, categorical_cols, float_cols, outcome,log_reg_cols_and_outcome, year_cols

tmp='tmp'


def get_session():
	return SparkSession \
		.builder \
		.appName("Polling ML") \
		.config("spark.jars", "/Users/zach/Documents/UCLA_classes/research/Polling/src/empirical/spark_pgsql_jdbc_driver/postgresql-42.2.19.jar") \
		.getOrCreate()

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
def cast_cols_date(df,cols,endDate="02-18-2021"):
	print(f"Casting {', '.join(cols)} as days-to {endDate}")
	for col in cols:
		# df = df.withColumn(col, from_unixtime(unix_timestamp(df[f'`{col}`']), 'yyyyMMdd'))
		df = rename_column(df,col,tmp)
		df = df.withColumn('today',to_date(lit(endDate),'MM-dd-yyyy')) #44 days after 20210105
		df = df.withColumn('tmpDate', to_date(df[tmp],'yyyyMMdd'))
		df = df.withColumn(col,datediff('today','tmpDate'))
		df = drop_column(df,tmp)
		df = drop_column(df,'today')
		df = drop_column(df,'tmpDate')
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
	pdf = pd.DataFrame(df.take(nrows),columns=df.columns)
	if printdf:
		print_pdf(pdf)
	return pdf

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

# https://towardsdatascience.com/machine-learning-with-pyspark-and-mllib-solving-a-binary-classification-problem-96396065d2aa
#https://stackoverflow.com/questions/41362295/sparkexception-values-to-assemble-cannot-be-null
def get_feature_vector_col(df,cols,featureOutputCol='lrFeatures'):
	assembler = VectorAssembler(inputCols=cols, outputCol=featureOutputCol)
	# assembler = assembler.setHandleInvalid('skip') # necessary if getting issues iterating through data
	df = assembler.transform(df)
	return df
