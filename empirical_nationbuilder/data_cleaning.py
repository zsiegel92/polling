import pandas as pd
import numpy as np
from pprint import pprint

from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt


from spark_utilities import get_table, df_subset, printSchema, sample_pandas, cast_cols_date,cast_cols, cast_cols_year, cast_cols_categorical_onehot, cast_numeric_binary,get_feature_vector_col, drop_column, replace_empty_with_na, drop_na,replace_na_with_default, rename_column, add_column_to_constant, multiply_column_by_constant,cast_cols_date_as_year,cast_nonnull_to_default,relabel_vals,run_query,drop_temp_view,run_query_on_tmp,get_frequency_of_vals,filter_dates_before

from log_reg_model import log_reg_cols, date_cols, categorical_cols, float_cols, outcome,log_reg_cols_and_outcome, year_cols, log_reg_cols_without_outcome, fractionCols, totalGenderPopCols, date_as_year_cols, date_filter_cols


# spark= SparkSession.builder.getOrCreate()

def get_lrdf(df,featureCols=None):
	featureOutputCol='features'
	labelCol = 'label'
	if featureCols is None:
		featureCols = [col for col in df.columns if col != outcome]
	lrdf = get_feature_vector_col(df,featureCols,featureOutputCol=featureOutputCol)
	lrdf = df_subset(lrdf,[featureOutputCol,outcome])
	lrdf = rename_column(lrdf,outcome,labelCol)
	return lrdf



def get_col_explanations(df,lr_coefs,labels_dict,pdf=None):
	if pdf is None:
		pdf = sample_pandas(df,nrows=10,printdf=False)
	# one_row = sample_pandas(lrdf,nrows=10,printdf=False)['features'].iloc[0]
	lr_coefs = lr_coefs.copy()
	col_coefs = {}
	col_explanations = {}
	for col in pdf.columns:
		if col != outcome:
			try:
				col_dim = len(pdf[col].iloc[0].toArray())
			except:
				col_dim = 1
			col_coefs[col] = [lr_coefs.pop(0) for i in range(col_dim)]
			if col_dim > 1:
				col_explanations[col] = list(zip(col_coefs[col] + ['default'], labels_dict[col])) # drop last category in one-hot encoding!
			else:
				col_explanations[col] = col_coefs[col]
	# print("COEFFICIENTS:")
	# pprint(col_coefs)
	# # print("LABELS:")
	# # pprint(labels_dict)
	# print("CATEGORIES:")
	# pprint(col_explanations)
	return col_explanations
	# col_coefs[col] = [lr_coefs.pop(0) for i in range(col






def get_ratio_cols(df):
	for newColName, sumCols in fractionCols.items():
		df = df.withColumn(newColName, sum([df[col] for col in sumCols])/df['tract_geoid$Total'])
	for newColName,sumCols in fractionCols.items():
		for col in sumCols:
			df = drop_column(df, col)
	return df

def get_pop_density(df):
	df = df.withColumn('popDensity',df['tract_geoid$Total']/df['tract_geoid$ALAND'])
	df= drop_column(df,'tract_geoid$ALAND')
	return df


def filter_top_n_tracts(df,n):
	ranked = get_top_tracts(df)
	census_tracts = ranked['Census_Tract'].values[:n].tolist()
	return df.filter(df['tract_geoid'].isin(census_tracts))

def get_top_tracts(df):
	tract_df = get_frequency_of_vals(df,"tract_geoid")
	tract_df.rename(columns={0: 'Census_Tract', 1: "Count"},inplace=True)
	return tract_df


def cast_all_columns(df):
	df = cast_cols_date(df,date_cols,datefmt='yyyy-MM-dd')
	df = filter_dates_before(df,'registered_at',only_before="2016-11-08",datefmt='yyyy-MM-dd',drop_after_filter=True)
	df = cast_cols(df,float_cols,casttype='float')
	df = cast_cols_year(df,year_cols)
	df = cast_cols_date_as_year(df,date_as_year_cols,datefmt='yyyy-MM-dd')
	df, labels_dict = cast_cols_categorical_onehot(df,categorical_cols)
	# df = cast_nonnull_to_default(df,outcome,0,1) #WORKS - changes outcome to 0,1 depending on whether voted or null
	df = relabel_vals(df,outcome,{'voted' : 1},change_others_to_default=True,default_val=0)
	df = get_ratio_cols(df)
	df = get_pop_density(df)
	return df, labels_dict



## TODO:
# * create combination columns (e.g. ratio of car owners, total individuals, etc.)
# *
##
if __name__=="__main__":
# if False:
	tablename = "georgia_vf_nationbuilder_full_report_rows"
	df = get_table(tablename,printSchema=False)
	# df = df_subset(df,log_reg_cols_and_outcome)
	# df, labels_dict = cast_all_columns(df)
	# df = drop_na(df,cols=[outcome])
	# df = drop_na(df,cols=df.columns) #possibly optional if using 'skip' option to VectorAssembler
