import pandas as pd
import numpy as np
from pprint import pprint
import os

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.sql.functions as F


import matplotlib.pyplot as plt
import seaborn as sns


from spark_utilities import get_table, df_subset, sample_pandas, drop_column, drop_na, persist_df_to_parquet, read_persisted_table, fromPandas,save_json_in_persistent_storage, load_json_in_persistent_storage, multiply_column_by_constant,vector_column_to_array_columns

from db_utilities import query_chunks, select_all


from data_cleaning import get_lrdf, get_col_explanations, cast_all_columns, log_reg_cols_and_outcome,get_top_tracts

from ml_utilities_sklearn import get_lr

newlinechar= '\n'
# spark= SparkSession.builder.getOrCreate()






def get_df_and_tract_df():
	tablename = "georgia_vf_nationbuilder_full_report_rows"
	temp_tablename = 'sensitivity_vs_tract_df'
	tract_rep_tablename = 'tract_representation'
	try:
		labels_dict = load_json_in_persistent_storage('labels_dict')
		df = read_persisted_table(temp_tablename)
		tract_df = read_persisted_table(tract_rep_tablename,pandas=True)
		print("Successfully loaded df, tract_df, and labels_dict")
	except:
		print("Generating df, tract_df, and labels_dict")
		df = get_table(tablename,printSchema=False)
		tract_df = get_top_tracts(df)
		df = df.filter(df['is_active_voter']=='Y')
		df = df_subset(df,log_reg_cols_and_outcome + ['tract_geoid'])
		df, labels_dict = cast_all_columns(df)
		df = drop_na(df,cols=df.columns) #possibly optional if using 'skip' option to VectorAssembler
		df = df_subset(df,[
			# 'tract_geoid$Total',
			'dist_to_pollingLocation',
			'tract_geoid',
			'dob',
			'demo',
			'sex',
			'voted_2016_11_08',
		])
		# df has 834959 rows
		persist_df_to_parquet(df,temp_tablename)
		persist_df_to_parquet(tract_df,tract_rep_tablename,from_pandas=True)
		save_json_in_persistent_storage(labels_dict,'labels_dict')
		return get_df_and_tract_df()
	tract_df.sort_values(by=['Count'],ascending=False,inplace=True)
	return df, tract_df, labels_dict

# Measure turnout vs. distance in each tract
def turnout_change_versus_tract_properties():
	tablename = "georgia_vf_nationbuilder_full_report_rows"
	temp_tablename = 'sensitivity_vs_tract_df'
	tract_rep_tablename = 'tract_representation'
	distcol = 'dist_to_pollingLocation'
	distmods = np.linspace(start=0.05,stop=2,num=8) #multiplicative

	df, tract_df, labels_dict = get_df_and_tract_df()
	tracts = tract_df['Census_Tract'].values.tolist()
	tract_counts = tract_df['Count'].values.tolist()
	featureOutputCol='features'
	labelCol = 'label'
	pdf = df.toPandas()
	print("\a")
	pdf_for_col_reference = sample_pandas(drop_column(df,'tract_geoid'),nrows=10,printdf=False)#['features'].iloc[0]
	evaluator = BinaryClassificationEvaluator()
	nSplits = 4
	results_df = pd.DataFrame()
	# i, tract_id, tract_count = 0, tracts[0], tract_counts[0]
	for i, (tract_id,tract_count) in enumerate(zip(tracts,tract_counts)):
		if tract_count < 200:
			continue
		filtered = pdf[pdf['tract_geoid']==tract_id]
		# filtered = filtered.drop(columns=['tract_geoid']) #pandas
		filtered = fromPandas(filtered)
		filtered= drop_column(filtered,'tract_geoid') #spark
		# lrdf = get_lrdf(filtered)
		true_tract_count = filtered.count()
		print(f"Processing tract {tract_id} ({i}/{len(tracts)}) with {tract_count} samples (true: {true_tract_count})")
		AUCs= []
		all_lr_coefs=[]
		intercepts = []
		all_pred_turnout = [] #done
		all_counterfactual_turnout = {distmod: [] for distmod in distmods}
		# mean_dists_with_mod = {distmod: [] for distmod in distmods}

		mean_dist = filtered.agg(F.mean(distcol)).collect()[0][0]
		all_true_turnout = filtered.agg(F.mean('voted_2016_11_08')).collect()[0][0]

		for _ in range(nSplits):
			train_filtered, test_filtered = filtered.randomSplit([0.8,0.2],seed=0)
			train, test = get_lrdf(train_filtered), get_lrdf(test_filtered)
			# lrdf = get_lrdf(filtered)
			# train, test = lrdf.randomSplit([0.8,0.2],seed=0)
			lr = LogisticRegression(featuresCol=featureOutputCol, labelCol=labelCol, maxIter=10)
			lrModel = lr.fit(train)
			predictions = lrModel.transform(test)

			#predictions/counterfactuals
			all_pred_turnout.append(predictions.select('probability').rdd.map(lambda row: float(row['probability'][1]) ).mean())

			for j, distmod in enumerate(distmods):
				# df_mod = add_column_to_constant(df,distcol,distmod).select(df.columns)
				test_mod = multiply_column_by_constant(test_filtered,distcol,distmod)
				test_mod_lrdf = get_lrdf(test_mod)
				all_counterfactual_turnout[distmod].append(
					lrModel.transform(test_mod_lrdf)\
					.select('probability')\
					.rdd.map(lambda row: float(row['probability'][1]))\
					.mean()
				)

			# predictions/counterfactuals

			AUCs.append(evaluator.evaluate(predictions))
			all_lr_coefs.append(lrModel.coefficients.toArray().tolist())
			intercepts.append(lrModel.interceptVector[0])

		avg_lr_coefs = np.mean(all_lr_coefs,axis=0).tolist()
		avg_intercept = np.mean(intercepts)
		pred_turnout = np.mean(all_pred_turnout)
		col_explanations = get_col_explanations(filtered,avg_lr_coefs.copy(),labels_dict,pdf=pdf_for_col_reference)
		all_counterfactual_turnout = {f"turnout_with_dist_times_{k}" : np.mean(v) for k,v in all_counterfactual_turnout.items()}
		AUC = np.mean(AUCs)
		result = {
			'tract_geoid' : tract_id, 'nSamples': tract_count, 'filtered_nSamples' : true_tract_count,'AUC' : AUC, 'intercept': avg_intercept, 'mean_distance':mean_dist,'test_predicted_turnout': pred_turnout, 'true_turnout': all_true_turnout,**all_counterfactual_turnout, **col_explanations,
		}
		results_df = results_df.append(pd.DataFrame.from_records([result]))
		if i % 10 == 0:
			results_df.to_pickle('tract_LR_coefficients_with_distmod.pickle')

	results_df.to_pickle('tract_LR_coefficients_with_distmod.pickle')





def model_coefficient_versus_tract_properties():
	tablename = "georgia_vf_nationbuilder_full_report_rows"
	temp_tablename = 'sensitivity_vs_tract_df'
	tract_rep_tablename = 'tract_representation'
	df, tract_df, labels_dict = get_df_and_tract_df()
	tracts = tract_df['Census_Tract'].values.tolist()
	tract_counts = tract_df['Count'].values.tolist()
	featureOutputCol='features'
	labelCol = 'label'
	pdf = df.toPandas()
	print("\a")
	pdf_for_col_reference = sample_pandas(drop_column(df,'tract_geoid'),nrows=10,printdf=False)#['features'].iloc[0]
	evaluator = BinaryClassificationEvaluator()
	nSplits = 4
	results_df = pd.DataFrame()
	# i, tract_id, tract_count = 0, tracts[0], tract_counts[0]
	for i, (tract_id,tract_count) in enumerate(zip(tracts,tract_counts)):
		print(f"Processing tract {tract_id} ({i}/{len(tracts)}) with {tract_count} samples")
		if tract_count < 200:
			continue
		filtered = pdf[pdf['tract_geoid']==tract_id]
		# filtered = filtered.drop(columns=['tract_geoid']) #pandas
		filtered = fromPandas(filtered)
		filtered= drop_column(filtered,'tract_geoid') #spark
		lrdf = get_lrdf(filtered)
		true_tract_count = filtered.count()
		AUCs= []
		all_lr_coefs=[]
		intercepts = []
		for _ in range(nSplits):
			train, test = lrdf.randomSplit([0.8,0.2],seed=0)
			lr = LogisticRegression(featuresCol=featureOutputCol, labelCol=labelCol, maxIter=10)
			lrModel = lr.fit(train)
			predictions = lrModel.transform(test)
			AUCs.append(evaluator.evaluate(predictions))
			all_lr_coefs.append(lrModel.coefficients.toArray().tolist())
			intercepts.append(lrModel.interceptVector[0])
		avg_lr_coefs = np.mean(all_lr_coefs,axis=0).tolist()
		avg_intercept = np.mean(intercepts)
		col_explanations = get_col_explanations(filtered,avg_lr_coefs.copy(),labels_dict,pdf=pdf_for_col_reference)
		AUC = np.mean(AUCs)
		result = {'tract_geoid': tract_id, 'nSamples': tract_count, 'filtered_nSamples' : true_tract_count,'AUC' : AUC, **col_explanations, 'intercept': avg_intercept}
		results_df = results_df.append(pd.DataFrame.from_records([result]))
		if i % 10 == 0:
			results_df.to_pickle('tract_LR_coefficients.pickle')

	results_df.to_pickle('tract_LR_coefficients.pickle')




def plot_turnout_vs_tract():
	# model_coefficient_versus_tract_properties()
	acs_tablename = 'acs5_2019'
	tract_data = select_all(acs_tablename,chunksize=None)

	results_df = pd.read_pickle('tract_LR_coefficients.pickle').merge(tract_data,left_on='tract_geoid',right_on='GEO_ID',how='inner')
	results_df['dist_to_pollingLocation'] = results_df['dist_to_pollingLocation'].apply(lambda row: row[0])
	results_df['frac_Black'] = results_df['Total_Black_or_African_American_alone']/results_df['Total']
	results_df['vehicles_per_person'] = results_df['Total_No_vehicle_available']/results_df['Total']
	results_df["frac_bachelor"] = results_df["Total_Bachelor's_degree"]/results_df["Total"]

	results_df.sort_values(by=['filtered_nSamples','AUC'], axis=0, inplace=True) # So that small/low AUC points are at bottom of plot (order=zorder)
	AUCMIN=0.6
	# results_df = results_df[results_df['AUC']>AUCMIN]
	# foldername=f'./figures/coefficient_study/AUC>{AUCMIN}'
	foldername=f'./figures/coefficient_study'
	if not os.path.exists(foldername):
		os.mkdir(foldername)
	for feature in ['Median_household_income_in_the_past_12_mon', 'frac_Black','vehicles_per_person', "frac_bachelor"]:
		print(f"Plotting {feature}")
		# feature = 'Median_household_income_in_the_past_12_mon'
		sns_axisgrid = sns.relplot(
	        x=feature,
			y='dist_to_pollingLocation',
			size='filtered_nSamples',
			hue='AUC',
			palette=sns.cubehelix_palette(rot=-.2, as_cmap=True),
			alpha=0.5,
			kind='scatter',
			data=results_df,
		)
		sns_axisgrid.set(title=f"Coefficient of 'dist_to_pollingLocation'\nvs. '{feature}'")
		fig = sns_axisgrid.fig
		plt.tight_layout()
		# fig.savefig(f"figures/coefficient_study/{feature}.pdf")
		fig.savefig(f"{foldername}/{feature}.pdf")


if __name__=="__main__":
	# turnout_change_versus_tract_properties()
	# model_coefficient_versus_tract_properties()
	# plot_turnout_vs_tract()
	tablename = "georgia_vf_nationbuilder_full_report_rows"
	temp_tablename = 'sensitivity_vs_tract_df'
	tract_rep_tablename = 'tract_representation'
	distcol = 'dist_to_pollingLocation'
	distmods = np.linspace(start=0.05,stop=2,num=8) #multiplicative

	df, tract_df, labels_dict = get_df_and_tract_df()
	tracts = tract_df['Census_Tract'].values.tolist()
	tract_counts = tract_df['Count'].values.tolist()
	featureOutputCol='features'
	labelCol = 'label'
	pdf = df.toPandas()
	print("\a")
	pdf_for_col_reference = sample_pandas(drop_column(df,'tract_geoid'),nrows=10,printdf=False)#['features'].iloc[0]
	evaluator = BinaryClassificationEvaluator()
	nSplits = 4
	results_df = pd.DataFrame()

	for col, labels in labels_dict.items():
		df = vector_column_to_array_columns(df,col,col_indices=labels)

	pdf = df.toPandas()

	outcome = 'voted_2016_11_08'
	# y = pdf[outcome]
	# pdf.drop(columns=[outcome, 'tract_geoid'], inplace=True)


	## TODO: fixed-effect columns

	# lr = get_lr(pdf,y)




	raise
	# i, tract_id, tract_count = 0, tracts[0], tract_counts[0]
	for i, (tract_id,tract_count) in enumerate(zip(tracts,tract_counts)):
		if tract_count < 200:
			continue
		filtered = pdf[pdf['tract_geoid']==tract_id]
		# filtered = filtered.drop(columns=['tract_geoid']) #pandas
		filtered = fromPandas(filtered)
		filtered= drop_column(filtered,'tract_geoid') #spark
		# lrdf = get_lrdf(filtered)
		true_tract_count = filtered.count()
		print(f"Processing tract {tract_id} ({i}/{len(tracts)}) with {tract_count} samples (true: {true_tract_count})")
		AUCs= []
		all_lr_coefs=[]
		intercepts = []
		all_pred_turnout = [] #done
		all_counterfactual_turnout = {distmod: [] for distmod in distmods}
		# mean_dists_with_mod = {distmod: [] for distmod in distmods}

		mean_dist = filtered.agg(F.mean(distcol)).collect()[0][0]
		all_true_turnout = filtered.agg(F.mean('voted_2016_11_08')).collect()[0][0]

		for _ in range(nSplits):
			train_filtered, test_filtered = filtered.randomSplit([0.8,0.2],seed=0)
			train, test = get_lrdf(train_filtered), get_lrdf(test_filtered)
			# lrdf = get_lrdf(filtered)
			# train, test = lrdf.randomSplit([0.8,0.2],seed=0)
			lr = LogisticRegression(featuresCol=featureOutputCol, labelCol=labelCol, maxIter=10)
			lrModel = lr.fit(train)
			predictions = lrModel.transform(test)

			#predictions/counterfactuals
			all_pred_turnout.append(predictions.select('probability').rdd.map(lambda row: float(row['probability'][1]) ).mean())

			for j, distmod in enumerate(distmods):
				# df_mod = add_column_to_constant(df,distcol,distmod).select(df.columns)
				test_mod = multiply_column_by_constant(test_filtered,distcol,distmod)
				test_mod_lrdf = get_lrdf(test_mod)
				all_counterfactual_turnout[distmod].append(
					lrModel.transform(test_mod_lrdf)\
					.select('probability')\
					.rdd.map(lambda row: float(row['probability'][1]))\
					.mean()
				)

			# predictions/counterfactuals

			AUCs.append(evaluator.evaluate(predictions))
			all_lr_coefs.append(lrModel.coefficients.toArray().tolist())
			intercepts.append(lrModel.interceptVector[0])

		avg_lr_coefs = np.mean(all_lr_coefs,axis=0).tolist()
		avg_intercept = np.mean(intercepts)
		pred_turnout = np.mean(all_pred_turnout)
		col_explanations = get_col_explanations(filtered,avg_lr_coefs.copy(),labels_dict,pdf=pdf_for_col_reference)
		all_counterfactual_turnout = {f"turnout_with_dist_times_{k}" : np.mean(v) for k,v in all_counterfactual_turnout.items()}
		AUC = np.mean(AUCs)
		result = {
			'tract_geoid' : tract_id, 'nSamples': tract_count, 'filtered_nSamples' : true_tract_count,'AUC' : AUC, 'intercept': avg_intercept, 'mean_distance':mean_dist,'test_predicted_turnout': pred_turnout, 'true_turnout': all_true_turnout,**all_counterfactual_turnout, **col_explanations,
		}
		results_df = results_df.append(pd.DataFrame.from_records([result]))
		if i % 10 == 0:
			print(i)
			results_df.to_pickle('tract_LR_coefficients_with_distmod.pickle')

	results_df.to_pickle('tract_LR_coefficients_with_distmod.pickle')

