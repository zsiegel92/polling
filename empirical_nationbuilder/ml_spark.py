import pandas as pd
import numpy as np
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.sql.functions as F
from pyspark.ml.classification import MultilayerPerceptronClassifier

import matplotlib.pyplot as plt
from plot_utilities import plot_roc, plot_hist

from ml_utilities import get_mlp, get_lrModel

from spark_utilities import get_table, df_subset, printSchema, sample_pandas, cast_cols_date,cast_cols, cast_cols_year, cast_cols_categorical_onehot, cast_numeric_binary,get_feature_vector_col, drop_column, replace_empty_with_na, drop_na,replace_na_with_default, rename_column, add_column_to_constant, multiply_column_by_constant,run_query_on_tmp

from db_utilities import query_chunks


from data_cleaning import get_lrdf, get_col_explanations, cast_all_columns, outcome, log_reg_cols_and_outcome,get_top_tracts, filter_top_n_tracts

newlinechar= '\n'
# spark= SparkSession.builder.getOrCreate()



def plot_roc_classifier(df,labels_dict,plotextra="",method='lr'):
	lrdf = get_lrdf(df)

	# lrdf.groupby(labelCol).count().show()
	train, test = lrdf.randomSplit([0.8,0.2],seed=0)
	if method == 'lr':
		model = get_lrModel(train)
	elif method == 'nn':
		model = get_mlp(train)

	predictions = model.transform(test)
	preds = predictions.select('label','probability').rdd.map(lambda row: (float(row['probability'][1]),float(row['label']))).collect()
	scores, target = zip(*preds)
	evaluator = BinaryClassificationEvaluator()
	AUC = evaluator.evaluate(predictions)
	print(f"AUC={AUC}")
	plot_roc(target, scores,AUC=AUC,title=f"ROC - Predicting Turnout{newlinechar + plotextra if plotextra else ''}", saveTitle=f'figures/{method}_roc{plotextra.replace(" ","_").replace("-","_")}.pdf')

def measure_effect_of_distance(df,labels_dict,method='lr',plotextra=''):
	distance_cols = ('dist_to_pollingLocation',)
	# distmods = np.linspace(start=-1,stop=2,num=3) #additive
	distmods = np.linspace(start=0.05,stop=2,num=8) #multiplicative
	N = df.count()
	lrdf = get_lrdf(df)
	if method == 'lr':
		model = get_lrModel(lrdf)
	elif method == 'nn':
		model = get_mlp(lrdf)

	true_turnout = df.agg(F.sum(outcome)).collect()[0][0]/N
	true_mean_dist = {distcol : df.agg(F.mean(distcol)).collect()[0][0] for distcol in distance_cols}
	true_stdev_dist = {distcol: df.agg(F.stddev(distcol)).collect()[0][0] for distcol in distance_cols}
	true_pred_turnout = model.transform(lrdf).select('probability').rdd.map(lambda row: (float(row['probability'][1]))).mean()
	mod_mean_dist, mod_std_dist, pred_turnout = {},{},{}
	for distcol in distance_cols:
		print(f"Processing {distcol}")
		mod_mean_dist[distcol], mod_std_dist[distcol], pred_turnout[distcol] = [],[],[]
		for j, distmod in enumerate(distmods):
			print(f"{j}/{len(distmods)}",end='   ',flush=True)
			# df_mod = add_column_to_constant(df,distcol,distmod).select(df.columns)
			df_mod = multiply_column_by_constant(df,distcol,distmod).select(df.columns)
			lrdf_mod = get_lrdf(df_mod)
			mod_mean_dist[distcol].append(df_mod.agg(F.mean(distcol)).collect()[0][0])
			mod_std_dist[distcol].append(df_mod.agg(F.mean(distcol)).collect()[0][0])
			pred_turnout[distcol].append(
				model.transform(lrdf_mod)\
				.select('probability')\
				.rdd.map(lambda row: (float(row['probability'][1])))\
				.mean()
			)
	fig,axs = plt.subplots(1,len(distance_cols),figsize=(5*len(distance_cols),5),sharey=True)
	try:
		axs[0]
	except:
		axs = [axs]
	for i, (ax,distcol) in enumerate(zip(axs,distance_cols)):
		distcol_mod_mean_dists = mod_mean_dist[distcol]
		# distcol_mod_std_dists = mod_std_dist[distcol]
		distcol_pred_turnout = pred_turnout[distcol]
		ax.set_xlabel(f"Mean {distcol}")
		# ax.set_ylabel(f"Predicted Turnout")
		ax.set_title(f"Predicted Turnout vs. Mean {distcol}{newlinechar + plotextra if plotextra else ''}")
		ax.plot(distcol_mod_mean_dists,distcol_pred_turnout,label="Counterfactual\nPredicted Turnout", color="blue",marker="o",zorder=0)
		for distmod, xcoord,ycoord in zip(distmods,distcol_mod_mean_dists,distcol_pred_turnout):
			ax.text(xcoord+.1,ycoord,f"$\\times{distmod : .02}$")
		# ax.errorbar(distcol_mod_mean_dists, distcol_pred_turnout,xerr=distcol_mod_std_dists, color="blue",label="Counterfactual Predicted Turnout")
		ax.scatter(true_mean_dist[distcol], true_turnout,marker="^",color="orange",label="True Turnout",zorder=1)
		# ax.errorbar(true_mean_dist[distcol], true_turnout, xerr=true_stdev_dist[distcol], color="orange",label="True Turnout")
		ax.scatter(true_mean_dist[distcol], true_pred_turnout,marker="^",color="red",label="Model Predicted Turnout",zorder=1)
		# ax.vlines(true_mean_dist[distcol],ax.get_ylim()[0],ax.get_ylim()[1],color="green",label=f"True {distcol}")
		ax.legend(loc='lower right',fontsize=7)
		if i == 0:
			ax.set_ylabel("Predicted Turnout")
	plt.tight_layout()
	plt.savefig(f'figures/{method}_turnout_vs_adjusted_distance_{plotextra.replace(" ","_").replace("-","_")}.pdf')


# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.MultilayerPerceptronClassifier.html
def test_mlp():
	from pyspark.ml.linalg import Vectors
	spark= SparkSession.builder.getOrCreate()
	df = spark.createDataFrame([
	    (0.0, Vectors.dense([0.0, 0.0])),
	    (1.0, Vectors.dense([0.0, 1.0])),
	    (1.0, Vectors.dense([1.0, 0.0])),
	    (0.0, Vectors.dense([1.0, 1.0]))], ["label", "features"])
	pdf = sample_pandas(df,nrows=1000,printdf=False)
	mlp = MultilayerPerceptronClassifier(layers=[2, 2, 2], seed=123)
	mlp.setMaxIter(100)
	model = mlp.fit(df)
	testDF = spark.createDataFrame([
	    (Vectors.dense([1.0, 0.0]),),
	    (Vectors.dense([0.0, 0.0]),)], ["features"])
	model.predict(testDF.head().features)
	model.predictRaw(testDF.head().features)
	model.predictProbability(testDF.head().features)
	model.transform(testDF).select("features", "prediction").show()






# https://medium.com/swlh/logistic-regression-with-pyspark-60295d41221
# https://stackoverflow.com/questions/54425084/pyspark-get-threshold-cuttoff-values-for-each-point-in-roc-curve
def train_test_lr(df,labels_dict,plotting_roc=True):
	featureOutputCol='features'
	labelCol = 'label'
	featureCols = [col for col in df.columns if col != outcome]
	# assembler = VectorAssembler(inputCols=featureCols, outputCol=featureOutputCol)
	# lrdf = assembler.transform(df)
	lrdf = get_feature_vector_col(df,featureCols,featureOutputCol=featureOutputCol)
	lrdf = df_subset(lrdf,[featureOutputCol,outcome])
	lrdf = rename_column(lrdf,outcome,labelCol)
	# lrdf.groupby(labelCol).count().show()
	train, test = lrdf.randomSplit([0.8,0.2],seed=0)
	lr = LogisticRegression(featuresCol=featureOutputCol, labelCol=labelCol, maxIter=10)
	lrModel = lr.fit(train)
	predictions = lrModel.transform(test)
	preds = predictions.select('label','probability').rdd.map(lambda row: (float(row['probability'][1]),float(row['label']))).collect()
	scores, target = zip(*preds)
	evaluator = BinaryClassificationEvaluator()
	AUC = evaluator.evaluate(predictions)
	print(f"AUC={AUC}")
	if plotting_roc:
		plot_roc(target, scores,AUC=AUC,title="ROC - Predicting Turnout", saveTitle='figures/roc.pdf')
	pdf = sample_pandas(df,nrows=10,printdf=False)
	# one_row = sample_pandas(lrdf,nrows=10,printdf=False)['features'].iloc[0]
	lr_coefs = lrModel.coefficients.toArray().tolist()
	col_explanations = get_col_explanations(df,lr_coefs,labels_dict)
	print("COEFFICIENTS:")
	pprint(col_explanations)
	print("INTERCEPT:")
	print(lrModel.interceptVector)



# https://spark.apache.org/docs/latest/ml-classification-regression.html#binomial-logistic-regression
# https://towardsdatascience.com/machine-learning-with-pyspark-and-mllib-solving-a-binary-classification-problem-96396065d2aa
def fit_lr(df):
	featureOutputCol='features'
	labelCol = 'label'
	featureCols = [col for col in df.columns if col != outcome]
	lrdf = get_feature_vector_col(df,featureCols,featureOutputCol=featureOutputCol)
	lrdf = df_subset(lrdf,[featureOutputCol,outcome])
	lrdf = rename_column(lrdf,outcome,labelCol)
	lr = LogisticRegression(featuresCol=featureOutputCol, labelCol=labelCol,maxIter=10)
	lrModel = lr.fit(lrdf)
	print("Coefficients: " + str(lrModel.coefficients))
	print("Intercept: " + str(lrModel.intercept))
	trainingSummary = lrModel.summary
	trainingSummary.roc.show()
	print("areaUnderROC: " + str(trainingSummary.areaUnderROC))
	fMeasure = trainingSummary.fMeasureByThreshold
	maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()
	bestThreshold = fMeasure.where(fMeasure['F-Measure'] == maxFMeasure['max(F-Measure)']).select('threshold').head()['threshold']
	lr.setThreshold(bestThreshold)
	return lrModel


def hist_of_tracts(df):
	tract_df = get_top_tracts(df)
	plot_hist(tract_df,"Count",f"Representation of the {len(tract_df)} Census Tracts in Voter History",f'figures/tract_samples_hist.pdf',bins=100)


# https://stackoverflow.com/questions/21288458/in-redshift-postgres-how-to-count-rows-that-meet-a-condition
def turnout_vs_number_in_dataset(df):
	query = f'''
		select tract_geoid,
		count(*) filter (where "voted_2016_11_08"='voted') as numVoted,
		count(*) filter (where "voted_2016_11_08"='absentee') as numAbsentee,
		count(*) as num
		from georgia_vf_nationbuilder_full_report_rows
		GROUP BY
		  tract_geoid
		ORDER BY num DESC;
		'''
	result =query_chunks(query,chunksize=None)
	result['total'] = result['numvoted'] + result['numabsentee']

	voteTypes={'numvoted' : "In-Person",'numabsentee' : "Absentee",'total' : "All Methods"}
	fig,axs = plt.subplots(1,3,figsize=(5*3,5),sharey=True)
	for i, (ax,voteType) in enumerate(zip(axs,voteTypes)):
		ax.scatter(result['num'],result[voteType]/result['num'],label=voteTypes[voteType],s=0.5,alpha=0.5)
		ax.legend()
		ax.set_xlabel("Number of Registered Voters from Tract")
		ax.set_ylabel(f"{voteTypes[voteType]} Turnout")
		ax.set_title(f"Turnout {voteTypes[voteType]} versus Representation\nin the {len(result)} Census Tracts in Voter History")
	plt.savefig('figures/separate_methods_voting_vs_number_represented.pdf')

	fig, ax = plt.subplots()
	ax.scatter(result['num'],result['numvoted']/result['num'],label='In-Person',s=0.5,alpha=0.5)
	ax.scatter(result['num'],result['numabsentee']/result['num'],label='Absentee',s=0.5,alpha=0.5)
	ax.legend()
	ax.set_xlabel("Number of Registered Voters from Tract")
	ax.set_ylabel("Turnout")
	ax.set_title(f"Turnout versus Representation\nin the {len(result)} Census Tracts in Voter History")
	plt.savefig('figures/voting_vs_number_represented.pdf')
## TODO:
# * create combination columns (e.g. ratio of car owners, total individuals, etc.)
# *
##

if __name__=="__main__":
# if False:
	tablename = "georgia_vf_nationbuilder_full_report_rows"
	df = get_table(tablename,printSchema=False)

	if True:
		hist_of_tracts(df)
		turnout_vs_number_in_dataset(df)

	# raise
	# df.select('is_active_voter').distinct().show()
	df = df.filter(df['is_active_voter']=='Y')
	top_tracts = 100
	df = filter_top_n_tracts(df,top_tracts)
	print("NOTE TO SELF: tract_geoid MUST BE IN categorical_cols FOR FIXED-EFFECT MODEL TO WORK!")
	# filterdist=4
	# df = df.filter(df['dist_to_pollingLocation'] < filterdist)

	df = df_subset(df,log_reg_cols_and_outcome)
	df, labels_dict = cast_all_columns(df)
	# df = drop_na(df,cols=[outcome])
	# df = drop_na(df,cols=['census_block','tract_geoid'])
	df = drop_na(df,cols=df.columns) #possibly optional if using 'skip' option to VectorAssembler


	print("\a")
	raise


	# pdf = sample_pandas(df,nrows=1000,printdf=False)
	# distcol = 'dist_to_pollingLocation'


		# lrModel = fit_lr(df)
		# pdf = sample_pandas(df,nrows=1000,printdf=False)
		# pdf = sample_pandas(lrdf,nrows=1000,printdf=False)
		# train_test_lr(df,labels_dict,plotting_roc=True)
		# measure_effect_of_distance(df,labels_dict)

	if True:
		trial_label = f"fixed-effect, Only in-person, top {top_tracts} tracts, only registered before 2016-11-08" #f"(<{filterdist}km)"
		plot_roc_classifier(df,labels_dict,plotextra=trial_label,method='lr')
		measure_effect_of_distance(df,labels_dict,plotextra=trial_label,method='lr')

print("\a")
