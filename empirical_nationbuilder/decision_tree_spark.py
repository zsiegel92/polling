import pandas as pd
import numpy as np
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.sql.functions as F
from pyspark.ml.classification import MultilayerPerceptronClassifier, DecisionTreeClassifier
# from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
# from pyspark.mllib.util import MLUtils


from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt

from spark_utilities import get_table, df_subset, printSchema, sample_pandas, cast_cols_date,cast_cols, cast_cols_year, cast_cols_categorical_onehot, cast_numeric_binary,get_feature_vector_col, drop_column, replace_empty_with_na, drop_na,replace_na_with_default, rename_column, add_column_to_constant, multiply_column_by_constant

from log_reg_model import log_reg_cols, date_cols, categorical_cols, float_cols, outcome,log_reg_cols_and_outcome, year_cols, log_reg_cols_without_outcome, fractionCols, totalGenderPopCols

from data_cleaning import get_lrdf, get_col_explanations, get_ratio_cols, get_pop_density, cast_all_columns

# spark= SparkSession.builder.getOrCreate()



# https://spark.apache.org/docs/latest/ml-classification-regression.html#multilayer-perceptron-classifier
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.MultilayerPerceptronClassifier.html#pyspark.ml.classification.MultilayerPerceptronClassifier
# "Number of inputs has to be equal to the size of feature vectors. Number of outputs has to be equal to the total number of labels."
def get_mlp(lrdf,featureOutputCol='features', labelCol = 'label', hiddenLayers = [40,40]):
	pdf = sample_pandas(lrdf,nrows=1,printdf=False)
	n_input = pdf[featureOutputCol][0].size
	n_output = 2 # for 2 categories - not 1! Uses one-hot encoding. # pdf[labelCol][0].size
	layers = [n_input] + hiddenLayers + [n_output]
	trainer = MultilayerPerceptronClassifier(maxIter=100, featuresCol=featureOutputCol, labelCol=labelCol,layers=layers, seed=1234)
	nnModel = trainer.fit(lrdf)
	return nnModel

# https://spark.apache.org/docs/1.5.2/ml-decision-tree.html
# https://spark.apache.org/docs/1.5.2/api/python/pyspark.ml.html#pyspark.ml.classification.DecisionTreeClassifier
def get_dtree(lrdf,featureOutputCol='features', labelCol = 'label'):
	dt = DecisionTreeClassifier(labelCol=labelCol, featuresCol=featureOutputCol, predictionCol="prediction", probabilityCol="probability", rawPredictionCol="rawPrediction", maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini")
	model = dt.fit(lrdf)
	return model


def measure_effect_of_distance_dtree(df,labels_dict):
	distance_cols = ('dist_to_pollingLocation', 'dist_to_dropOffLocation', 'dist_to_earlyVoteSite')
	# distmods = np.linspace(start=-1,stop=2,num=3) #additive
	distmods = np.linspace(start=0.05,stop=2,num=8) #multiplicative
	N = df.count()
	lrdf = get_lrdf(df)
	dtModel = get_dtree(lrdf)

	true_turnout = df.agg(F.sum(outcome)).collect()[0][0]/N
	true_mean_dist = {distcol : df.agg(F.mean(distcol)).collect()[0][0] for distcol in distance_cols}
	# true_stdev_dist = {distcol: df.agg(F.stddev(distcol)).collect()[0][0] for distcol in distance_cols}
	true_pred_turnout = dtModel.transform(lrdf).select('probability').rdd.map(lambda row: (float(row['probability'][1]))).mean()
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
				dtModel.transform(lrdf_mod)\
				.select('probability')\
				.rdd.map(lambda row: (float(row['probability'][1])))\
				.mean()
			)
	fig,axs = plt.subplots(1,len(distance_cols),figsize=(15,5),sharey=True)
	for i, (ax,distcol) in enumerate(zip(axs,distance_cols)):
		distcol_mod_mean_dists = mod_mean_dist[distcol]
		# distcol_mod_std_dists = mod_std_dist[distcol]
		distcol_pred_turnout = pred_turnout[distcol]
		ax.set_xlabel(f"Mean {distcol}")
		# ax.set_ylabel(f"Predicted Turnout")
		ax.set_title(f"Predicted Turnout vs. Mean {distcol}")
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
	plt.savefig(f"figures/turnout_vs_adjusted_distance_dtree.pdf")



## TODO:
# * create combination columns (e.g. ratio of car owners, total individuals, etc.)
# *
##

if __name__=="__main__":
# if False:
	tablename = "georgia_vf_2021_full_report_rows"
	df = get_table(tablename,printSchema=False)
	df = df_subset(df,log_reg_cols_and_outcome)
	df, labels_dict = cast_all_columns(df)
	df = drop_na(df,cols=[outcome])
	df = drop_na(df,cols=df.columns) #possibly optional if using 'skip' option to VectorAssembler

	distcol = 'dist_to_pollingLocation'
	df = df.filter(df[distcol] < 7)

	# lrModel = fit_lr(df)
	# pdf = sample_pandas(df,nrows=1000,printdf=False)
	# pdf = sample_pandas(lrdf,nrows=1000,printdf=False)
	# train_test_lr(df,labels_dict,plotting_roc=True)
	# measure_effect_of_distance(df,labels_dict)
	measure_effect_of_distance_dtree(df,labels_dict)
