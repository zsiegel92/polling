import pandas as pd
import numpy as np
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.sql.functions as F
from pyspark.ml.classification import MultilayerPerceptronClassifier


from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt

from spark_utilities import get_table, df_subset, printSchema, sample_pandas, cast_cols_date,cast_cols, cast_cols_year, cast_cols_categorical_onehot, cast_numeric_binary,get_feature_vector_col, drop_column, replace_empty_with_na, drop_na,replace_na_with_default, rename_column, add_column_to_constant, multiply_column_by_constant

from log_reg_model import log_reg_cols, date_cols, categorical_cols, float_cols, outcome,log_reg_cols_and_outcome, year_cols, log_reg_cols_without_outcome, fractionCols, totalGenderPopCols

from data_cleaning import get_lrdf, get_col_explanations, get_ratio_cols, get_pop_density, cast_all_columns

# spark= SparkSession.builder.getOrCreate()



def get_lrModel(lrdf, featureOutputCol='features', labelCol = 'label'):
	lr = LogisticRegression(featuresCol=featureOutputCol, labelCol=labelCol, maxIter=10)
	lrModel = lr.fit(lrdf)
	return lrModel



def measure_effect_of_distance(df,labels_dict):
	distance_cols = ('dist_to_pollingLocation', 'dist_to_dropOffLocation', 'dist_to_earlyVoteSite')
	# distmods = np.linspace(start=-1,stop=2,num=3) #additive
	distmods = np.linspace(start=0.05,stop=2,num=8) #multiplicative
	N = df.count()
	lrdf = get_lrdf(df)
	lrModel = get_lrModel(lrdf)
	lr_coefs = lrModel.coefficients.toArray().tolist()
	true_turnout = df.agg(F.sum(outcome)).collect()[0][0]/N
	true_mean_dist = {distcol : df.agg(F.mean(distcol)).collect()[0][0] for distcol in distance_cols}
	true_stdev_dist = {distcol: df.agg(F.stddev(distcol)).collect()[0][0] for distcol in distance_cols}
	true_pred_turnout = lrModel.transform(lrdf).select('probability').rdd.map(lambda row: (float(row['probability'][1]))).mean()
	col_explanations = get_col_explanations(df,lr_coefs,labels_dict)
	print("COEFFICIENTS:")
	pprint(col_explanations)
	print("INTERCEPT:")
	print(lrModel.interceptVector)
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
				lrModel.transform(lrdf_mod)\
				.select('probability')\
				.rdd.map(lambda row: (float(row['probability'][1])))\
				.mean()
			)
	fig,axs = plt.subplots(1,len(distance_cols),figsize=(15,5),sharey=True)
	for i, (ax,distcol) in enumerate(zip(axs,distance_cols)):
		distcol_mod_mean_dists = mod_mean_dist[distcol]
		distcol_mod_std_dists = mod_std_dist[distcol]
		distcol_pred_turnout = pred_turnout[distcol]
		ax.set_xlabel(f"Mean {distcol}")
		# ax.set_ylabel(f"Predicted Turnout")
		ax.set_title(f"Predicted Turnout vs. Mean {distcol}")
		ax.plot(distcol_mod_mean_dists,distcol_pred_turnout,label="Counterfactual\nPredicted Turnout", color="blue",marker="o",zorder=0)
		for distmod, xcoord,ycoord in zip(distmods,distcol_mod_mean_dists,distcol_pred_turnout):
			ax.text(xcoord+.1,ycoord+.0001,f"$\\times{distmod : .02}$")
		# ax.errorbar(distcol_mod_mean_dists, distcol_pred_turnout,xerr=distcol_mod_std_dists, color="blue",label="Counterfactual Predicted Turnout")
		ax.scatter(true_mean_dist[distcol], true_turnout,marker="^",color="orange",label="True Turnout",zorder=1)
		# ax.errorbar(true_mean_dist[distcol], true_turnout, xerr=true_stdev_dist[distcol], color="orange",label="True Turnout")
		ax.scatter(true_mean_dist[distcol], true_pred_turnout,marker="^",color="red",label="Model Predicted Turnout",zorder=1)
		# ax.vlines(true_mean_dist[distcol],ax.get_ylim()[0],ax.get_ylim()[1],color="green",label=f"True {distcol}")
		ax.legend(loc='lower right',fontsize=7)
		if i == 0:
			ax.set_ylabel("Predicted Turnout")
	plt.tight_layout()
	plt.savefig(f"figures/turnout_vs_adjusted_distance.pdf")


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

def measure_effect_of_distance_nn(df,labels_dict):
	distance_cols = ('dist_to_pollingLocation', 'dist_to_dropOffLocation', 'dist_to_earlyVoteSite')
	# distmods = np.linspace(start=-1,stop=2,num=3) #additive
	distmods = np.linspace(start=0.05,stop=2,num=8) #multiplicative
	N = df.count()
	lrdf = get_lrdf(df)
	nnModel = get_mlp(lrdf)
	true_turnout = df.agg(F.sum(outcome)).collect()[0][0]/N
	true_mean_dist = {distcol : df.agg(F.mean(distcol)).collect()[0][0] for distcol in distance_cols}
	true_stdev_dist = {distcol: df.agg(F.stddev(distcol)).collect()[0][0] for distcol in distance_cols}
	true_pred_turnout = nnModel.transform(lrdf).select('probability').rdd.map(lambda row: (float(row['probability'][1]))).mean()
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
				nnModel.transform(lrdf_mod)\
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
	plt.savefig(f"figures/turnout_vs_adjusted_distance_nn.pdf")


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

def plot_roc(targets,scores, AUC=None,ax=None,xlabel='False Positive Rate',ylabel='True Positive Rate',title="ROC Curve",nlabels=5,saveTitle=None):
	if ax is None:
		fig,ax = plt.subplots()

	fprs,tprs,thresh = roc_curve(targets,scores)
	label_inds = [i for i in range(0,len(thresh),len(thresh)//nlabels)]
	if len(thresh)-1 not in label_inds:
		label_inds.pop()
		label_inds.append(len(thresh)-1)
	labelx = fprs[label_inds]
	labely = tprs[label_inds]
	labeltext = thresh[label_inds]
	if AUC is not None:
		ax.plot(fprs, tprs, lw=2, label= f'AUC= {AUC : 0.04f}')
		ax.legend(loc="lower right",fontsize="x-small")
	else:
		ax.plot(fprs, tprs, lw=2)
	ax.scatter(labelx,labely,marker="x")
	for x,y,lab in zip(labelx,labely,labeltext):
		ax.text(x+0.02,y-0.04,f"{lab:.02}",fontsize=12)
	ax.grid(color='0.7', linestyle='--', linewidth=1)
	# ax.set_xlim([-0.1, 1.1])
	# ax.set_ylim([0.0, 1.05])
	ax.set_xlabel(xlabel,fontsize=15)
	ax.set_ylabel(ylabel,fontsize=15)
	ax.set_title(title)
	for label in ax.get_xticklabels()+ax.get_yticklabels():
		label.set_fontsize(15)
	if saveTitle is not None:
		plt.savefig('figures/roc.pdf')


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
	measure_effect_of_distance_nn(df,labels_dict)
