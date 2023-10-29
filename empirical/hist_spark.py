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



def measure_effect_of_distance_nn(df,labels_dict):
	distance_cols = ('dist_to_pollingLocation', 'dist_to_dropOffLocation', 'dist_to_earlyVoteSite')
	# distmods = np.linspace(start=-1,stop=2,num=3) #additive
	distmods = np.linspace(start=0.05,stop=2,num=8) #multiplicative
	N = df.count()
	lrdf = get_lrdf(df)
	nnModel = get_mlp(lrdf)
	true_turnout = df.agg(F.sum(outcome)).collect()[0][0]/N
	true_mean_dist = df.agg(F.mean(distcol)).collect()[0][0]
	true_stdev_dist = df.agg(F.stddev(distcol)).collect()[0][0] for distcol in distance_cols}
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


def histogram_pandas(df,distcol):
	nrows_total = df.count()
	all_dists = sample_pandas(df.select(distcol),nrows=nrows_total,printdf=False)
	all_dists = all_dists.loc[all_dists[distcol]<8]
	distmean = np.mean(all_dists[distcol].values)
	# bins =  [round(bin,4) for bin in np.linspace(0.05,7,100).tolist()]
	all_dists.hist(column=distcol)
	plt.savefig("figures/histogram_pandas.pdf")
	# plt.show()


def histogram_spark(df):
	distance_cols = ('dist_to_pollingLocation', 'dist_to_dropOffLocation', 'dist_to_earlyVoteSite')
	fig,axs = plt.subplots(1,len(distance_cols),figsize=(15,5),sharey=True)
	for i, (ax,distcol) in enumerate(zip(axs,distance_cols)):
		print(f"Processing {distcol}")
		ax.set_title(f"Distribution {distcol}")
		dist_mean = df.agg(F.mean(distcol)).collect()[0][0]
		print("Got dist_mean")
		dist_stddev = df.agg(F.stddev(distcol)).collect()[0][0]
		print("Got dist_stddev")
		bins, counts = df.select(distcol).rdd.flatMap(lambda x: x).histogram(20)
		ax.hist(bins[:-1], bins=bins, weights=counts)
		ax.axvline(dist_mean, color='red', linestyle='dashed', linewidth=1)
		min_ylim, max_ylim = ax.get_ylim()
		ax.text(dist_mean*1.1, max_ylim*0.9, f'Mean: {dist_mean :.2f}, Std dev: {dist_stddev :.2f}')
		ax.set_xlabel(f"{distcol} (km)")
		if i == 0:
			ax.set_ylabel("Frequency")
	plt.savefig("figures/histogram_spark.pdf")
	plt.show()
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
	# # hist = df.groupby(distcol).count().select('count').rdd.flatMap(lambda x: x).histogram(20)



	# df = df.filter(df[distcol] < 7)
	# dist_mean = df.agg(F.mean(distcol)).collect()[0][0]
	# dist_stddev = df.agg(F.stddev(distcol)).collect()[0][0]
	# bins =  [round(bin,4) for bin in np.linspace(0.05,7,25).tolist()]
	# hist = df.groupBy(distcol).count().rdd.values().histogram(bins)
	# freqs = pd.DataFrame(list(zip(*hist)),columns=['bin', 'frequency']).set_index('bin')
	# freqs.plot(kind='bar')
	# plt.show()


	# bins =  [round(bin,4) for bin in np.linspace(0.05,7,1000).tolist()]
	# # hist = filtered.groupby(distcol).count().select('count').rdd.flatMap(lambda x: x).histogram(bins)
	# hist = filtered.groupBy(distcol).count().rdd.values().histogram(bins)
	# hist2 = df.select(distcol).rdd.flatMap(lambda x: x).histogram(20)
	# freqs = pd.DataFrame(list(zip(*hist)),columns=['bin', 'frequency']).set_index('bin')
	# freqs.plot(kind='bar')

	# bins, counts = df.select(distcol).rdd.flatMap(lambda x: x).histogram(20)
	# plt.hist(bins[:-1], bins=bins, weights=counts)


	# plt.show()
	# # pdf = sample_pandas(df,nrows=1000,printdf=False)

 # "COUNTY_CODE", "REGISTRATION_NUMBER", "VOTER_STATUS", "LAST_NAME", "FIRST_NAME", "MIDDLE_MAIDEN_NAME", "NAME_SUFFIX", "NAME_TITLE", "RESIDENCE_HOUSE_NUMBER", "RESIDENCE_STREET_NAME", "RESIDENCE_STREET_SUFFIX", "RESIDENCE_APT_UNIT_NBR", "RESIDENCE_CITY", "RESIDENCE_ZIPCODE", "BIRTHDATE", "REGISTRATION_DATE", "RACE", "GENDER",	 "latitude", "longitude", "census_state", "census_county", "census_tract", "census_block", "tract_geoid", "closest_pollingLocation", "closest_dropOffLocation", "closest_earlyVoteSite", "dist_to_pollingLocation", "dist_to_dropOffLocation", "dist_to_earlyVoteSite", "tract_geoid$index"
