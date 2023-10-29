import numpy as np
from scipy.special import binom
from numpy import floor
import pandas as pd
import pyei
from itertools import product
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, Normalize

# from scipy.stats import truncnorm
from trun_mvnt import rtmvn, rtmvt

from plot_utilities import plotter
from pyRserve_utilities import start_rserve_daemon, do_eco_inf, stop_rserve, create_df, conn


@plotter
def plot_kde_eco_vs_agg_nScenarios(df):
	# https://seaborn.pydata.org/generated/seaborn.kdeplot.html
	bw_method = 'silverman' #'scott' is default
	sns.kdeplot(data=df,x='nScenarios_agg',bw_method=bw_method,bw_adjust=0.5,label=f"Aggregated statistics",cumulative=False)
	sns.kdeplot(data=df,x='nScenarios_eco',bw_method=bw_method,bw_adjust=0.5,label=f"(Oracle) Eco statistics",cumulative=False)



@plotter
def plot_hist_ratio_advantage(df):
	# https://seaborn.pydata.org/generated/seaborn.kdeplot.html
	bw_method = 'silverman' #'scott' is default
	sns.histplot(data=df,x='ratio',label=f"Ratio of #Scenarios",stat='probability',cumulative=False,log_scale=True)
	plt.title(f"Ratio of Total #Scenarios\nWhen Using Only Aggregate Statistics vs. Ecological Correlations\nTotal Ratio Over All Aggregations: {df['nScenarios_agg'].sum()/df['nScenarios_eco'].sum():.2f}",fontdict=dict(fontsize=10))


@plotter
def plot_eco_accuracy_vs_advantage_ratio(df):
	# https://seaborn.pydata.org/generated/seaborn.kdeplot.html
	splot = sns.scatterplot(data=df,x='ratio',y='pct_error_beta_B')
	splot.set(xscale='log')
	# pct_error_beta_B
	# pct_error_beta_W
# plot_eco_accuracy_vs_advantage_ratio(df)





@plotter(combined_df=True,legend=False)
def plot_hist_eco_vs_agg_nScenarios_one_df(df):
	sns.histplot(data=df,x='nScenarios',hue='agg',cumulative=False,multiple='dodge',log_scale=True)

@plotter(combined_df=True)
def plot_kde_nScenarios_one_df(df):
	# https://seaborn.pydata.org/generated/seaborn.kdeplot.html
	sns.kdeplot(data=df,x='nScenarios',hue='agg',bw_adjust=0.7,cumulative=False)




def gen_error_heatmap_function(valuecol='eco_improvement_B_R'):
	def _f(error_df):
		mat = error_df.pivot_table(index='Bw',columns='Bb',values=valuecol)
		if valuecol in ['ratio']:
			kwargs = dict(norm=LogNorm())
		else:
			kwargs = dict()
		ax = sns.heatmap(data=mat,annot=True,cmap="YlGnBu",annot_kws=dict(fontsize=5),**kwargs)
		ax.set_title(f'Values of {valuecol}')
		# plt.title(f'Values of {valuecol}')
	_f.__name__ = f'heatmap_Bw_Bb_{valuecol}'
	_f = plotter(subfolder='heatmaps',error_df=True)(_f)
	return _f

# @plotter(combined_df=True)
# def plot_heatmap_value_vs(error_df,valuecol):
# 	valuecol = 'eco_improvement_B_R'
# 	mat = error_df.pivot_table(index='Bw',columns='Bb',values=valuecol)
# 	ax = sns.heatmap(data=mat,annot=True,cmap="YlGnBu")
# 	ax.set_title(f'Values of {valuecol}')
# 	plt.savefig(f"")


# @plotter(combined_df=True)
# def plot_hist_eco_vs_agg_nScenarios_one_df(df):
# 	ax = sns.histplot(data=df,x='nScenarios',hue='agg',cumulative=False,multiple='dodge',log_scale=True,legend=True)
# 	return ax

def gen_scatterplot_function_feature_vs_feature(featurex,featurey,logy=False,logx=False):
	def _f(df):
		splot = sns.scatterplot(data=df,y=featurey,x=featurex)
		if logy:
			splot.set(yscale='log')
		if logx:
			splot.set(xscale='log')
		plt.title(f"{featurey} vs. {featurex}")
	_f.__name__ = f'scatterplot_{featurey}_vs_{featurex}'
	_f = plotter(subfolder='feature_vs_feature')(_f)
	return _f




def get_scenario_df(nPerPrecinct, nPrecincts, Bb, Bw, sigma_b, sigma_w, sigma_bw):
	Sigma = np.array([[sigma_b, sigma_bw],[sigma_bw, sigma_w]])
	reqPrecision = int(np.ceil(np.log10(nPerPrecinct)))
	betas = rtmvn(n=nPrecincts, Mean=np.array([Bb,Bw]), Sigma=Sigma, D=np.eye(2), lower=np.array([0,0]), upper=np.array([1,1]), burn=10, thin=1, ini=[])
	betas = np.around(betas, decimals=reqPrecision) # car ownership given race
	X = np.around(np.random.rand(nPrecincts),decimals=reqPrecision) # eXplanatory variable (race)
	print("Done with randomization")
	T = [beta_B*x + beta_W*(1-x) for x,(beta_B, beta_W) in zip(X,betas)]
	df = pd.DataFrame(data=dict(T=T,X=X,beta_B=[beta_B for (beta_B, beta_W) in betas],beta_W=[beta_W for (beta_B, beta_W) in betas]))
	df['N'] = nPerPrecinct
	df['nScenarios_agg'] = np.array([binom(nPerPrecinct,floor(t * nPerPrecinct)) for t in T])
	df['nScenarios_eco'] = np.array([binom(floor(x*nPerPrecinct),floor(beta_B*x*nPerPrecinct)) * binom(floor((1-x)*nPerPrecinct),floor(beta_W*(1-x)*nPerPrecinct)) for x,(beta_B, beta_W) in zip(X,betas)])
	df['ratio'] = df['nScenarios_agg']/df['nScenarios_eco']
	N = df['N'].values
	df['pct_error_beta_B_using_T'] = (df['beta_B']-df['T'])/df['beta_B']
	df['pct_error_beta_W_using_T'] = (df['beta_W']-df['T'])/df['beta_W']
	print("Done with dataframe construction. Beginning ecological inference")
	return df

def combine_columns_add_key(df,old_col1,old_col2,new_col,key_col,key1,key2):
	df1,df2 = df.copy(),df.copy()
	df1[new_col] = df1[old_col1]
	df2[new_col] = df2[old_col2]
	df1[key_col] = key1
	df2[key_col] = key2
	df_new = pd.concat([df1,df2])
	df_new.drop([old_col1,old_col2],axis=1,inplace=True)
	return df_new

def add_eco_inference(df,model_name="king99_pareto_modification"):
	if model_name=="king99_pareto_modification":
		ei = pyei.TwoByTwoEI(model_name=model_name, pareto_scale=8, pareto_shape=2)
	else:
		ei = pyei.TwoByTwoEI(model_name=model_name)
	ei.fit(df['X'].values, df['T'].values, df['N'].values, demographic_group_name="Black", candidate_name="Vehicle Owners", precinct_names=df.index.values)
	# precinct_level_estimates() method of pyei.two_by_two.TwoByTwoEI instance
	#     If desired, we can return precinct-level estimates
	#     Returns:
	#         precinct_posterior_means: num_precincts x 2 (groups) x 2 (candidates)
	#         precinct_credible_intervals: num_precincts x 2 (groups) x 2 (candidates) x 2 (endpoints)
	ests = ei.precinct_level_estimates()[0]
	beta_b_i = [est[0][0] for est in ests]
	beta_w_i = [est[1][0] for est in ests]
	df['beta_B_est'] = beta_b_i
	df['beta_W_est'] = beta_w_i
	df['pct_error_beta_B'] = (df['beta_B_est']-df['beta_B'])/df['beta_B']
	df['pct_error_beta_W'] = (df['beta_W_est']-df['beta_W'])/df['beta_W']

	df['eco_improvement_B'] = abs(df['pct_error_beta_B_using_T']) - abs(df['pct_error_beta_B'])
	df['eco_improvement_W'] = abs(df['pct_error_beta_W_using_T']) - abs(df['pct_error_beta_W'])
	# return df


def add_eco_inference_rserve(df):
	# start_rserve_daemon()
	bb_out = do_eco_inf(df)
	df['beta_B_est_R'] = bb_out['betab']
	df['beta_W_est_R'] = bb_out['betaw']

	df['abs_error_beta_B_R'] = (df['beta_B_est_R']-df['beta_B'])
	df['abs_error_beta_B_W'] = (df['beta_W_est_R']-df['beta_W'])
	df['pct_error_beta_B_R'] = (df['beta_B_est_R']-df['beta_B'])/df['beta_B']
	df['pct_error_beta_W_R'] = (df['beta_W_est_R']-df['beta_W'])/df['beta_W']
	df['eco_improvement_B_R'] = abs(df['pct_error_beta_B_using_T']) - abs(df['pct_error_beta_B_R'])
	df['eco_improvement_W_R'] = abs(df['pct_error_beta_W_using_T']) - abs(df['pct_error_beta_W_R'])
	# return df


	# stop_rserve()
	# return b_i_B
def error_df_experiment(Bbs,Bws,nPerPrecinct,nPrecincts,sigma_b,sigma_w,sigma_bw):
	errors = {}
	mean_error_cols = ['nScenarios_agg', 'nScenarios_eco','ratio', 'pct_error_beta_B_using_T', 'pct_error_beta_W_using_T','beta_B_est_R', 'beta_W_est_R', 'pct_error_beta_B_R','pct_error_beta_W_R', 'eco_improvement_B_R', 'eco_improvement_W_R', 'abs_error_beta_B_R', 'abs_error_beta_B_W',]
	for Bb,Bw in product(Bbs,Bws):
	# for Bb,Bw in product(np.linspace(0.25,0.75,2),np.linspace(0.5,0.99,2)):
		print(f"{Bb=},{Bw=}")

		df = get_scenario_df(nPerPrecinct, nPrecincts, Bb, Bw, sigma_b, sigma_w, sigma_bw)
		# add_eco_inference(df)
		add_eco_inference_rserve(df)
		# print(f"Eco Advantage (pyEI) for beta_B, beta_W: {df.eco_improvement_B.mean():.4f},{df.eco_improvement_W.mean():.4f}")
		print(f"Eco Advantage (eiR ) for beta_B, beta_W: {df.eco_improvement_B_R.mean():.4f},{df.eco_improvement_W_R.mean():.4f}")
		# df2 = df[['beta_B','beta_B_est','beta_W_est','beta_B_est_R','beta_W_est_R','eco_improvement_B_R', 'eco_improvement_W_R', 'eco_improvement_B', 'eco_improvement_W']]
		errors[Bb,Bw] = [df[col].mean() for col in mean_error_cols]
		# (df.eco_improvement_B_R.mean(), df.eco_improvement_W_R.mean(),df.beta_B_est_R.mean(), df.beta_W_est_R.mean(), df.pct_error_beta_B_using_T.mean(), df.pct_error_beta_W_using_T.mean())

	error_df = pd.DataFrame([[Bb,Bw] + [row[i] for i,col in enumerate(mean_error_cols)] for (Bb,Bw),row in errors.items()],columns=['Bb','Bw'] + mean_error_cols)
	error_df.Bb= error_df.Bb.round(2)
	error_df.Bw= error_df.Bw.round(2)
	return error_df

if __name__=="__main__":
	## From eiR output:
	# $`Truncated psi's (ultimate scale)`
	#         BB        BW       SB        SW       RHO
	#  0.9656009 0.9902505 0.033126 0.0136933 0.2010252
	Bbs,Bws= np.linspace(0.25,0.75,10),np.linspace(0.5,0.99,10)
	# Bbs,Bws= np.linspace(0.25,0.75,2),np.linspace(0.5,0.99,2)
	nPerPrecinct = 100 # make multiple of 10 to always have compatible fractions
	nPrecincts = 800
	Bb, Bw = 0.7, 0.95 # 0.965, 0.991 = estimates from tract data
	sigma_b = .01 #2.72 # .033
	sigma_w = .02 #2.65 # .014
	sigma_bw = 0.008 #0.40 # .20

	df = get_scenario_df(nPerPrecinct, nPrecincts, Bb, Bw, sigma_b, sigma_w, sigma_bw)
	add_eco_inference(df)
	add_eco_inference_rserve(df)
	df_combined = combine_columns_add_key(df,'nScenarios_agg','nScenarios_eco','nScenarios','agg','Only Aggregate Statistics Used','Ecological Correlations Used')

	error_df = error_df_experiment(Bbs,Bws,nPerPrecinct,nPrecincts,sigma_b,sigma_w,sigma_bw)


	for feature in error_df.columns:
		if feature not in ['Bb','Bw']:
			gen_error_heatmap_function(valuecol=feature)



	for feature in df.columns:
		gen_scatterplot_function_feature_vs_feature(feature,'ratio',logy=True)
		gen_scatterplot_function_feature_vs_feature('ratio',feature,logx=True)

	if True:
		for k,f in plotter.wrapped.items():
			print("\n\n")
			print(f.__name__)
			print(k)
			print(f)
			# if 'scatterplot_' not in f.__name__:
			# 	continue
			if getattr(f,'combined_df',False):
				continue
				f(df_combined)
			elif getattr(f,'error_df',False):
				f(error_df)
			else:
				continue
				f(df)
			print("\n\n")

